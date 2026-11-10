"""Command line interface: ``ducklake-iceberg sync --table schema.table``."""

from __future__ import annotations

import logging
import time
from importlib.metadata import version as pkg_version
from pathlib import Path
from typing import Annotated, Literal

import typer

from ducklake_iceberg import timing
from ducklake_iceberg.config import load_settings
from ducklake_iceberg.errors import DuckLakeIcebergError, ExitCode
from ducklake_iceberg.obs.logging import (
    LogFormat,
    configure_logging,
    is_configured,
    log_event,
    reset_logging,
)
from ducklake_iceberg.obs.redaction import (
    redaction_failures,
    reset_redaction_failures,
    scrub,
)
from ducklake_iceberg.output import Envelope, Problem, emit
from ducklake_iceberg.pipeline import (
    apply_concurrency_limits,
    problem_from_error,
    sync_table,
)

LOG = logging.getLogger("ducklake_iceberg")

app = typer.Typer(
    name="ducklake-iceberg",
    help=(
        "Publishes DuckLake tables as Iceberg v2 in AWS Glue.\n\n"
        "Note: a parser usage error exits with code 2, which collides with the "
        "compatibility blocker; use --json to disambiguate."
    ),
    no_args_is_help=True,
    add_completion=False,
    # the traceback must not print local variables: FileIO/GlueCatalog frames
    # carry the profile credential in ``s3.*``
    pretty_exceptions_show_locals=False,
)

#: exceptions that control the exit (``typer.Exit``, ``typer.Abort``, usage
#: errors): they are not internal errors and go on to Typer. Only Typer's
#: public names are used: Typer 0.29 vendors Click instead of depending on it.
_CLICK_CONTROL: tuple[type[BaseException], ...] = (
    typer.Exit,
    typer.Abort,
    typer.BadParameter,
)


def _version_callback(value: bool) -> None:
    """Short-circuit during parsing; without it the group requires a subcommand."""
    if value:
        typer.echo(
            f"ducklake-iceberg {pkg_version('ducklake-iceberg')} "
            f"(duckdb {pkg_version('duckdb')}, "
            f"pyiceberg {pkg_version('pyiceberg')})"
        )
        raise typer.Exit(code=int(ExitCode.SUCCESS))


@app.callback()
def root(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            callback=_version_callback,
            is_eager=True,
            help="print the version and exit",
        ),
    ] = False,
) -> None:
    """No cross-cutting flags: they are declared per command."""


def _internal_error(envelope: Envelope, exc: BaseException) -> None:
    """An unhandled exception becomes a redacted ``problem`` and exit 1.

    Without this, Typer would print the traceback with the raw message, which
    may carry the DSN or a credential echoed by a dependency. The traceback,
    already redacted, only goes to the log at ``--log-level DEBUG``.
    """
    LOG.debug("internal error", exc_info=exc)
    detail = str(exc) or type(exc).__name__
    envelope.problems.append(
        Problem(
            code="internal_error",
            message=f"internal error: {scrub(detail)}",
            hint="rerun with --log-level DEBUG",
        )
    )
    envelope.set_exit(ExitCode.ERROR)


def _finish(
    envelope: Envelope,
    started: float,
    *,
    json_out: bool,
    log_format: LogFormat,
) -> None:
    if not is_configured():
        # the failure came before the config was loaded: the log still comes out
        # in the requested format, so every stderr line follows the contract
        reset_redaction_failures()
        configure_logging("INFO", fmt=log_format, command=envelope.command)
    envelope.duration_ms = int((time.perf_counter() - started) * 1000)
    if timing.enabled():
        envelope.timings = timing.snapshot()
    for problem in envelope.problems:
        log_event(
            "problem",
            level=(
                logging.ERROR
                if envelope.exit_code == int(ExitCode.ERROR)
                and problem.level != "warning"
                else logging.WARNING
            ),
            code=problem.code,
            table=problem.table,
            message=problem.message,
        )
    log_event(
        "command.end",
        level=(
            logging.ERROR if envelope.exit_code == int(ExitCode.ERROR) else logging.INFO
        ),
        duration_ms=envelope.duration_ms,
        exit_code=envelope.exit_code,
        dry_run=envelope.dry_run,
    )
    dropped = redaction_failures()
    if dropped:
        # fail-closed: the lines were dropped; the exit code does not change.
        # Checked after ``command.end`` so the count covers the last line.
        envelope.problems.append(
            Problem(
                code="redaction_failed",
                message=(f"{dropped} log line(s) dropped: secret redaction failed"),
                hint="open an incident; no credential was emitted",
                level="warning",
            )
        )
    # the command is over: the handler and library log levels go back to their
    # previous state (consecutive runs in the same process do not inherit the
    # stream)
    reset_logging()
    emit(envelope, json_out=json_out)
    raise typer.Exit(code=envelope.exit_code)


@app.command()
def sync(
    table: Annotated[
        str, typer.Option("--table", help="schema.table", show_default=False)
    ],
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="write nothing (no flush, no export)")
    ] = False,
    json_out: Annotated[
        bool, typer.Option("--json", help="JSON envelope on stdout")
    ] = False,
    profile: Annotated[
        str | None, typer.Option("--profile", help="AWS profile")
    ] = None,
    log_level: Annotated[str | None, typer.Option("--log-level")] = None,
    log_format: Annotated[
        Literal["auto", "json", "text"],
        typer.Option("--log-format", help="log format on stderr"),
    ] = "auto",
    config: Annotated[Path | None, typer.Option("--config", help="YAML file")] = None,
) -> None:
    """Publish one table: doctor + flush of inlined data + export + register."""
    started = time.perf_counter()
    envelope = Envelope(command="sync", dry_run=dry_run)
    try:
        overrides: dict[str, str] = {}
        if profile is not None:
            overrides["aws_profile"] = profile
        if log_level is not None:
            overrides["log_level"] = log_level
        settings = load_settings(config_file=config, overrides=overrides)
        envelope.config = settings.redacted()
        reset_redaction_failures()
        run_id = configure_logging(settings.log_level, fmt=log_format, command="sync")
        settings = apply_concurrency_limits(settings, envelope)
        envelope.config = settings.redacted()
        code = sync_table(
            settings, table, run_id=run_id, dry_run=dry_run, envelope=envelope
        )
        envelope.set_exit(code)
    except DuckLakeIcebergError as exc:
        envelope.problems.append(problem_from_error(exc, table=table))
        envelope.set_exit(exc.exit_code)
    except _CLICK_CONTROL:
        raise
    except Exception as exc:
        _internal_error(envelope, exc)
    _finish(envelope, started, json_out=json_out, log_format=log_format)


def main() -> None:
    """Real entry point; `app` is the console script callable."""
    app()
