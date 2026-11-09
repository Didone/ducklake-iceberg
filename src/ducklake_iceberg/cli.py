from __future__ import annotations

import logging
import re
import time
from collections.abc import Iterable, Iterator
from contextlib import ExitStack, contextmanager, nullcontext
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from importlib.metadata import version as pkg_version
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal, cast
from urllib.parse import urlparse

import click
import typer

from ducklake_iceberg import timing
from ducklake_iceberg.catalog.connection import metadata_catalog, open_catalog
from ducklake_iceberg.catalog.reader import (
    current_snapshot,
    inlined_data_table,
    list_tables,
    parse_table_ref,
    resolve_table,
    snapshot_at,
)
from ducklake_iceberg.checkpoint.command import run_checkpoint
from ducklake_iceberg.checkpoint.partitions import DEFAULT_CLOSED_AFTER_SECONDS
from ducklake_iceberg.concurrency import (
    CANCELLED,
    INTERRUPTED,
    ConsolidatedReport,
    Interrupted,
    Limits,
    Outcome,
    apply_limits,
    map_parallel,
)
from ducklake_iceberg.config import DuckLakeIcebergSettings, load_settings
from ducklake_iceberg.doctor import rules as doctor_rules
from ducklake_iceberg.doctor.model import DoctorContext, DoctorTarget, RuleStatus
from ducklake_iceberg.doctor.report import DoctorReport
from ducklake_iceberg.doctor.runner import run as run_rules
from ducklake_iceberg.errors import (
    AwsError,
    ConfigError,
    DivergenceError,
    DuckLakeIcebergError,
    ExitCode,
    IdentityError,
    NothingToDo,
)
from ducklake_iceberg.export import run_export
from ducklake_iceberg.obs.logging import (
    LogFormat,
    bind,
    configure_logging,
    current_run_id,
    is_configured,
    log_event,
    reset_logging,
)
from ducklake_iceberg.obs.metrics import RunMetrics, add_work, collect
from ducklake_iceberg.obs.redaction import (
    redaction_failures,
    reset_redaction_failures,
    scrub,
)
from ducklake_iceberg.output import (
    DEPRECATED_ENVELOPE_VERSIONS,
    ENVELOPE_VERSION,
    SUPPORTED_ENVELOPE_VERSIONS,
    Envelope,
    Problem,
    emit,
    worse_exit,
)
from ducklake_iceberg.register import run_register
from ducklake_iceberg.validate import (
    Tolerances,
    ValidateOptions,
    ValidationReport,
    empty_validation_result,
    lake_row_count,
    resolve_published_snapshot,
    validate_table,
)

if TYPE_CHECKING:
    from pyiceberg.catalog.glue import GlueCatalog
    from pyiceberg.io import FileIO

    from ducklake_iceberg.catalog.connection import DuckLakeConnection
    from ducklake_iceberg.catalog.reader import Snapshot, TableIdentity, TableRef
    from ducklake_iceberg.checkpoint.dry_run import DryRunReport
    from ducklake_iceberg.checkpoint.model import CheckpointRunReport
    from ducklake_iceberg.s3 import ObjectStore

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

# --- aliases for the cross-cutting flags (declared PER command) ---
TableOpt = Annotated[
    str | None, typer.Option("--table", help="schema.table", show_default=False)
]
AllTablesOpt = Annotated[bool, typer.Option("--all-tables", help="all tables")]
SchemaOpt = Annotated[
    str | None, typer.Option("--schema", help="restrict to one schema")
]
DryRunOpt = Annotated[bool, typer.Option("--dry-run", help="write nothing")]
JsonOpt = Annotated[bool, typer.Option("--json", help="JSON envelope on stdout")]
YesOpt = Annotated[
    bool, typer.Option("--yes", help="skip confirmation with --all-tables")
]
ProfileOpt = Annotated[str | None, typer.Option("--profile", help="AWS profile")]
LogLevelOpt = Annotated[str | None, typer.Option("--log-level")]
ConfigOpt = Annotated[Path | None, typer.Option("--config", help="YAML file")]
LogFormatOpt = Annotated[
    Literal["auto", "json", "text"],
    typer.Option("--log-format", help="log format on stderr"),
]
#: off by default outside ``sync``, which has ``--metrics/--no-metrics`` on
MetricsOpt = Annotated[
    bool,
    typer.Option(
        "--metrics",
        help="include the metrics block (phases and counters) in results[]",
    ),
]


def _json_version_callback(value: int) -> int:
    """``--json-version N`` outside the supported versions is a usage error (exit 2)."""
    if value not in SUPPORTED_ENVELOPE_VERSIONS:
        raise typer.BadParameter(
            f"unsupported envelope version {value} (current {ENVELOPE_VERSION})"
        )
    return value


JsonVersionOpt = Annotated[
    int,
    typer.Option(
        "--json-version",
        callback=_json_version_callback,
        help="version of the --json envelope",
    ),
]


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


# --------------------------------------------------------------------------
# shared infrastructure
# --------------------------------------------------------------------------
def _configure_logging(
    level: str, *, fmt: LogFormat = "auto", command: str = "unknown"
) -> str:
    reset_redaction_failures()
    return configure_logging(level, fmt=fmt, command=command)


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


#: click exceptions that control the exit (``typer.Exit``, ``Abort``, usage
#: errors): they are not internal errors and go on to Typer
_CLICK_CONTROL: tuple[type[BaseException], ...] = (
    click.exceptions.Exit,
    click.exceptions.Abort,
    click.ClickException,
)


@contextmanager
def _table_metrics(
    envelope: Envelope, command: str, table: str | None, *, enabled: bool
) -> Iterator[RunMetrics]:
    """Metrics for one unit of work (a table, or the whole command).

    The block's phases go into their own ``RunMetrics``, the log context gains
    ``table`` and, with ``--metrics``, the ``metrics`` block goes into the
    first ``results[]`` item appended inside the block (each block is a unit
    of work; the item's ``table`` is not usable as a key because ``export``
    publishes only the table name, which is part of its output contract).
    """
    metrics = RunMetrics(current_run_id(), command, table=table)
    first = len(envelope.results)
    context = bind(table=table) if table is not None else nullcontext()
    try:
        with context, collect(metrics):
            yield metrics
    finally:
        if enabled and len(envelope.results) > first:
            envelope.results[first]["metrics"] = metrics.to_json()


def _overrides(*, profile: str | None, log_level: str | None) -> dict[str, Any]:
    over: dict[str, Any] = {}
    if profile is not None:
        over["aws_profile"] = profile
    if log_level is not None:
        over["log_level"] = log_level
    return over


def _validate_target_flags(
    *, table: str | None, all_tables: bool, schema: str | None
) -> None:
    """Flag-combination check in the body (exits with 1, not 2)."""
    if table and all_tables:
        raise ConfigError("use --table or --all-tables, not both")
    if not table and not all_tables:
        raise ConfigError("provide --table or --all-tables")
    if schema and not all_tables:
        raise ConfigError("--schema only applies to --all-tables")


def _problem_from_error(exc: BaseException, *, table: str | None = None) -> Problem:
    """``Problem`` for any pipeline error.

    Several export errors (enumeration, manifest, metadata.json) do not inherit
    from ``DuckLakeIcebergError`` but carry ``exit_code``; handling them here keeps them
    from escaping as a traceback and preserves the contractual exit code.
    """
    message = scrub(
        str(getattr(exc, "message", None) or str(exc) or type(exc).__name__)
    )
    return Problem(
        code=_code_for(exc),
        message=str(message),
        hint=getattr(exc, "hint", None),
        table=table,
    )


def _exit_of(exc: BaseException) -> ExitCode:
    """``exit_code`` declared by the error; ``ERROR`` when missing/invalid."""
    try:
        return ExitCode(int(getattr(exc, "exit_code", ExitCode.ERROR)))
    except (TypeError, ValueError):
        return ExitCode.ERROR


def _code_for(exc: BaseException) -> str:
    from ducklake_iceberg.errors import DivergenceError, NothingToDo, OperationAborted

    text = str(getattr(exc, "message", None) or exc).lower()
    if "profile" in text:
        return "profile_not_found"
    if "does not exist at snapshot" in text or "not found" in text:
        return "table_not_found"
    if isinstance(exc, OperationAborted):
        return "aborted"
    if isinstance(exc, NothingToDo):
        return "nothing_to_do"
    if isinstance(exc, DivergenceError):
        return "catalog_diverged"
    if isinstance(exc, ConfigError):
        return "config_invalid"
    if isinstance(exc, AwsError):
        return "aws_error"
    if _exit_of(exc) is ExitCode.BLOCKED:
        return "blocked"
    if _exit_of(exc) is ExitCode.DIVERGED:
        return "catalog_diverged"
    return "error"


def _finish(
    envelope: Envelope,
    started: float,
    *,
    json_out: bool,
    log_format: LogFormat = "auto",
    json_version: int = ENVELOPE_VERSION,
) -> None:
    if not is_configured():
        # the failure came before the config was loaded: the log still comes out
        # in the requested format, so every stderr line follows the contract
        _configure_logging("INFO", fmt=log_format, command=envelope.command)
    envelope.duration_ms = int((time.perf_counter() - started) * 1000)
    envelope.version = json_version
    if json_version in DEPRECATED_ENVELOPE_VERSIONS:
        envelope.problems.append(
            Problem(
                code="envelope_version_deprecated",
                message=(
                    f"envelope v{json_version} is deprecated "
                    f"(current v{ENVELOPE_VERSION})"
                ),
                hint=f"migrate the consumer to --json-version {ENVELOPE_VERSION}",
                level="warning",
            )
        )
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


# --------------------------------------------------------------------------
# exit-code aggregation and scope resolution
# --------------------------------------------------------------------------
#: priority of `aggregate_exit_code`: ERROR > BLOCKED > DIVERGED >
#: NOTHING_TO_DO > SUCCESS. It is not the numeric order: `1` is the only
#: possibly transient condition, while `2`/`3` require human intervention.
_EXIT_PRIORITY: tuple[ExitCode, ...] = (
    ExitCode.ERROR,
    ExitCode.BLOCKED,
    ExitCode.DIVERGED,
    ExitCode.NOTHING_TO_DO,
    ExitCode.SUCCESS,
)


def aggregate_exit_code(codes: Iterable[ExitCode]) -> ExitCode:
    """Reduce a list of exit codes to the most pessimistic one.

    Priority ``ERROR > BLOCKED > DIVERGED > NOTHING_TO_DO > SUCCESS``.
    Empty list -> ``NOTHING_TO_DO`` (empty scope).
    """
    present = set(codes)
    if not present:
        return ExitCode.NOTHING_TO_DO
    for code in _EXIT_PRIORITY:
        if code in present:
            return code
    return ExitCode.SUCCESS


def _resolve_scope(
    settings: DuckLakeIcebergSettings,
    *,
    table: str | None,
    schema: str | None,
) -> list[tuple[str, str]]:
    """Return the list of ``(schema_name, table_name)`` in scope.

    ``--table`` resolves directly (no connection is opened for a single
    qualified table); ``--all-tables``/``--schema`` list the live tables of the
    current snapshot through a READ_ONLY connection.
    """
    if table is not None:
        ref = parse_table_ref(table)
        if "." not in table:
            raise ConfigError(
                f"--table requires a qualified name: <schema>.<table> (got {table!r})"
            )
        return [(ref.schema_name, ref.table_name)]
    with open_catalog(settings, read_only=True) as con, con.read_transaction():
        snapshot = current_snapshot(con)
        refs = list_tables(con, snapshot, schema_name=schema)
    return [(r.schema_name, r.table_name) for r in refs]


def _apply_limits(
    settings: DuckLakeIcebergSettings, envelope: Envelope, *, table_workers: int
) -> tuple[DuckLakeIcebergSettings, Limits]:
    """Reconcile the limits against the global cap and install them in the process.

    ``table_workers`` is the number of concurrent tables (1 outside
    ``--all-tables``). Each reduced limit becomes a ``warning``-level
    ``problem`` with ``code="workers_capped"``; the internal pools (footers,
    I/O, PyIceberg) then use the effective values.
    """
    requested = Limits(
        max_table_workers=table_workers,
        max_footer_workers=settings.max_footer_workers,
        max_io_workers=settings.max_io_workers,
        max_total_workers=settings.max_total_workers,
        duckdb_threads=settings.duckdb_threads,
        duckdb_memory_limit=settings.duckdb_memory_limit,
        pyiceberg_max_workers=settings.pyiceberg_max_workers,
    )
    limits = requested.for_all_tables()
    capped = limits.capped_from(requested)
    if capped:
        envelope.problems.append(
            Problem(
                code="workers_capped",
                message=(
                    "; ".join(
                        f"{name} reduced from {asked} to {effective}"
                        for name, asked, effective in capped
                    )
                    + f" by the global cap (max_total_workers="
                    f"{limits.max_total_workers})"
                ),
                hint=(
                    "raise DUCKLAKE_ICEBERG_MAX_TOTAL_WORKERS or lower the other limits"
                ),
                level="warning",
            )
        )
    apply_limits(limits)
    updates = {
        name: getattr(limits, name)
        for name in ("max_footer_workers", "max_io_workers", "pyiceberg_max_workers")
        if getattr(limits, name) != getattr(settings, name)
    }
    return (settings.model_copy(update=updates) if updates else settings), limits


# --------------------------------------------------------------------------
# doctor
# --------------------------------------------------------------------------
@app.command()
def doctor(
    table: TableOpt = None,
    all_tables: AllTablesOpt = False,
    schema: SchemaOpt = None,
    dry_run: DryRunOpt = False,
    json_out: JsonOpt = False,
    profile: ProfileOpt = None,
    log_level: LogLevelOpt = None,
    log_format: LogFormatOpt = "auto",
    config: ConfigOpt = None,
    metrics_enabled: MetricsOpt = False,
    json_version: JsonVersionOpt = ENVELOPE_VERSION,
) -> None:
    """Compatibility preflight: identity, connectivity and rules."""
    started = time.perf_counter()
    envelope = Envelope(command="doctor", dry_run=dry_run)
    try:
        _validate_target_flags(table=table, all_tables=all_tables, schema=schema)
        settings = load_settings(
            config_file=config,
            overrides=_overrides(profile=profile, log_level=log_level),
        )
        envelope.config = settings.redacted()
        _configure_logging(settings.log_level, fmt=log_format, command="doctor")
        _run_doctor(
            settings,
            envelope,
            table=table,
            schema=schema,
            metrics_enabled=metrics_enabled,
        )
    except DuckLakeIcebergError as exc:
        envelope.problems.append(_problem_from_error(exc))
        envelope.set_exit(exc.exit_code)
    except _CLICK_CONTROL:
        raise
    except Exception as exc:
        _internal_error(envelope, exc)
    _finish(
        envelope,
        started,
        json_out=json_out,
        log_format=log_format,
        json_version=json_version,
    )


def _run_doctor(
    settings: DuckLakeIcebergSettings,
    envelope: Envelope,
    *,
    table: str | None,
    schema: str | None,
    metrics_enabled: bool = False,
) -> None:
    with open_catalog(settings, read_only=True) as con, con.read_transaction():
        snapshot = current_snapshot(con)
        refs = _resolve_targets(con, snapshot, table=table, schema=schema)
        env_diag = con.environment()
        aws = _aws_checks(settings)
        worst = ExitCode.SUCCESS
        for ref in refs:
            with _table_metrics(envelope, "doctor", str(ref), enabled=metrics_enabled):
                try:
                    result, rules_exit = _doctor_one(
                        con, snapshot, ref, env_diag, aws, settings
                    )
                    envelope.results.append(result)
                    if rules_exit is ExitCode.BLOCKED:
                        log_event(
                            "compat.blocked",
                            level=logging.ERROR,
                            table=str(ref),
                            exit_code=int(rules_exit),
                        )
                    worst = worse_exit(worst, rules_exit)
                except DuckLakeIcebergError as exc:
                    envelope.problems.append(_problem_from_error(exc, table=str(ref)))
                    worst = worse_exit(worst, exc.exit_code)
        worst = worse_exit(worst, _reconcile_data_path(con, settings, envelope))

    if not refs:
        worst = worse_exit(worst, ExitCode.NOTHING_TO_DO)
    if envelope.problems and worst == ExitCode.SUCCESS:
        worst = ExitCode.ERROR
    envelope.set_exit(worst)


def _resolve_targets(
    con: DuckLakeConnection,
    snapshot: Snapshot,
    *,
    table: str | None,
    schema: str | None,
) -> tuple[TableRef, ...]:
    if table:
        return (parse_table_ref(table),)
    return list_tables(con, snapshot, schema_name=schema)


def _doctor_one(
    con: DuckLakeConnection,
    snapshot: Snapshot,
    ref: TableRef,
    env_diag: Any,
    aws: dict[str, Any],
    settings: DuckLakeIcebergSettings,
) -> tuple[dict[str, Any], ExitCode]:
    with timing.phase("resolve"):
        identity = resolve_table(con, ref, snapshot)
        add_work("resolve", items=1)
    inlined = inlined_data_table(con, identity)
    with timing.phase("doctor"):
        report = _run_rules(con, identity)
    rules_exit = _exit_code_from_report(report)
    return (
        {
            "table": str(ref),
            "identity": {
                "schema_name": identity.ref.schema_name,
                "table_name": identity.ref.table_name,
                "table_id": identity.table_id,
                "table_uuid": str(identity.table_uuid),
                "schema_id": identity.schema_id,
                "schema_version": identity.schema_version,
            },
            "snapshot": {
                "snapshot_id": snapshot.snapshot_id,
                "snapshot_time": snapshot.snapshot_time.isoformat(),
                "schema_version": snapshot.schema_version,
                "next_catalog_id": snapshot.next_catalog_id,
                "next_file_id": snapshot.next_file_id,
            },
            "environment": {
                "duckdb_version": env_diag.duckdb_version,
                "extensions": env_diag.extensions,
                "attached": list(env_diag.attached),
                "read_only": env_diag.read_only,
            },
            "aws": aws,
            "inlined_rows": inlined.row_count if inlined else 0,
            "rules": report.to_json(),
        },
        rules_exit,
    )


def _run_rules(con: DuckLakeConnection, identity: TableIdentity) -> DoctorReport:
    """Run the preflight rules in the transaction already opened by the CLI."""
    target = DoctorTarget(
        catalog=con.alias,
        metadata_catalog=con.meta,
        schema_name=identity.ref.schema_name,
        table_name=identity.ref.table_name,
        table_id=identity.table_id,
        snapshot_id=identity.snapshot.snapshot_id,
        schema_version=identity.schema_version,
    )
    ctx = DoctorContext(con=con.raw, target=target)
    report = run_rules(ctx, list(doctor_rules.RULES), manage_transaction=False)
    # ``doctor`` phase: rules evaluated (those skipped by a gate do not count)
    add_work(
        "doctor",
        items=sum(1 for r in report.results if r.status is not RuleStatus.SKIPPED),
    )
    return report


def _exit_code_from_report(report: DoctorReport) -> ExitCode:
    """Map the rules report exit code (0/1/2) to `ExitCode`."""
    code = report.exit_code()
    if code == 1:
        return ExitCode.ERROR
    if code == 2:
        return ExitCode.BLOCKED
    return ExitCode.SUCCESS


def _reconcile_data_path(
    con: DuckLakeConnection, settings: DuckLakeIcebergSettings, envelope: Envelope
) -> ExitCode:
    """Reconcile data_path: compare the declared data_path with the catalog's
    for equality (after normalization) and, on divergence, record a problem
    and return ExitCode.ERROR (exit 1)."""
    from ducklake_iceberg.catalog.reader import catalog_data_path

    catalog_value = catalog_data_path(con)
    declared = settings.data_path.rstrip("/") + "/"
    if catalog_value != declared:
        envelope.problems.append(
            Problem(
                code="data_path_mismatch",
                message=(
                    f"data_path mismatch - config: {declared}, catalog: {catalog_value}"
                ),
                hint="align the configured data_path with the catalog value",
            )
        )
        return ExitCode.ERROR
    return ExitCode.SUCCESS


def _aws_checks(settings: DuckLakeIcebergSettings) -> dict[str, Any]:
    """Read-only AWS checks (Glue get_database, S3 list)."""
    import boto3
    from botocore.exceptions import ClientError

    result: dict[str, Any] = {
        "region": settings.aws_region,
        "profile": settings.aws_profile,
        "glue_database": settings.glue_database,
        "glue_reachable": False,
        "glue_database_exists": False,
        "data_path_readable": None,
        "metadata_prefix_writable": None,
    }
    session = boto3.Session(
        profile_name=settings.aws_profile, region_name=settings.aws_region
    )

    glue = session.client("glue")
    try:
        glue.get_database(Name=settings.glue_database)
        result["glue_reachable"] = True
        result["glue_database_exists"] = True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code == "EntityNotFoundException":
            result["glue_reachable"] = (
                True  # reachable, but the database does not exist
            )
        elif code == "AccessDeniedException":
            raise AwsError(
                f"access denied to Glue database {settings.glue_database!r}",
                hint="review the IAM permissions",
            ) from exc
        else:
            raise AwsError(f"failed to query Glue: {exc}") from exc

    if settings.data_path.startswith("s3://"):
        result["data_path_readable"] = _s3_readable(session, settings.data_path)
    return result


def _s3_readable(session: Any, data_path: str) -> bool:
    from botocore.exceptions import ClientError

    parsed = urlparse(data_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    s3 = session.client("s3")
    try:
        s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"AccessDenied", "AccessDeniedException"}:
            raise AwsError(
                f"access denied to data_path {data_path}",
                hint="review the bucket policy and IAM",
            ) from exc
        raise AwsError(f"failed to access data_path: {exc}") from exc
    return True


def _checkpoint_confirm(table_count: int) -> bool:
    """Confirmation for `--all-tables`.

    Only asks on an interactive terminal; non-interactive runs proceed, because
    the real safeguard against the wrong catalog is the fingerprint.
    """
    import sys

    if not sys.stdin.isatty():
        return True
    return typer.confirm(
        f"checkpoint will write to {table_count} DuckLake table(s) "
        "(--all-tables). Continue?",
        default=False,
    )


@app.command()
def checkpoint(
    table: TableOpt = None,
    all_tables: AllTablesOpt = False,
    schema: SchemaOpt = None,
    dry_run: DryRunOpt = False,
    assume_yes: YesOpt = False,
    json_out: JsonOpt = False,
    profile: ProfileOpt = None,
    log_level: LogLevelOpt = None,
    log_format: LogFormatOpt = "auto",
    config: ConfigOpt = None,
    target_file_size_bytes: Annotated[
        int | None,
        typer.Option(
            "--target-file-size",
            min=1,
            help="compaction target in bytes, applied to this session only",
        ),
    ] = None,
    rewrite_delete_threshold: Annotated[
        float | None,
        typer.Option(
            "--rewrite-delete-threshold",
            min=0.0,
            max=1.0,
            help="minimum fraction of deleted rows for the rewrite to rewrite the "
            "file, for this run only (0 = every file with a delete)",
        ),
    ] = None,
    closed_after_seconds: Annotated[
        int,
        typer.Option(
            "--closed-after",
            min=0,
            help="seconds without writes for the report to classify the partition "
            "as closed",
        ),
    ] = DEFAULT_CLOSED_AFTER_SECONDS,
    metrics_enabled: MetricsOpt = False,
    json_version: JsonVersionOpt = ENVELOPE_VERSION,
) -> None:
    """Table checkpoint in DuckLake: flush + rewrite + merge."""
    started = time.perf_counter()
    envelope = Envelope(command="checkpoint", dry_run=dry_run)
    try:
        settings = load_settings(
            config_file=config,
            overrides=_overrides(profile=profile, log_level=log_level),
        )
        envelope.config = settings.redacted()
        _configure_logging(settings.log_level, fmt=log_format, command="checkpoint")
        report: CheckpointRunReport | DryRunReport
        # one report for the whole scope: the metrics belong to the command
        with (
            _table_metrics(envelope, "checkpoint", table, enabled=metrics_enabled),
            timing.phase("checkpoint"),
        ):
            if dry_run:
                report = run_checkpoint(
                    settings,
                    table=table,
                    schema=schema,
                    all_tables=all_tables,
                    dry_run=True,
                    assume_yes=assume_yes,
                    target_file_size_bytes=target_file_size_bytes,
                    rewrite_delete_threshold=rewrite_delete_threshold,
                    closed_after_seconds=closed_after_seconds,
                )
            else:
                report = run_checkpoint(
                    settings,
                    table=table,
                    schema=schema,
                    all_tables=all_tables,
                    assume_yes=assume_yes,
                    confirm=None if json_out else _checkpoint_confirm,
                    target_file_size_bytes=target_file_size_bytes,
                    rewrite_delete_threshold=rewrite_delete_threshold,
                    closed_after_seconds=closed_after_seconds,
                )
            add_work("checkpoint", items=_checkpoint_work(report))
            payload = report.to_json()
            if table is not None:
                payload.setdefault("table", table)
            envelope.results.append(payload)
        envelope.set_exit(report.exit_code)
    except DuckLakeIcebergError as exc:
        envelope.problems.append(_problem_from_error(exc, table=table))
        envelope.set_exit(exc.exit_code)
    except _CLICK_CONTROL:
        raise
    except Exception as exc:
        _internal_error(envelope, exc)
    _finish(
        envelope,
        started,
        json_out=json_out,
        log_format=log_format,
        json_version=json_version,
    )


def _checkpoint_work(report: CheckpointRunReport | DryRunReport) -> int:
    """``items`` of the ``checkpoint`` phase: tables with work to do."""
    from ducklake_iceberg.checkpoint.dry_run import DryRunReport

    if isinstance(report, DryRunReport):
        return report.tables_with_work
    return sum(1 for result in report.results if result.changed)


MaterializeOpt = Annotated[
    bool,
    typer.Option(
        "--materialize-deletes",
        help="rewrite data files with deletes in DuckLake before the export "
        "(breaks zero-copy; resolves puffin delete files)",
    ),
]
DeleteThresholdOpt = Annotated[
    float,
    typer.Option(
        "--delete-threshold",
        min=0.0,
        max=1.0,
        help="minimum fraction of deleted rows to rewrite (with --materialize-deletes)",
    ),
]


@app.command()
def export(
    table: TableOpt = None,
    all_tables: AllTablesOpt = False,
    schema: SchemaOpt = None,
    dry_run: DryRunOpt = False,
    materialize: MaterializeOpt = False,
    delete_threshold: DeleteThresholdOpt = 0.0,
    json_out: JsonOpt = False,
    profile: ProfileOpt = None,
    log_level: LogLevelOpt = None,
    log_format: LogFormatOpt = "auto",
    config: ConfigOpt = None,
    max_footer_workers: Annotated[
        int | None, typer.Option("--max-footer-workers", min=1)
    ] = None,
    max_total_workers: Annotated[
        int | None, typer.Option("--max-total-workers", min=1)
    ] = None,
    max_io_workers: Annotated[
        int | None, typer.Option("--max-io-workers", min=1)
    ] = None,
    duckdb_threads: Annotated[
        int | None, typer.Option("--duckdb-threads", min=1)
    ] = None,
    metrics_enabled: MetricsOpt = False,
    json_version: JsonVersionOpt = ENVELOPE_VERSION,
) -> None:
    """Generate Iceberg metadata.json + manifests on S3.

    Only reads the catalog, except with --materialize-deletes."""
    started = time.perf_counter()
    envelope = Envelope(command="export", dry_run=dry_run)
    try:
        _validate_target_flags(table=table, all_tables=all_tables, schema=schema)
        settings = load_settings(
            config_file=config,
            overrides=_overrides(profile=profile, log_level=log_level),
        )
        export_updates = {
            key: value
            for key, value in {
                "max_footer_workers": max_footer_workers,
                "max_total_workers": max_total_workers,
                "max_io_workers": max_io_workers,
                "duckdb_threads": duckdb_threads,
            }.items()
            if value is not None
        }
        if export_updates:
            settings = settings.model_copy(update=export_updates)
        # export processes tables serially: one table at a time within the budget
        settings, _ = _apply_limits(settings, envelope, table_workers=1)
        envelope.config = settings.redacted()
        _configure_logging(settings.log_level, fmt=log_format, command="export")
        scope = _resolve_scope(settings, table=table, schema=schema)
        codes: list[ExitCode] = []
        for schema_name, table_name in scope:
            qualified = f"{schema_name}.{table_name}"
            with _table_metrics(
                envelope, "export", qualified, enabled=metrics_enabled
            ) as metrics:
                steps: list[dict[str, Any]] = []
                if materialize:
                    code = _materialize_step(
                        settings,
                        schema_name,
                        table_name,
                        dry_run=dry_run,
                        delete_threshold=delete_threshold,
                        steps=steps,
                        envelope=envelope,
                    )
                    if code not in (ExitCode.SUCCESS, ExitCode.NOTHING_TO_DO):
                        codes.append(code)
                        continue
                codes.append(
                    _export_one(
                        settings, schema_name, table_name, dry_run, envelope, metrics
                    )
                )
                if steps:
                    envelope.results.append({"table": qualified, "steps": steps})
        envelope.set_exit(aggregate_exit_code(codes))
    except DuckLakeIcebergError as exc:
        envelope.problems.append(_problem_from_error(exc, table=table))
        envelope.set_exit(exc.exit_code)
    except _CLICK_CONTROL:
        raise
    except Exception as exc:
        _internal_error(envelope, exc)
    _finish(
        envelope,
        started,
        json_out=json_out,
        log_format=log_format,
        json_version=json_version,
    )


def _export_one(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    dry_run: bool,
    envelope: Envelope,
    metrics: RunMetrics,
) -> ExitCode:
    qualified = f"{schema_name}.{table_name}"
    try:
        result = run_export(
            settings,
            schema_name,
            table_name,
            dry_run=dry_run,
            max_footer_workers=settings.max_footer_workers,
        )
        payload = result.to_json()
        # publication totals, added on top of the export keys
        payload.update(_publication_totals(payload, metrics))
        envelope.results.append(payload)
        return result.exit_code
    except Exception as exc:
        LOG.debug("export of %s failed", qualified, exc_info=exc)
        envelope.problems.append(_problem_from_error(exc, table=qualified))
        return _exit_of(exc)


def _publication_totals(
    export_json: dict[str, Any], metrics: RunMetrics
) -> dict[str, int]:
    """Publication totals computed from the export's ``to_json``.

    ``files_*`` count data files; delete files go in ``delete_files_added``.
    ``bytes_referenced`` is the sum of ``data_file_size_bytes`` of the published
    snapshot; ``bytes_written`` is the metadata and delete files written.
    """
    return {
        "files_added": int(export_json.get("added_data_files") or 0),
        "files_removed": int(export_json.get("removed_data_files") or 0),
        "delete_files_added": int(export_json.get("added_delete_files") or 0),
        "bytes_referenced": int(export_json.get("total_files_size") or 0),
        "bytes_written": metrics.bytes_written(),
    }


def _materialize_step(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    *,
    dry_run: bool,
    delete_threshold: float,
    steps: list[dict[str, Any]],
    envelope: Envelope,
) -> ExitCode:
    """``--materialize-deletes``: exit 4 (nothing to materialize) carries on."""
    from ducklake_iceberg.deletes.materialize import run_materialize

    qualified = f"{schema_name}.{table_name}"
    try:
        detail = run_materialize(
            settings,
            schema_name,
            table_name,
            dry_run=dry_run,
            delete_threshold=delete_threshold,
        )
        steps.append(
            {"step": "materialize", "exit_code": int(ExitCode.SUCCESS), **detail}
        )
        return ExitCode.SUCCESS
    except NothingToDo as exc:
        steps.append(
            {
                "step": "materialize",
                "exit_code": int(exc.exit_code),
                "detail": exc.message,
            }
        )
        return ExitCode.NOTHING_TO_DO
    except Exception as exc:
        steps.append({"step": "materialize", "exit_code": int(_exit_of(exc))})
        envelope.problems.append(_problem_from_error(exc, table=qualified))
        return _exit_of(exc)


@app.command()
def register(
    table: TableOpt = None,
    all_tables: AllTablesOpt = False,
    schema: SchemaOpt = None,
    metadata_location: Annotated[
        str | None,
        typer.Option("--metadata-location", help="explicit metadata.json"),
    ] = None,
    create_database: Annotated[
        bool, typer.Option("--create-database/--no-create-database")
    ] = True,
    dry_run: DryRunOpt = False,
    json_out: JsonOpt = False,
    profile: ProfileOpt = None,
    log_level: LogLevelOpt = None,
    log_format: LogFormatOpt = "auto",
    config: ConfigOpt = None,
    metrics_enabled: MetricsOpt = False,
    json_version: JsonVersionOpt = ENVELOPE_VERSION,
) -> None:
    """Publish the table to Glue. Only talks to Glue; no DuckDB connection."""
    started = time.perf_counter()
    envelope = Envelope(command="register", dry_run=dry_run)
    try:
        _validate_target_flags(table=table, all_tables=all_tables, schema=schema)
        if metadata_location is not None and all_tables:
            raise ConfigError("--metadata-location only applies to a single table")
        settings = load_settings(
            config_file=config,
            overrides=_overrides(profile=profile, log_level=log_level),
        )
        envelope.config = settings.redacted()
        _configure_logging(settings.log_level, fmt=log_format, command="register")
        scope = _resolve_scope(settings, table=table, schema=schema)
        codes: list[ExitCode] = []
        for schema_name, table_name in scope:
            with _table_metrics(
                envelope,
                "register",
                f"{schema_name}.{table_name}",
                enabled=metrics_enabled,
            ):
                codes.append(
                    _register_one(
                        settings,
                        schema_name,
                        table_name,
                        metadata_location=metadata_location,
                        create_database=create_database,
                        dry_run=dry_run,
                        envelope=envelope,
                    )
                )
        envelope.set_exit(aggregate_exit_code(codes))
    except DuckLakeIcebergError as exc:
        envelope.problems.append(_problem_from_error(exc, table=table))
        envelope.set_exit(exc.exit_code)
    except _CLICK_CONTROL:
        raise
    except Exception as exc:
        _internal_error(envelope, exc)
    _finish(
        envelope,
        started,
        json_out=json_out,
        log_format=log_format,
        json_version=json_version,
    )


def _register_one(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    *,
    metadata_location: str | None,
    create_database: bool,
    dry_run: bool,
    envelope: Envelope,
) -> ExitCode:
    from ducklake_iceberg.glue import RegisterOutcome, normalize_table_name

    qualified = f"{schema_name}.{table_name}"
    glue_table = normalize_table_name(table_name)
    try:
        with timing.phase("register"):
            result = run_register(
                settings,
                schema_name,
                table_name,
                metadata_location=metadata_location,
                create_database=create_database,
                dry_run=dry_run,
            )
            add_work("glue", items=1)
        envelope.results.append(
            {
                "table": qualified,
                "glue_database": settings.glue_database,
                "glue_table": glue_table,
                "created": result.outcome is RegisterOutcome.REGISTERED,
                "metadata_location": result.metadata_location,
                "previous_metadata_location": result.previous_metadata_location,
                "outcome": str(result.outcome),
                "database_created": result.database_created,
                "lake_formation": result.lake_formation,
                "dry_run": dry_run,
                "exit_code": int(ExitCode.SUCCESS),
            }
        )
        log_event(
            "glue.register",
            table=qualified,
            metadata_location=result.metadata_location,
            outcome=str(result.outcome),
        )
        return ExitCode.SUCCESS
    except NothingToDo as exc:
        # table already published at this metadata.json: a rerun, exit 4
        envelope.results.append(
            {
                "table": qualified,
                "glue_database": settings.glue_database,
                "glue_table": glue_table,
                "created": False,
                "metadata_location": getattr(exc, "metadata_location", None)
                or metadata_location,
                "outcome": "already-exists",
                "detail": exc.message,
            }
        )
        return exc.exit_code
    except Exception as exc:
        LOG.debug("register of %s failed", qualified, exc_info=exc)
        envelope.problems.append(_problem_from_error(exc, table=qualified))
        return _exit_of(exc)


@app.command()
def sync(
    table: TableOpt = None,
    all_tables: AllTablesOpt = False,
    schema: SchemaOpt = None,
    dry_run: DryRunOpt = False,
    skip_checkpoint: Annotated[
        bool, typer.Option("--skip-checkpoint", help="do not run the checkpoint")
    ] = False,
    materialize: MaterializeOpt = False,
    delete_threshold: DeleteThresholdOpt = 0.0,
    no_lock: Annotated[
        bool, typer.Option("--no-lock", help="do not acquire the process lock")
    ] = False,
    compact: Annotated[
        bool,
        typer.Option(
            "--compact",
            help="full checkpoint (expire/merge/rewrite), not just the flush",
        ),
    ] = False,
    target_file_size_bytes: Annotated[
        int | None,
        typer.Option(
            "--target-file-size",
            min=1,
            help="compaction target in bytes; requires --compact and the checkpoint",
        ),
    ] = None,
    assume_yes: YesOpt = False,
    metrics_enabled: Annotated[
        bool, typer.Option("--metrics/--no-metrics", help="include per-phase metrics")
    ] = True,
    json_out: JsonOpt = False,
    profile: ProfileOpt = None,
    log_level: LogLevelOpt = None,
    log_format: LogFormatOpt = "auto",
    config: ConfigOpt = None,
    max_table_workers: Annotated[
        int | None, typer.Option("--max-table-workers", min=1)
    ] = None,
    max_footer_workers: Annotated[
        int | None, typer.Option("--max-footer-workers", min=1)
    ] = None,
    max_total_workers: Annotated[
        int | None, typer.Option("--max-total-workers", min=1)
    ] = None,
    max_io_workers: Annotated[
        int | None, typer.Option("--max-io-workers", min=1)
    ] = None,
    duckdb_threads: Annotated[
        int | None, typer.Option("--duckdb-threads", min=1)
    ] = None,
    glue_stats: Annotated[Literal["off", "on"], typer.Option("--glue-stats")] = "off",
    glue_stats_ndv: Annotated[
        Literal["off", "approx"], typer.Option("--glue-stats-ndv")
    ] = "approx",
    glue_stats_sample: Annotated[
        float | None, typer.Option("--glue-stats-sample", min=0.01, max=100)
    ] = None,
    fail_fast: Annotated[
        bool,
        typer.Option(
            "--fail-fast",
            help="with --all-tables, the 1st failure cancels tables not yet started",
        ),
    ] = False,
    json_version: JsonVersionOpt = ENVELOPE_VERSION,
) -> None:
    """doctor + checkpoint + export + register, with stop points per stage."""
    started = time.perf_counter()
    envelope = Envelope(command="sync", dry_run=dry_run)
    try:
        _validate_target_flags(table=table, all_tables=all_tables, schema=schema)
        if target_file_size_bytes is not None and (not compact or skip_checkpoint):
            raise ConfigError(
                "--target-file-size requires --compact without --skip-checkpoint"
            )
        settings = load_settings(
            config_file=config,
            overrides=_overrides(profile=profile, log_level=log_level),
        )
        setting_updates = {
            key: value
            for key, value in {
                "max_table_workers": max_table_workers,
                "max_footer_workers": max_footer_workers,
                "max_total_workers": max_total_workers,
                "max_io_workers": max_io_workers,
                "duckdb_threads": duckdb_threads,
            }.items()
            if value is not None
        }
        if setting_updates:
            settings = settings.model_copy(update=setting_updates)
        envelope.config = settings.redacted()
        run_id = _configure_logging(settings.log_level, fmt=log_format, command="sync")
        scope = _resolve_scope(settings, table=table, schema=schema)
        settings, limits = _apply_limits(
            settings,
            envelope,
            table_workers=settings.max_table_workers if all_tables else 1,
        )
        if all_tables and settings.duckdb_threads is None:
            # DuckDB internal parallelism is split among the tables
            settings = settings.model_copy(
                update={"duckdb_threads": limits.duckdb_threads or 1}
            )
        envelope.config = settings.redacted()
        interrupted = False
        try:
            outcomes = map_parallel(
                lambda target: _sync_table(
                    settings,
                    target[0],
                    target[1],
                    run_id=run_id,
                    dry_run=dry_run,
                    skip_checkpoint=skip_checkpoint,
                    materialize=materialize,
                    delete_threshold=delete_threshold,
                    no_lock=no_lock,
                    assume_yes=assume_yes,
                    compact=compact,
                    target_file_size_bytes=target_file_size_bytes,
                    metrics_enabled=metrics_enabled,
                    glue_stats=glue_stats,
                    glue_stats_ndv=glue_stats_ndv,
                    glue_stats_sample=glue_stats_sample,
                ),
                scope,
                workers=limits.max_table_workers if all_tables else 1,
                fail_fast=fail_fast,
                exit_code_of=lambda value: int(value[0]),
            )
        except Interrupted as exc:
            outcomes = exc.outcomes
            interrupted = True
        consolidated = ConsolidatedReport(command="sync")
        not_started: dict[str, list[str]] = {CANCELLED: [], INTERRUPTED: []}
        for outcome in outcomes:
            if outcome.value is None:
                table_ref = f"{outcome.item[0]}.{outcome.item[1]}"
                error = scrub(outcome.error or "sync failed")
                if outcome.error in not_started:
                    not_started[outcome.error].append(table_ref)
                else:
                    envelope.problems.append(
                        Problem(code="sync_failed", message=error, table=table_ref)
                    )
                envelope.results.append(
                    {
                        "table": table_ref,
                        "exit_code": int(ExitCode.ERROR),
                        "duration_ms": outcome.duration_ms,
                        "error": error,
                    }
                )
                consolidated.outcomes.append(
                    Outcome(
                        outcome.item,
                        None,
                        int(ExitCode.ERROR),
                        outcome.error,
                        outcome.duration_ms,
                    )
                )
                continue
            code, table_envelope = outcome.value
            envelope.results.extend(table_envelope.results)
            envelope.problems.extend(table_envelope.problems)
            consolidated.outcomes.append(
                Outcome(
                    outcome.item,
                    table_envelope.results,
                    int(code),
                    duration_ms=outcome.duration_ms,
                )
            )
        # deterministic results[]: sorted by name, not by completion time
        envelope.results.sort(key=lambda item: str(item.get("table", "")))
        if not_started[CANCELLED]:
            envelope.problems.append(
                Problem(
                    code="cancelled",
                    message=(
                        f"--fail-fast: {len(not_started[CANCELLED])} table(s) "
                        f"not started: {', '.join(not_started[CANCELLED])}"
                    ),
                    hint="fix the failure and rerun sync",
                )
            )
        if interrupted:
            envelope.problems.append(
                Problem(
                    code="interrupted",
                    message=(
                        "sync interrupted (SIGINT): "
                        f"{len(not_started[INTERRUPTED])} table(s) not "
                        "started; those in progress were completed"
                    ),
                    hint="rerun sync; already-published tables give exit 4",
                )
            )
        envelope.set_exit(
            ExitCode.ERROR if interrupted else ExitCode(consolidated.exit_code)
        )
    except DuckLakeIcebergError as exc:
        envelope.problems.append(_problem_from_error(exc, table=table))
        envelope.set_exit(exc.exit_code)
    except _CLICK_CONTROL:
        raise
    except Exception as exc:
        _internal_error(envelope, exc)
    _finish(
        envelope,
        started,
        json_out=json_out,
        log_format=log_format,
        json_version=json_version,
    )


def _sync_table(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    *,
    run_id: str,
    dry_run: bool,
    skip_checkpoint: bool,
    materialize: bool,
    delete_threshold: float,
    no_lock: bool,
    assume_yes: bool,
    compact: bool,
    target_file_size_bytes: int | None,
    metrics_enabled: bool,
    glue_stats: Literal["off", "on"],
    glue_stats_ndv: Literal["off", "approx"],
    glue_stats_sample: float | None,
) -> tuple[ExitCode, Envelope]:
    qualified = f"{schema_name}.{table_name}"
    table_envelope = Envelope(command="sync", dry_run=dry_run)
    log_event("table.start", table=qualified)
    metrics = RunMetrics(run_id, "sync", table=qualified)
    with bind(table=qualified), collect(metrics):
        code = _sync_locked(
            settings,
            schema_name,
            table_name,
            dry_run=dry_run,
            skip_checkpoint=skip_checkpoint,
            materialize=materialize,
            delete_threshold=delete_threshold,
            no_lock=no_lock,
            assume_yes=assume_yes,
            compact=compact,
            target_file_size_bytes=target_file_size_bytes,
            glue_stats=glue_stats,
            glue_stats_ndv=glue_stats_ndv,
            glue_stats_sample=glue_stats_sample,
            envelope=table_envelope,
        )
    result = next(
        (
            item
            for item in reversed(table_envelope.results)
            if item.get("table") == qualified
        ),
        None,
    )
    export_step: dict[str, Any] = next(
        (
            step
            for step in (result or {}).get("steps", [])
            if step.get("step") == "export"
        ),
        {},
    )
    totals = _publication_totals(export_step, metrics)
    if result is not None:
        result.update(
            {
                "exit_code": int(code),
                "duration_ms": metrics.duration_ms,
                "ducklake_snapshot_id": export_step.get("ducklake_snapshot_id"),
                "iceberg_snapshot_id": export_step.get("iceberg_snapshot_id"),
                "metadata_location": export_step.get("metadata_location"),
                **totals,
            }
        )
    if result is not None and metrics_enabled:
        metrics.summary = dict(totals)
        result["metrics"] = metrics.to_json()
    log_event(
        "sync.end",
        table=qualified,
        snapshot_id=export_step.get("ducklake_snapshot_id"),
        iceberg_snapshot_id=export_step.get("iceberg_snapshot_id"),
        duration_ms=metrics.duration_ms,
        dry_run=dry_run,
        exit_code=int(code),
        **totals,
    )
    log_event("table.end", table=qualified, exit_code=int(code))
    return code, table_envelope


def _doctor_gate(
    settings: DuckLakeIcebergSettings,
    qualified: str,
    *,
    before_checkpoint: bool = False,
    at_export_target: bool = False,
    con: DuckLakeConnection | None = None,
) -> ExitCode:
    """Run the preflight rules for a table and return the ExitCode.

    Opens the catalog READ_ONLY and runs the rules in the read transaction.
    The exit code follows the doctor mapping (0 ok, 2 blocker). Before the
    checkpoint, auto-remediable gates (``inlined-data``) do not block: the
    checkpoint itself resolves them, and the post-checkpoint revalidation
    runs all the rules again on the snapshot that will be exported. With
    ``at_export_target``, the rules run on that snapshot (the most recent one
    with no pending inlined data), not the current one: it is what the export
    will publish.
    """
    from ducklake_iceberg.checkpoint.detect import clean_snapshot
    from ducklake_iceberg.checkpoint.model import TableRef as CheckpointTableRef
    from ducklake_iceberg.doctor.report import AUTO_REMEDIABLE_GATES

    ref = parse_table_ref(qualified)
    with metadata_catalog(settings, con) as ro, ro.read_transaction():
        snapshot = current_snapshot(ro)
        identity = resolve_table(ro, ref, snapshot)
        if at_export_target:
            target = clean_snapshot(
                ro.raw,
                ro.meta,
                CheckpointTableRef(
                    identity.ref.schema_name, identity.ref.table_name, identity.table_id
                ),
                snapshot.snapshot_id,
            )
            if target != snapshot.snapshot_id:
                snapshot = snapshot_at(ro, target)
                identity = resolve_table(ro, ref, snapshot)
        report = _run_rules(ro, identity)
    if before_checkpoint and not report.has_error:
        from ducklake_iceberg.doctor.model import Severity

        blocking = [
            f
            for r in report.results
            if r.rule_id not in AUTO_REMEDIABLE_GATES
            for f in r.findings
            if f.severity is Severity.BLOCKER
        ]
        return ExitCode.BLOCKED if blocking else ExitCode.SUCCESS
    return _exit_code_from_report(report)


def _sync_locked(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    *,
    no_lock: bool,
    envelope: Envelope,
    **kwargs: Any,
) -> ExitCode:
    """``_sync_one`` under the table's process lock."""
    from ducklake_iceberg.glue import normalize_table_name
    from ducklake_iceberg.sync.lock import _lock_key, table_lock

    key = _lock_key(
        profile=settings.aws_profile,
        glue_database=settings.glue_database,
        glue_table=normalize_table_name(table_name),
    )
    qualified = f"{schema_name}.{table_name}"
    try:
        with table_lock(
            key,
            metadata={"table": qualified},
            skip=no_lock or bool(kwargs.get("dry_run")),
        ):
            return _sync_one(
                settings, schema_name, table_name, envelope=envelope, **kwargs
            )
    except DuckLakeIcebergError as exc:
        envelope.problems.append(_problem_from_error(exc, table=qualified))
        return exc.exit_code


class _SharedCatalog:
    """READ_ONLY metadata connection of a ``sync``, lent to the stages.

    Opened on demand and without S3 (doctor, detection and enumeration only
    read the catalog); reopened after any write to DuckLake (flush,
    materialize), like a re-read on a fresh connection.
    """

    def __init__(self, settings: DuckLakeIcebergSettings) -> None:
        self._settings = settings
        self._con: DuckLakeConnection | None = None

    def get(self) -> DuckLakeConnection:
        if self._con is None:
            self._con = open_catalog(self._settings, read_only=True, s3=False)
        return self._con

    def invalidate(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None


def _sync_one(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    *,
    dry_run: bool,
    skip_checkpoint: bool,
    assume_yes: bool,
    envelope: Envelope,
    materialize: bool = False,
    delete_threshold: float = 0.0,
    compact: bool = False,
    target_file_size_bytes: int | None = None,
    glue_stats: Literal["off", "on"] = "off",
    glue_stats_ndv: Literal["off", "approx"] = "approx",
    glue_stats_sample: float | None = None,
) -> ExitCode:
    """Chain the stages, with their stop points."""
    catalog = _SharedCatalog(settings)
    try:
        return _sync_steps(
            settings,
            schema_name,
            table_name,
            catalog,
            dry_run=dry_run,
            skip_checkpoint=skip_checkpoint,
            assume_yes=assume_yes,
            envelope=envelope,
            materialize=materialize,
            delete_threshold=delete_threshold,
            compact=compact,
            target_file_size_bytes=target_file_size_bytes,
            glue_stats=glue_stats,
            glue_stats_ndv=glue_stats_ndv,
            glue_stats_sample=glue_stats_sample,
        )
    finally:
        catalog.invalidate()


def _sync_steps(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    catalog: _SharedCatalog,
    *,
    dry_run: bool,
    skip_checkpoint: bool,
    assume_yes: bool,
    envelope: Envelope,
    materialize: bool,
    delete_threshold: float,
    compact: bool,
    target_file_size_bytes: int | None,
    glue_stats: Literal["off", "on"],
    glue_stats_ndv: Literal["off", "approx"],
    glue_stats_sample: float | None,
) -> ExitCode:
    qualified = f"{schema_name}.{table_name}"
    steps: list[dict[str, Any]] = []
    envelope.results.append({"table": qualified, "steps": steps})

    # doctor stage: a compatibility blocker (exit 2) stops the sync.
    with timing.phase("doctor"):
        doctor_exit = _doctor_gate(
            settings,
            qualified,
            before_checkpoint=not skip_checkpoint,
            con=catalog.get(),
        )
    steps.append({"step": "doctor", "exit_code": int(doctor_exit)})
    if doctor_exit is ExitCode.BLOCKED:
        log_event(
            "compat.blocked",
            level=logging.ERROR,
            table=qualified,
            exit_code=int(doctor_exit),
        )
        return doctor_exit
    if doctor_exit is ExitCode.ERROR:
        envelope.problems.append(
            _problem_from_error(
                ConfigError(f"doctor failed on {qualified}"), table=qualified
            )
        )
        return doctor_exit

    # checkpoint stage. Skipped with --skip-checkpoint; exit 4 does not stop it.
    if not skip_checkpoint:
        # without a DuckLake write the snapshot did not change and the doctor
        # above already covers it; only a write (or the dry-run, which does not
        # write) requires re-reading.
        revalidate = True
        try:
            report: CheckpointRunReport | DryRunReport
            with timing.phase("checkpoint"):
                if dry_run:
                    report = run_checkpoint(
                        settings,
                        table=qualified,
                        dry_run=True,
                        assume_yes=assume_yes,
                        con=catalog.get(),
                        target_file_size_bytes=target_file_size_bytes,
                    )
                else:
                    report = run_checkpoint(
                        settings,
                        table=qualified,
                        dry_run=False,
                        assume_yes=assume_yes,
                        con=catalog.get(),
                        compact=compact,
                        target_file_size_bytes=target_file_size_bytes,
                    )
                    revalidate = report.wrote
                    if report.wrote:
                        catalog.invalidate()
                add_work("checkpoint", items=_checkpoint_work(report))
            checkpoint_exit = ExitCode(report.exit_code)
            steps.append({"step": "checkpoint", "exit_code": int(checkpoint_exit)})
            if checkpoint_exit in (ExitCode.ERROR, ExitCode.DIVERGED):
                return checkpoint_exit  # stop
            # exit 0 or 4 (nothing to do) -> carry on
        except NothingToDo as exc:
            steps.append(
                {"step": "checkpoint", "exit_code": int(exc.exit_code)}
            )  # carry on
        except DuckLakeIcebergError as exc:
            steps.append({"step": "checkpoint", "exit_code": int(exc.exit_code)})
            envelope.problems.append(_problem_from_error(exc, table=qualified))
            return exc.exit_code

        # post-checkpoint revalidation: the flush may expose a blocked type.
        if revalidate:
            with timing.phase("doctor"):
                revalidate_exit = _doctor_gate(
                    settings, qualified, at_export_target=True, con=catalog.get()
                )
            steps.append({"step": "doctor", "exit_code": int(revalidate_exit)})
            if revalidate_exit in (ExitCode.BLOCKED, ExitCode.DIVERGED):
                return revalidate_exit

    # materialize stage: only with the flag; exit 4 (nothing to do) carries on
    if materialize:
        code = _materialize_step(
            settings,
            schema_name,
            table_name,
            dry_run=dry_run,
            delete_threshold=delete_threshold,
            steps=steps,
            envelope=envelope,
        )
        if code not in (ExitCode.SUCCESS, ExitCode.NOTHING_TO_DO):
            return code
        if code is ExitCode.SUCCESS:
            catalog.invalidate()

    # export stage: incremental against the latest metadata.json
    try:
        with timing.phase("export"):
            result = run_export(
                settings,
                schema_name,
                table_name,
                dry_run=dry_run,
                con=catalog.get(),
                max_footer_workers=settings.max_footer_workers,
            )
        steps.append({"step": "export", **result.to_json()})
    except Exception as exc:
        LOG.debug("export of %s failed", qualified, exc_info=exc)
        steps.append({"step": "export", "exit_code": int(_exit_of(exc))})
        envelope.problems.append(_problem_from_error(exc, table=qualified))
        return _exit_of(exc)
    catalog.invalidate()

    if dry_run:
        # register --dry-run requires the real metadata.json
        if glue_stats == "on":
            stats_code = _run_glue_stats(
                settings,
                schema_name,
                table_name,
                snapshot_version=result.plan.ducklake_snapshot_id,
                bytes_scanned=result.plan.total_files_size,
                catalog=catalog,
                envelope=envelope,
                steps=steps,
                ndv=glue_stats_ndv,
                sample_percent=glue_stats_sample,
                dry_run=True,
            )
            if stats_code is not ExitCode.SUCCESS:
                return stats_code
        return result.exit_code

    # register stage. It runs even when export exits 4: a previous sync may have
    # written the metadata.json and failed before reaching Glue.
    try:
        with timing.phase("register"):
            reg = run_register(
                settings,
                schema_name,
                table_name,
                handoff=result.handoff,
                metadata=result.metadata,
            )
            add_work("glue", items=1)
        steps.append(
            {
                "step": "register",
                "exit_code": int(ExitCode.SUCCESS),
                "outcome": str(reg.outcome),
                "metadata_location": reg.metadata_location,
            }
        )
        log_event(
            "glue.register",
            table=qualified,
            metadata_location=reg.metadata_location,
            outcome=str(reg.outcome),
        )
        if glue_stats == "on":
            return _run_glue_stats(
                settings,
                schema_name,
                table_name,
                snapshot_version=result.plan.ducklake_snapshot_id,
                bytes_scanned=result.plan.total_files_size,
                catalog=catalog,
                envelope=envelope,
                steps=steps,
                ndv=glue_stats_ndv,
                sample_percent=glue_stats_sample,
                dry_run=False,
            )
        return ExitCode.SUCCESS
    except NothingToDo as exc:
        steps.append({"step": "register", "exit_code": int(exc.exit_code)})
        if glue_stats == "on":
            stats_code = _run_glue_stats(
                settings,
                schema_name,
                table_name,
                snapshot_version=result.plan.ducklake_snapshot_id,
                bytes_scanned=result.plan.total_files_size,
                catalog=catalog,
                envelope=envelope,
                steps=steps,
                ndv=glue_stats_ndv,
                sample_percent=glue_stats_sample,
                dry_run=False,
            )
            # publishing statistics is not a sync "change": a table with no delta
            # stays at exit 4; only a statistics failure changes the code
            if stats_code is not ExitCode.SUCCESS:
                return stats_code
        # already published: sync only "did something" if the export wrote
        return (
            result.exit_code
            if result.exit_code is ExitCode.SUCCESS
            else (exc.exit_code)
        )
    except Exception as exc:
        LOG.debug("register of %s failed", qualified, exc_info=exc)
        steps.append({"step": "register", "exit_code": int(_exit_of(exc))})
        envelope.problems.append(_problem_from_error(exc, table=qualified))
        return _exit_of(exc)


def _run_glue_stats(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    *,
    snapshot_version: int,
    bytes_scanned: int,
    catalog: _SharedCatalog,
    envelope: Envelope,
    steps: list[dict[str, Any]],
    ndv: Literal["off", "approx"],
    sample_percent: float | None,
    dry_run: bool,
) -> ExitCode:
    """Collect and publish column statistics of the published snapshot.

    ``bytes_scanned`` is the sum of the snapshot's data files (what the scan
    reads in the worst case), used for the ``bytes`` metric of the
    ``glue_stats`` phase. A table with no data files (inlined only) skips the
    statistics.
    """
    from ducklake_iceberg.aws import client as aws_client
    from ducklake_iceberg.glue import normalize_table_name
    from ducklake_iceberg.glue_stats import (
        collect_column_statistics,
        plan_column_statistics,
        publish_column_statistics,
    )

    qualified = f"{schema_name}.{table_name}"
    if bytes_scanned <= 0:
        steps.append(
            {
                "step": "glue_stats",
                "table": qualified,
                "outcome": "skipped",
                "reason": "no_data_files",
            }
        )
        return ExitCode.SUCCESS
    try:
        with timing.phase("glue_stats"):
            glue_table = normalize_table_name(table_name)
            iceberg_table = _glue_catalog(settings).load_table(
                (settings.glue_database, glue_table)
            )
            plans = plan_column_statistics(iceberg_table.schema(), ndv=ndv)
            skipped = [
                {"column": plan.column_name, "reason": plan.skip_reason}
                for plan in plans
                if plan.skip_reason is not None
            ]
            # AnalyzedTime = start of the scan, in UTC
            analyzed_at = datetime.now(UTC)
            stats = collect_column_statistics(
                catalog.get(),
                table_ref=(f"{settings.catalog_alias}.{schema_name}.{table_name}"),
                snapshot_version=snapshot_version,
                plans=plans,
                sample_percent=sample_percent,
            )
            if dry_run:
                report: dict[str, Any] = {
                    "table": qualified,
                    "columns_collected": len(stats),
                    "columns_skipped": skipped,
                    "sampled_percent": sample_percent,
                    "ndv_estimated": any(
                        stat.glue_type not in ("BOOLEAN", "BINARY") for stat in stats
                    ),
                    "analyzed_at": analyzed_at.isoformat(),
                    "outcome": "dry_run",
                }
                add_work("glue_stats", items=len(stats), byte_count=bytes_scanned)
            else:
                report_obj = publish_column_statistics(
                    aws_client("glue", settings.aws_region, settings.aws_profile),
                    database=settings.glue_database,
                    table=glue_table,
                    stats=stats,
                    analyzed_at=analyzed_at,
                )
                add_work(
                    "glue_stats",
                    items=report_obj.columns_published,
                    byte_count=bytes_scanned,
                )
                report_obj.columns_skipped.extend(
                    (item["column"], item["reason"]) for item in skipped
                )
                report_obj.sampled_percent = sample_percent
                report = asdict(report_obj)
                # an error on one column does not abort the others: with some
                # column published it is a warning; with none, an error (exit 1)
                level: Literal["error", "warning"] = (
                    "warning" if report_obj.columns_published else "error"
                )
                for error in report_obj.errors:
                    envelope.problems.append(
                        Problem(
                            code="glue_column_stats",
                            message=(f"{error['column']}: {scrub(error['message'])}"),
                            table=qualified,
                            level=level,
                        )
                    )
        steps.append({"step": "glue_stats", **report})
        # no dedicated event: the event catalog is closed and the `phase.end`
        # of the `glue_stats` phase already carries the counters
        LOG.info(
            "Glue statistics for %s: %s published, %d skipped%s",
            qualified,
            report.get("columns_published", report.get("columns_collected", 0)),
            len(skipped),
            " (dry-run)" if dry_run else "",
        )
        if (
            not dry_run
            and report.get("errors")
            and report.get("columns_published", 0) == 0
        ):
            return ExitCode.ERROR
        return ExitCode.SUCCESS
    except Exception as exc:
        LOG.debug("Glue statistics for %s failed", qualified, exc_info=exc)
        steps.append({"step": "glue_stats", "exit_code": int(_exit_of(exc))})
        envelope.problems.append(_problem_from_error(exc, table=qualified))
        return _exit_of(exc)


NoDeltaOpt = Annotated[
    bool, typer.Option("--no-delta", help="skip the pending-changes count")
]
ExitZeroOpt = Annotated[
    bool,
    typer.Option(
        "--exit-zero",
        help="always exit 0 (diagnostics in the JSON)",
    ),
]


@app.command()
def status(
    table: TableOpt = None,
    all_tables: AllTablesOpt = False,
    schema: SchemaOpt = None,
    no_delta: NoDeltaOpt = False,
    exit_zero: ExitZeroOpt = False,
    json_out: JsonOpt = False,
    profile: ProfileOpt = None,
    log_level: LogLevelOpt = None,
    log_format: LogFormatOpt = "auto",
    config: ConfigOpt = None,
    metrics_enabled: MetricsOpt = False,
    json_version: JsonVersionOpt = ENVELOPE_VERSION,
) -> None:
    """DuckLake vs Glue: snapshots and divergence."""
    started = time.perf_counter()
    envelope = Envelope(command="status", dry_run=False)
    try:
        _validate_target_flags(table=table, all_tables=all_tables, schema=schema)
        settings = load_settings(
            config_file=config,
            overrides=_overrides(profile=profile, log_level=log_level),
        )
        envelope.config = settings.redacted()
        _configure_logging(settings.log_level, fmt=log_format, command="status")
        _run_status(
            settings,
            envelope,
            table=table,
            schema=schema,
            no_delta=no_delta,
            exit_zero=exit_zero,
            json_out=json_out,
            metrics_enabled=metrics_enabled,
        )
    except DuckLakeIcebergError as exc:
        envelope.problems.append(_problem_from_error(exc))
        envelope.set_exit(exc.exit_code)
    except _CLICK_CONTROL:
        raise
    except Exception as exc:
        _internal_error(envelope, exc)
    _finish(
        envelope,
        started,
        json_out=json_out,
        log_format=log_format,
        json_version=json_version,
    )


def _glue_catalog(settings: DuckLakeIcebergSettings) -> GlueCatalog:
    """``GlueCatalog`` with the same properties as ``register`` (glue.region,
    glue.profile-name)."""
    from ducklake_iceberg.glue import GlueTarget, build_catalog

    return build_catalog(
        GlueTarget(
            database=settings.glue_database,
            table="",  # the catalog is per account/region; the table goes per call
            region=settings.aws_region,
            profile=settings.aws_profile,
            catalog_id=None,
        )
    )


def _run_status(
    settings: DuckLakeIcebergSettings,
    envelope: Envelope,
    *,
    table: str | None,
    schema: str | None,
    no_delta: bool,
    exit_zero: bool,
    json_out: bool,
    metrics_enabled: bool = False,
) -> None:
    """Run the status command for one or more tables. Never writes.

    An unreadable table is recorded as ``UNREADABLE`` (exit 1) and the scan
    continues; errors outside the matrix (e.g. a table missing on both sides)
    become ``problems`` with the error's exit code. With ``--json`` the status
    document is the only result in the envelope (a single JSON on stdout).
    """
    from ducklake_iceberg import report
    from ducklake_iceberg.glue import normalize_table_name
    from ducklake_iceberg.status import TableStatus, table_status

    scope = _resolve_scope(settings, table=table, schema=schema)
    catalog = _glue_catalog(settings)

    statuses: list[TableStatus] = []
    codes: list[ExitCode] = []
    table_metrics: dict[str, RunMetrics] = {}
    with open_catalog(settings, read_only=True) as con, con.read_transaction():
        for schema_name, table_name in scope:
            qualified = f"{schema_name}.{table_name}"
            metrics = RunMetrics(current_run_id(), "status", table=qualified)
            table_metrics[qualified] = metrics
            with bind(table=qualified), collect(metrics):
                try:
                    # identity and snapshot of both sides (DuckLake and Glue)
                    with timing.phase("resolve"):
                        status_obj = table_status(
                            con,
                            catalog,
                            ducklake_schema=schema_name,
                            ducklake_table=table_name,
                            glue_identifier=(
                                settings.glue_database,
                                normalize_table_name(table_name),
                            ),
                            with_delta=not no_delta,
                        )
                        add_work("resolve", items=1)
                except Exception as exc:
                    LOG.debug("status of %s failed", qualified, exc_info=exc)
                    envelope.problems.append(_problem_from_error(exc, table=qualified))
                    codes.append(_exit_of(exc))
                    continue
            statuses.append(status_obj)
            codes.append(ExitCode(status_obj.exit_code))

    if json_out:
        document = report.render_json(statuses)
        if metrics_enabled:
            # the status document is the only result: the metrics go per
            # table, in ``tables[]``
            for entry in cast(list[dict[str, Any]], document["tables"]):
                found = table_metrics.get(str(entry.get("table")))
                if found is not None:
                    entry["metrics"] = found.to_json()
        envelope.results.append(document)
    else:
        report.render_rich(statuses)

    aggregated = aggregate_exit_code(codes)
    envelope.set_exit(ExitCode.SUCCESS if exit_zero else aggregated)


@app.command()
def validate(
    table: TableOpt = None,
    all_tables: AllTablesOpt = False,
    schema: SchemaOpt = None,
    dry_run: DryRunOpt = False,
    json_out: JsonOpt = False,
    profile: ProfileOpt = None,
    log_level: LogLevelOpt = None,
    log_format: LogFormatOpt = "auto",
    config: ConfigOpt = None,
    deep: Annotated[
        bool, typer.Option("--deep", help="count rows by reading the Iceberg files")
    ] = False,
    sample_rows: Annotated[
        int, typer.Option("--sample-rows", min=1, help="maximum sample size")
    ] = 1_000,
    strict: Annotated[
        bool,
        typer.Option(
            "--strict",
            help="warnings (bounds, schema_drift, retention) become divergence",
        ),
    ] = False,
    timestamp_precision: Annotated[
        str,
        typer.Option(
            "--timestamp-precision",
            help="compared precision: us (default) or ms (Athena)",
        ),
    ] = "us",
    only: Annotated[
        Literal["all", "data", "retention"],
        typer.Option("--only", help="run all, data or retention"),
    ] = "all",
    check_s3: Annotated[
        Literal["off", "sample", "full"],
        typer.Option("--check-s3", help="check that the files exist"),
    ] = "off",
    s3_sample: Annotated[
        int, typer.Option("--s3-sample", min=1, help="S3 sample size")
    ] = 100,
    max_io_workers: Annotated[
        int, typer.Option("--max-io-workers", min=1, help="S3 HEAD parallelism")
    ] = 8,
    metrics_enabled: MetricsOpt = False,
    json_version: JsonVersionOpt = ENVELOPE_VERSION,
) -> None:
    """DuckLake/Iceberg reconciliation of counts, metrics and samples."""
    started = time.perf_counter()
    envelope = Envelope(command="validate", dry_run=dry_run)
    codes: list[ExitCode] = []
    try:
        _validate_target_flags(table=table, all_tables=all_tables, schema=schema)
        if timestamp_precision not in {"us", "ms"}:
            raise ConfigError("--timestamp-precision accepts only us or ms")
        settings = load_settings(
            config_file=config,
            overrides=_overrides(profile=profile, log_level=log_level),
        )
        envelope.config = settings.redacted()
        _configure_logging(settings.log_level, fmt=log_format, command="validate")
        from ducklake_iceberg.glue import GlueTarget, normalize_table_name
        from ducklake_iceberg.retention import no_snapshot_result, render_retention

        scope = _resolve_scope(settings, table=table, schema=schema)
        catalog = _glue_catalog(settings)
        options = ValidateOptions(
            deep_count=deep,
            sample_rows=sample_rows,
            strict=strict,
            tolerances=Tolerances(
                timestamp_precision=cast(Literal["us", "ms"], timestamp_precision)
            ),
        )
        with open_catalog(settings, read_only=True) as con, con.read_transaction():
            for schema_name, table_name in scope:
                qualified = f"{schema_name}.{table_name}"
                with _table_metrics(
                    envelope, "validate", qualified, enabled=metrics_enabled
                ):
                    try:
                        target = GlueTarget(
                            database=settings.glue_database,
                            table=normalize_table_name(table_name),
                            region=settings.aws_region,
                            profile=settings.aws_profile,
                            catalog_id=None,
                        )
                        with timing.phase("resolve"):
                            iceberg_table = catalog.load_table(target.identifier)
                            add_work("resolve", items=1)
                        if iceberg_table.current_snapshot() is None:
                            if only == "retention":
                                result = no_snapshot_result(qualified)
                                envelope.results.append(result)
                                codes.append(ExitCode.NOTHING_TO_DO)
                                continue
                            # empty on both sides is "nothing to validate" (exit 4);
                            # with rows in DuckLake, the table was just not
                            # published.
                            if lake_row_count(con, schema_name, table_name) == 0:
                                empty = empty_validation_result(qualified)
                                if only == "all":
                                    empty["retention"] = no_snapshot_result(qualified)
                                envelope.results.append(empty)
                                codes.append(ExitCode.NOTHING_TO_DO)
                                continue
                            raise ConfigError(
                                f"table {qualified} has no published Iceberg snapshot",
                                hint=f"ducklake-iceberg sync --table {qualified}",
                            )
                        published_snapshot_id = resolve_published_snapshot(
                            iceberg_table
                        )
                        current = current_snapshot(con)
                        current_identity = resolve_table(
                            con, parse_table_ref(qualified), current
                        )
                        validation_report: ValidationReport | None = None
                        if only in {"all", "data"}:
                            try:
                                try:
                                    published_snapshot = snapshot_at(
                                        con, published_snapshot_id
                                    )
                                except IdentityError as exc:
                                    # an expired published snapshot is a divergence.
                                    raise DivergenceError(
                                        f"snapshot {published_snapshot_id} no longer "
                                        "exists in DuckLake",
                                        hint=(
                                            "run 'ducklake-iceberg sync' and "
                                            "review the retention "
                                            "(ducklake-iceberg validate "
                                            "--only retention)"
                                        ),
                                    ) from exc
                                identity = resolve_table(
                                    con, parse_table_ref(qualified), published_snapshot
                                )
                                with timing.phase("validate"):
                                    validation_report = validate_table(
                                        con,
                                        iceberg_table,
                                        lake_alias=con.alias,
                                        meta_catalog=con.meta,
                                        schema_name=schema_name,
                                        table_name=table_name,
                                        table_id=identity.table_id,
                                        iceberg_identifier=target.identifier,
                                        options=options,
                                    )
                                    add_work(
                                        "validate",
                                        items=len(validation_report.checks),
                                    )
                            except Exception as exc:
                                if only == "data":
                                    raise
                                LOG.debug(
                                    "data reconciliation of %s failed",
                                    qualified,
                                    exc_info=exc,
                                )
                                envelope.problems.append(
                                    _problem_from_error(exc, table=qualified)
                                )
                                codes.append(_exit_of(exc))
                        retention_result = None
                        if only in {"all", "retention"}:
                            from ducklake_iceberg.retention import validate_retention

                            try:
                                with timing.phase("retention"):
                                    retention_report = validate_retention(
                                        con,
                                        iceberg_table,
                                        table_id=current_identity.table_id,
                                        snapshot_id=published_snapshot_id,
                                        table_name=qualified,
                                        configured=settings.retention,
                                        strict=strict,
                                        check_s3=check_s3,
                                        s3_sample=s3_sample,
                                        max_io_workers=max_io_workers,
                                        region=settings.aws_region,
                                        profile=settings.aws_profile,
                                        layers_snapshot_id=current.snapshot_id,
                                        iceberg_retention_hours=(
                                            settings.snapshot_retention_hours
                                        ),
                                    )
                                    retention_result = retention_report.as_dict(
                                        strict=strict
                                    )
                                    # referenced files checked
                                    add_work(
                                        "retention",
                                        items=int(
                                            retention_result.get("referenced_files")
                                            or 0
                                        ),
                                    )
                            except Exception as exc:
                                if only == "retention":
                                    raise
                                LOG.debug(
                                    "retention check of %s failed",
                                    qualified,
                                    exc_info=exc,
                                )
                                envelope.problems.append(
                                    _problem_from_error(exc, table=qualified)
                                )
                                codes.append(_exit_of(exc))
                        if only == "retention":
                            assert retention_result is not None
                            envelope.results.append(retention_result)
                            retention_code = ExitCode(retention_result["exit_code"])
                            codes.append(retention_code)
                            if retention_result["risk"]["severity"] == "critical":
                                log_event(
                                    "retention.risk",
                                    level=logging.ERROR,
                                    table=qualified,
                                    severity=retention_result["risk"]["severity"],
                                    exit_code=int(retention_code),
                                )
                        elif validation_report is not None:
                            result_dict = validation_report.as_dict()
                            if retention_result is not None:
                                result_dict["retention"] = retention_result
                                retention_code = ExitCode(retention_result["exit_code"])
                                if retention_result["risk"]["severity"] == "critical":
                                    log_event(
                                        "retention.risk",
                                        level=logging.ERROR,
                                        table=qualified,
                                        severity=retention_result["risk"]["severity"],
                                        exit_code=int(retention_code),
                                    )
                                codes.append(
                                    aggregate_exit_code(
                                        [
                                            validation_report.exit_code,
                                            retention_code,
                                        ]
                                    )
                                )
                            else:
                                codes.append(validation_report.exit_code)
                            if validation_report.exit_code is ExitCode.DIVERGED:
                                log_event(
                                    "validate.divergence",
                                    level=logging.ERROR,
                                    table=qualified,
                                    exit_code=int(validation_report.exit_code),
                                )
                            envelope.results.append(result_dict)
                        elif retention_result is not None:
                            envelope.results.append(retention_result)
                            codes.append(ExitCode(retention_result["exit_code"]))
                    except Exception as exc:
                        LOG.debug("validate of %s failed", qualified, exc_info=exc)
                        envelope.problems.append(
                            _problem_from_error(exc, table=qualified)
                        )
                        codes.append(_exit_of(exc))
        envelope.set_exit(aggregate_exit_code(codes))
        if not json_out:
            if only == "retention":
                render_retention(envelope.results)
            else:
                from ducklake_iceberg.validate import render_validation

                render_validation(envelope.results)
                render_retention(
                    [
                        result["retention"]
                        for result in envelope.results
                        if "retention" in result
                    ]
                )
    except DuckLakeIcebergError as exc:
        envelope.problems.append(_problem_from_error(exc))
        envelope.set_exit(exc.exit_code)
    except _CLICK_CONTROL:
        raise
    except Exception as exc:
        _internal_error(envelope, exc)
    _finish(
        envelope,
        started,
        json_out=json_out,
        log_format=log_format,
        json_version=json_version,
    )


GraceOpt = Annotated[
    str,
    typer.Option(
        "--grace",
        help="grace window (e.g. 24h, 1d, 2h30m)",
    ),
]
ApplyOpt = Annotated[
    bool,
    typer.Option("--apply", help="actually delete; without it, only list (dry-run)"),
]
MaxDeletesOpt = Annotated[
    int,
    typer.Option("--max-deletes", help="cap on deletions per run"),
]
DuckLakeFilesOpt = Annotated[
    bool,
    typer.Option(
        "--ducklake-files",
        help="clean scheduled DuckLake files (global operation)",
    ),
]


@app.command()
def clean(
    table: TableOpt = None,
    all_tables: AllTablesOpt = False,
    schema: SchemaOpt = None,
    grace: GraceOpt = "24h",
    apply: ApplyOpt = False,
    max_deletes: MaxDeletesOpt = 1000,
    ducklake_files: DuckLakeFilesOpt = False,
    json_out: JsonOpt = False,
    profile: ProfileOpt = None,
    log_level: LogLevelOpt = None,
    log_format: LogFormatOpt = "auto",
    config: ConfigOpt = None,
    metrics_enabled: MetricsOpt = False,
    json_version: JsonVersionOpt = ENVELOPE_VERSION,
) -> None:
    """Remove Iceberg orphans or scheduled DuckLake files."""
    started = time.perf_counter()
    envelope = Envelope(command="clean", dry_run=not apply)
    try:
        if ducklake_files:
            if table is not None or all_tables or schema is not None:
                raise ConfigError(
                    "--ducklake-files is global; do not combine it with --table, "
                    "--schema or --all-tables"
                )
        else:
            _validate_target_flags(table=table, all_tables=all_tables, schema=schema)
        settings = load_settings(
            config_file=config,
            overrides=_overrides(profile=profile, log_level=log_level),
        )
        envelope.config = settings.redacted()
        _configure_logging(settings.log_level, fmt=log_format, command="clean")
        _run_clean(
            settings,
            envelope,
            table=table,
            schema=schema,
            grace_str=grace,
            apply=apply,
            max_deletes=max_deletes,
            ducklake_files=ducklake_files,
            json_out=json_out,
            metrics_enabled=metrics_enabled,
        )
    except DuckLakeIcebergError as exc:
        envelope.problems.append(_problem_from_error(exc))
        envelope.set_exit(exc.exit_code)
    except _CLICK_CONTROL:
        raise
    except Exception as exc:
        _internal_error(envelope, exc)
    _finish(
        envelope,
        started,
        json_out=json_out,
        log_format=log_format,
        json_version=json_version,
    )


def _run_clean(
    settings: DuckLakeIcebergSettings,
    envelope: Envelope,
    *,
    table: str | None,
    schema: str | None,
    grace_str: str,
    apply: bool,
    max_deletes: int,
    ducklake_files: bool,
    json_out: bool,
    metrics_enabled: bool = False,
) -> None:
    """Run the clean command for one or more tables.

    Without ``--apply`` it only lists (and does not take the lock); with
    ``--apply`` each table is cleaned under the same process lock as ``sync``.
    """
    if ducklake_files:
        # global operation: one metrics block for the command
        with _table_metrics(envelope, "clean", None, enabled=metrics_enabled):
            _run_ducklake_clean(
                settings,
                envelope,
                grace_str=grace_str,
                apply=apply,
                max_deletes=max_deletes,
                json_out=json_out,
            )
        return

    from ducklake_iceberg.maintenance.orphans import validate_grace
    from ducklake_iceberg.register import build_file_io
    from ducklake_iceberg.s3 import object_store_for

    grace = _parse_duration(grace_str)
    validate_grace(grace)
    scope = _resolve_scope(settings, table=table, schema=schema)
    catalog = _glue_catalog(settings)
    io = build_file_io(settings)
    store = object_store_for(
        settings.iceberg_metadata_prefix,
        region=settings.aws_region,
        profile=settings.aws_profile,
    )

    codes: list[ExitCode] = []
    with open_catalog(settings, read_only=True) as con, con.read_transaction():
        for schema_name, table_name in scope:
            qualified = f"{schema_name}.{table_name}"
            with _table_metrics(envelope, "clean", qualified, enabled=metrics_enabled):
                try:
                    codes.append(
                        _clean_one(
                            settings,
                            con,
                            catalog,
                            io,
                            store,
                            schema_name,
                            table_name,
                            grace=grace,
                            apply=apply,
                            max_deletes=max_deletes,
                            envelope=envelope,
                        )
                    )
                except DuckLakeIcebergError as exc:
                    LOG.error("%s: %s", qualified, exc.message)
                    envelope.problems.append(_problem_from_error(exc, table=qualified))
                    codes.append(exc.exit_code)

    if not json_out:
        _render_clean(envelope.results)
    envelope.set_exit(aggregate_exit_code(codes))


def _run_ducklake_clean(
    settings: DuckLakeIcebergSettings,
    envelope: Envelope,
    *,
    grace_str: str,
    apply: bool,
    max_deletes: int,
    json_out: bool,
) -> None:
    """Globally clean the DuckLake files that are already eligible."""
    from ducklake_iceberg.glue import normalize_table_name
    from ducklake_iceberg.maintenance.ducklake_cleanup import (
        apply_cleanup_plan,
        plan_scheduled_files,
    )
    from ducklake_iceberg.maintenance.orphans import validate_grace
    from ducklake_iceberg.s3 import object_store_for
    from ducklake_iceberg.sync.lock import _lock_key, table_lock

    grace = _parse_duration(grace_str)
    validate_grace(grace)
    if max_deletes < 1:
        raise ConfigError("--max-deletes must be greater than zero")

    glue = _glue_catalog(settings)
    try:
        identifiers = list(glue.list_tables(settings.glue_database))
    except Exception as exc:
        raise AwsError(f"failed to list Glue tables: {exc}") from exc

    ducklake_scope = _resolve_scope(settings, table=None, schema=None)
    lock_names = {
        normalize_table_name(table_name) for _schema_name, table_name in ducklake_scope
    }
    lock_names.update(
        normalize_table_name(str(identifier[-1])) for identifier in identifiers
    )
    store = object_store_for(
        settings.data_path,
        region=settings.aws_region,
        profile=settings.aws_profile,
    )
    with ExitStack() as locks:
        for table_name in sorted(lock_names):
            locks.enter_context(
                table_lock(
                    _lock_key(
                        profile=settings.aws_profile,
                        glue_database=settings.glue_database,
                        glue_table=table_name,
                    ),
                    metadata={
                        "table": f"{settings.glue_database}.{table_name}",
                        "op": "clean-ducklake",
                    },
                    skip=not apply,
                )
            )
        with open_catalog(settings, read_only=not apply) as con:
            plan = plan_scheduled_files(
                con,
                older_than=datetime.now(UTC) - grace,
                max_deletes=max_deletes,
                data_path=settings.data_path,
            )
            referenced = _iceberg_references(
                glue,
                identifiers,
                set(plan.paths),
            )
            report = apply_cleanup_plan(
                con,
                store,
                plan,
                apply=apply,
                referenced_paths=referenced,
            )

    payload = report.to_json()
    envelope.results.append(payload)
    if report.referenced:
        envelope.problems.append(
            Problem(
                code="cleanup_blocked",
                message=(
                    f"{len(report.referenced)} DuckLake file(s) are still "
                    "referenced by retained Iceberg snapshots"
                ),
                hint="increase the DuckLake retention or expire Iceberg snapshots",
            )
        )
    elif report.failed:
        envelope.problems.append(
            Problem(
                code="aws_error",
                message=f"{len(report.failed)} S3 deletion(s) failed",
                hint="check s3:DeleteObject on the maintenance role and rerun",
            )
        )
    if report.truncated:
        LOG.warning("DuckLake cleanup limited by --max-deletes; rerun to continue")
    if not json_out:
        _render_ducklake_clean(report.to_json())
    envelope.set_exit(report.exit_code)


def _iceberg_references(
    glue: GlueCatalog,
    identifiers: list[tuple[str, ...]],
    candidates: set[str],
) -> set[str]:
    """Cross-check candidates against all retained snapshots of the Glue tables."""
    from ducklake_iceberg.s3 import canonical_uri

    referenced: set[str] = set()
    if not candidates:
        return referenced
    for identifier in identifiers:
        try:
            table = glue.load_table(identifier)
            for snapshot in table.metadata.snapshots:
                for task in table.scan(snapshot_id=snapshot.snapshot_id).plan_files():
                    paths = [task.file.file_path]
                    paths.extend(delete.file_path for delete in task.delete_files)
                    for path in paths:
                        canonical = canonical_uri(str(path))
                        if canonical in candidates:
                            referenced.add(canonical)
                    if referenced == candidates:
                        return referenced
        except Exception as exc:
            raise AwsError(
                f"failed to check Iceberg snapshots of {identifier}: {exc}",
                hint="no file was removed; fix the access and rerun",
            ) from exc
    return referenced


def _render_ducklake_clean(report: dict[str, Any]) -> None:
    if report["referenced"]:
        print(
            f"cleanup blocked: {len(report['referenced'])} file(s) "
            "still referenced by Iceberg"
        )
    elif report["dry_run"] and report["candidates"]:
        print(
            f"{len(report['candidates'])} eligible DuckLake file(s); "
            "use --apply to remove"
        )
    elif report["deleted"]:
        suffix = " (truncated: rerun)" if report["truncated"] else ""
        print(f"removed {len(report['deleted'])} DuckLake file(s){suffix}")
    else:
        print("no eligible DuckLake files")


def _clean_one(
    settings: DuckLakeIcebergSettings,
    con: DuckLakeConnection,
    catalog: GlueCatalog,
    io: FileIO,
    store: ObjectStore,
    schema_name: str,
    table_name: str,
    *,
    grace: timedelta,
    apply: bool,
    max_deletes: int,
    envelope: Envelope,
) -> ExitCode:
    """One table: lock, divergence guard, cleanup and result."""
    from ducklake_iceberg.glue import normalize_table_name
    from ducklake_iceberg.maintenance.orphans import CleanError, clean_table
    from ducklake_iceberg.register import PointerError, read_pointer
    from ducklake_iceberg.status import DIVERGENT_STATUSES, StatusCode, table_status
    from ducklake_iceberg.sync.lock import _lock_key, table_lock

    qualified = f"{schema_name}.{table_name}"
    glue_table = normalize_table_name(table_name)
    # same location as the export (_table_location): {prefix}/{db}/{table}
    location = (
        f"{settings.iceberg_metadata_prefix.rstrip('/')}/"
        f"{settings.glue_database}/{glue_table}"
    )
    key = _lock_key(
        profile=settings.aws_profile,
        glue_database=settings.glue_database,
        glue_table=glue_table,
    )
    with table_lock(key, metadata={"table": qualified, "op": "clean"}, skip=not apply):
        with timing.phase("resolve"):
            status = table_status(
                con,
                catalog,
                ducklake_schema=schema_name,
                ducklake_table=table_name,
                glue_identifier=(settings.glue_database, glue_table),
                with_delta=False,
            )
            add_work("resolve", items=1)
        if status.status in DIVERGENT_STATUSES:
            envelope.problems.append(
                Problem(
                    code="catalog_diverged",
                    message=f"table is {status.status}; cleanup suspended",
                    hint="resolve with 'ducklake-iceberg status' before cleaning",
                    table=qualified,
                )
            )
            return ExitCode.DIVERGED
        if status.status is StatusCode.UNREADABLE:
            raise CleanError(
                f"table {settings.glue_database}.{glue_table} is unreadable: "
                f"{status.findings[0].message}"
            )

        # the handoff pointer target is the base of the next export: reachable
        extra_roots: list[str] = []
        try:
            extra_roots.append(read_pointer(io, location).metadata_location)
        except PointerError as exc:
            if "no export found" not in exc.message:
                raise CleanError(
                    f"unreadable handoff pointer at {location}: {exc.message}"
                ) from exc

        report = clean_table(
            catalog,
            io,
            store,
            glue_identifier=(settings.glue_database, glue_table),
            prefix=location,
            data_path=settings.data_path,
            grace=grace,
            apply=apply,
            max_deletes=max_deletes,
            extra_roots=extra_roots,
        )

    envelope.results.append({"table": qualified, **report.to_json()})
    if report.failed:
        envelope.problems.append(
            Problem(
                code="aws_error",
                message=(
                    f"{len(report.failed)} deletion(s) failed: "
                    f"{[k for k, _ in report.failed[:5]]}"
                ),
                hint="check s3:DeleteObject and rerun",
                table=qualified,
            )
        )
        return ExitCode.ERROR
    if report.truncated:
        LOG.warning("%s: hit --max-deletes; rerun to continue", qualified)
    if report.deleted or (report.dry_run and report.deletable):
        return ExitCode.SUCCESS
    return ExitCode.NOTHING_TO_DO


def _render_clean(results: list[dict[str, Any]]) -> None:
    """Human-readable clean summary (one line per table)."""
    from rich.console import Console
    from rich.markup import escape

    console = Console()
    for r in results:
        table = escape(str(r["table"]))
        if not r["deletable"]:
            console.print(
                f"{table}: no eligible orphans ({r['scanned']} object(s), "
                f"{r['reachable']} reachable)"
            )
        elif r["dry_run"]:
            console.print(
                f"{table}: {r['deletable']} eligible orphan(s) "
                f"({r['deletable_bytes']} bytes) of {r['scanned']} object(s); "
                f"use --apply"
            )
        else:
            more = " (truncated: rerun)" if r["truncated"] else ""
            console.print(
                f"{table}: removed {len(r['deleted'])} file(s) "
                f"({r['bytes_reclaimed']} bytes){more}"
            )


_DURATION_RE = re.compile(r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?")


def _parse_duration(duration_str: str) -> timedelta:
    """Parse a duration (e.g. ``24h``, ``1d``, ``2h30m``, ``90m``).

    Raises:
        ConfigError: Invalid format (exit 1, never a traceback).
    """
    text = duration_str.strip().lower()
    match = _DURATION_RE.fullmatch(text)
    if not text or match is None or not any(match.groups()):
        raise ConfigError(
            f"invalid duration format: {duration_str!r} (expected e.g. 24h, 1d, 2h30m)"
        )
    days, hours, minutes, seconds = (int(g) if g else 0 for g in match.groups())
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


def main() -> None:
    """Real entry point; `app` is the console script callable."""
    app()
