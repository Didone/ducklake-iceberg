"""The ``sync`` pipeline for one table: doctor, flush, export and register.

Each stage has a stop point: a compatibility blocker (exit 2), a divergence
(exit 3) or an error (exit 1) stops the pipeline; "nothing to do" (exit 4)
never stops it, because a previous run may have written the ``metadata.json``
and failed before reaching Glue.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from ducklake_iceberg import timing
from ducklake_iceberg.catalog.connection import metadata_catalog, open_catalog
from ducklake_iceberg.catalog.reader import (
    current_snapshot,
    parse_table_ref,
    resolve_table,
    snapshot_at,
)
from ducklake_iceberg.checkpoint.detect import clean_snapshot
from ducklake_iceberg.checkpoint.flush import flush_inlined
from ducklake_iceberg.checkpoint.model import TableRef as CheckpointTableRef
from ducklake_iceberg.concurrency import Limits, apply_limits
from ducklake_iceberg.doctor import rules as doctor_rules
from ducklake_iceberg.doctor.model import (
    DoctorContext,
    DoctorTarget,
    Finding,
    RuleStatus,
    Severity,
)
from ducklake_iceberg.doctor.report import AUTO_REMEDIABLE_GATES, DoctorReport
from ducklake_iceberg.doctor.runner import run as run_rules
from ducklake_iceberg.errors import (
    ConfigError,
    DuckLakeIcebergError,
    ExitCode,
    NothingToDo,
)
from ducklake_iceberg.export import run_export
from ducklake_iceberg.glue import normalize_table_name
from ducklake_iceberg.obs.logging import bind, log_event
from ducklake_iceberg.obs.metrics import RunMetrics, add_work, collect
from ducklake_iceberg.obs.redaction import scrub
from ducklake_iceberg.output import Envelope, Problem
from ducklake_iceberg.register import run_register
from ducklake_iceberg.sync.lock import lock_key, table_lock

if TYPE_CHECKING:
    from ducklake_iceberg.catalog.connection import DuckLakeConnection
    from ducklake_iceberg.catalog.reader import TableIdentity
    from ducklake_iceberg.config import DuckLakeIcebergSettings

LOG = logging.getLogger("ducklake_iceberg")


# -- errors -------------------------------------------------------------------
def exit_of(exc: BaseException) -> ExitCode:
    """``exit_code`` declared by the error; ``ERROR`` when missing or invalid."""
    try:
        return ExitCode(int(getattr(exc, "exit_code", ExitCode.ERROR)))
    except (TypeError, ValueError):
        return ExitCode.ERROR


def _code_for(exc: BaseException) -> str:
    """Stable ``problems[].code`` declared by the error class.

    Errors outside the tool's hierarchy (some export errors only carry an
    ``exit_code``) map by that exit code.
    """
    if isinstance(exc, DuckLakeIcebergError):
        return exc.problem_code
    code = exit_of(exc)
    if code is ExitCode.BLOCKED:
        return "blocked"
    if code is ExitCode.DIVERGED:
        return "catalog_diverged"
    return "error"


def problem_from_error(exc: BaseException, *, table: str | None = None) -> Problem:
    """``Problem`` for any pipeline error.

    Several export errors (enumeration, manifest, metadata.json) do not inherit
    from ``DuckLakeIcebergError`` but carry ``exit_code``; handling them here
    keeps them from escaping as a traceback and preserves the exit code.
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


# -- limits -------------------------------------------------------------------
def apply_concurrency_limits(
    settings: DuckLakeIcebergSettings, envelope: Envelope
) -> DuckLakeIcebergSettings:
    """Reconcile the pool limits against the global cap and install them.

    Each reduced limit becomes a ``warning`` problem (``workers_capped``).
    """
    requested = Limits(
        max_footer_workers=settings.max_footer_workers,
        max_io_workers=settings.max_io_workers,
        max_total_workers=settings.max_total_workers,
        duckdb_threads=settings.duckdb_threads,
        duckdb_memory_limit=settings.duckdb_memory_limit,
        pyiceberg_max_workers=settings.pyiceberg_max_workers,
    )
    limits = requested.capped()
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
    return settings.model_copy(update=updates) if updates else settings


# -- doctor -------------------------------------------------------------------
def _run_rules(con: DuckLakeConnection, identity: TableIdentity) -> DoctorReport:
    """Run the preflight rules in the transaction already opened by the caller."""
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
    add_work(
        "doctor",
        items=sum(1 for r in report.results if r.status is not RuleStatus.SKIPPED),
    )
    return report


def _exit_code_from_report(report: DoctorReport) -> ExitCode:
    """Map the rules report exit code (0/1/2) to ``ExitCode``."""
    code = report.exit_code()
    if code == 1:
        return ExitCode.ERROR
    if code == 2:
        return ExitCode.BLOCKED
    return ExitCode.SUCCESS


def _blockers(
    report: DoctorReport, *, skip: frozenset[str] = frozenset()
) -> list[Finding]:
    return [
        f
        for r in report.results
        if r.rule_id not in skip
        for f in r.findings
        if f.severity is Severity.BLOCKER
    ]


def _doctor_gate(
    settings: DuckLakeIcebergSettings,
    qualified: str,
    *,
    before_flush: bool = False,
    at_export_target: bool = False,
    con: DuckLakeConnection | None = None,
) -> tuple[ExitCode, list[Finding]]:
    """Run the preflight rules for a table and return the ``ExitCode``.

    Opens the catalog READ_ONLY and runs the rules in the read transaction.
    Before the flush, auto-remediable gates (``inlined-data``) do not block:
    the flush itself resolves them, and the post-flush revalidation runs all
    the rules again on the snapshot that will be exported. With
    ``at_export_target``, the rules run on that snapshot (the most recent one
    with no pending inlined data), not the current one: it is what the export
    will publish.
    """
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
    if before_flush and not report.has_error:
        blocking = _blockers(report, skip=frozenset(AUTO_REMEDIABLE_GATES))
        return (ExitCode.BLOCKED if blocking else ExitCode.SUCCESS), blocking
    return _exit_code_from_report(report), _blockers(report)


def _blocked_problem(qualified: str, findings: list[Finding]) -> Problem:
    detail = "; ".join(f"{f.rule_id}: {f.subject}: {f.detail}" for f in findings)
    return Problem(
        code="blocked",
        message=f"Iceberg v2 compatibility blocker: {detail}",
        hint=findings[0].remediation if findings else None,
        table=qualified,
    )


# -- pipeline -----------------------------------------------------------------
class _SharedCatalog:
    """READ_ONLY metadata connection of a sync, lent to the stages.

    Opened on demand and without S3 (doctor, detection and enumeration only
    read the catalog); reopened after any write to DuckLake (the flush).
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


def _sync_steps(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    catalog: _SharedCatalog,
    *,
    dry_run: bool,
    envelope: Envelope,
) -> ExitCode:
    qualified = f"{schema_name}.{table_name}"
    steps: list[dict[str, Any]] = []
    envelope.results.append({"table": qualified, "steps": steps})

    # doctor stage: a compatibility blocker (exit 2) stops the sync.
    with timing.phase("doctor"):
        doctor_exit, blockers = _doctor_gate(
            settings, qualified, before_flush=True, con=catalog.get()
        )
    steps.append({"step": "doctor", "exit_code": int(doctor_exit)})
    if doctor_exit is ExitCode.BLOCKED:
        envelope.problems.append(_blocked_problem(qualified, blockers))
        log_event(
            "compat.blocked",
            level=logging.ERROR,
            table=qualified,
            exit_code=int(doctor_exit),
        )
        return doctor_exit
    if doctor_exit is ExitCode.ERROR:
        envelope.problems.append(
            problem_from_error(
                ConfigError(f"doctor failed on {qualified}"), table=qualified
            )
        )
        return doctor_exit

    # flush stage. Writes to DuckLake, so a dry run skips it: the export then
    # plans against the latest snapshot without pending inlined data.
    if not dry_run:
        try:
            with timing.phase("checkpoint"):
                flush = flush_inlined(settings, qualified, con=catalog.get())
                if flush.wrote:
                    catalog.invalidate()
                add_work("checkpoint", items=1 if flush.wrote else 0)
            steps.append({"step": "checkpoint", "exit_code": int(flush.exit_code)})
        except DuckLakeIcebergError as exc:
            steps.append({"step": "checkpoint", "exit_code": int(exc.exit_code)})
            envelope.problems.append(problem_from_error(exc, table=qualified))
            return exc.exit_code

        # post-flush revalidation: the flush may expose a blocked type.
        if flush.wrote:
            with timing.phase("doctor"):
                revalidate_exit, blockers = _doctor_gate(
                    settings, qualified, at_export_target=True, con=catalog.get()
                )
            steps.append({"step": "doctor", "exit_code": int(revalidate_exit)})
            if revalidate_exit in (ExitCode.BLOCKED, ExitCode.DIVERGED):
                if blockers:
                    envelope.problems.append(_blocked_problem(qualified, blockers))
                return revalidate_exit

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
        steps.append({"step": "export", "exit_code": int(exit_of(exc))})
        envelope.problems.append(problem_from_error(exc, table=qualified))
        return exit_of(exc)
    catalog.invalidate()

    if dry_run:
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
        return ExitCode.SUCCESS
    except NothingToDo as exc:
        steps.append({"step": "register", "exit_code": int(exc.exit_code)})
        # already published: sync only "did something" if the export wrote
        return (
            result.exit_code if result.exit_code is ExitCode.SUCCESS else exc.exit_code
        )
    except Exception as exc:
        LOG.debug("register of %s failed", qualified, exc_info=exc)
        steps.append({"step": "register", "exit_code": int(exit_of(exc))})
        envelope.problems.append(problem_from_error(exc, table=qualified))
        return exit_of(exc)


def _sync_locked(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    *,
    dry_run: bool,
    envelope: Envelope,
) -> ExitCode:
    """The stages under the table's process lock (skipped in a dry run)."""
    key = lock_key(
        profile=settings.aws_profile,
        glue_database=settings.glue_database,
        glue_table=normalize_table_name(table_name),
    )
    qualified = f"{schema_name}.{table_name}"
    catalog = _SharedCatalog(settings)
    try:
        with table_lock(key, metadata={"table": qualified}, skip=dry_run):
            return _sync_steps(
                settings,
                schema_name,
                table_name,
                catalog,
                dry_run=dry_run,
                envelope=envelope,
            )
    except DuckLakeIcebergError as exc:
        envelope.problems.append(problem_from_error(exc, table=qualified))
        return exc.exit_code
    finally:
        catalog.invalidate()


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


def sync_table(
    settings: DuckLakeIcebergSettings,
    qualified: str,
    *,
    run_id: str,
    dry_run: bool,
    envelope: Envelope,
) -> ExitCode:
    """Sync one ``schema.table``; results and problems go into ``envelope``."""
    ref = parse_table_ref(qualified)
    if "." not in qualified:
        raise ConfigError(
            f"--table requires a qualified name: <schema>.<table> (got {qualified!r})"
        )
    schema_name, table_name = ref.schema_name, ref.table_name
    qualified = f"{schema_name}.{table_name}"
    log_event("table.start", table=qualified)
    metrics = RunMetrics(run_id, "sync", table=qualified)
    with bind(table=qualified), collect(metrics):
        code = _sync_locked(
            settings, schema_name, table_name, dry_run=dry_run, envelope=envelope
        )
    result = next(
        (item for item in reversed(envelope.results) if item.get("table") == qualified),
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
    return code
