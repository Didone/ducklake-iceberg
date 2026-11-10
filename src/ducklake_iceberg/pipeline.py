"""The ``sync`` pipeline for one table: doctor, flush, export and register.

Each stage has a stop point: a compatibility blocker (exit 2), a divergence
(exit 3) or an error (exit 1) stops the pipeline; "nothing to do" (exit 4)
never stops it, because a previous run may have written the ``metadata.json``
and failed before reaching Glue.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ducklake_iceberg import timing
from ducklake_iceberg.catalog.connection import metadata_catalog, open_catalog
from ducklake_iceberg.catalog.reader import (
    current_snapshot,
    parse_table_ref,
    resolve_table,
    snapshot_at,
)
from ducklake_iceberg.checkpoint.detect import clean_snapshot, detect_inlined
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
from ducklake_iceberg.export import ExportResult, run_export
from ducklake_iceberg.handoff import (
    ExportPointer,
    normalize_table_name,
    read_current_pointer,
)
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
) -> tuple[ExitCode, DoctorReport]:
    """Run preflight rules and return the exit code and complete report.

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
        return (ExitCode.BLOCKED if blocking else ExitCode.SUCCESS), report
    return _exit_code_from_report(report), report


def _blocked_problem(qualified: str, findings: list[Finding]) -> Problem:
    detail = "; ".join(f"{f.rule_id}: {f.subject}: {f.detail}" for f in findings)
    return Problem(
        code="blocked",
        message=f"Iceberg v2 compatibility blocker: {detail}",
        hint=findings[0].remediation if findings else None,
        table=qualified,
    )


def _doctor_problem(qualified: str, code: ExitCode, report: DoctorReport) -> Problem:
    if code is ExitCode.BLOCKED:
        return _blocked_problem(qualified, _blockers(report))
    errors = "; ".join(
        f"{r.rule_id}: {r.error}"
        for r in report.results
        if r.status is RuleStatus.ERROR
    )
    return Problem(
        code="doctor_failed",
        message=f"doctor failed on {qualified}: {errors}",
        hint="resolve the reported rule failure before publishing",
        table=qualified,
    )


# -- pipeline -----------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class _SyncResult:
    exit_code: ExitCode
    export: ExportResult | None = None


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
) -> _SyncResult:
    qualified = f"{schema_name}.{table_name}"
    steps: list[dict[str, Any]] = []
    envelope.results.append({"table": qualified, "steps": steps})

    # doctor stage: a compatibility blocker (exit 2) stops the sync.
    with timing.phase("doctor"):
        doctor_exit, report = _doctor_gate(
            settings, qualified, before_flush=True, con=catalog.get()
        )
    steps.append(
        {"step": "doctor", "exit_code": int(doctor_exit), "report": report.to_json()}
    )
    if doctor_exit is ExitCode.BLOCKED:
        envelope.problems.append(
            _blocked_problem(qualified, _blockers(report, skip=AUTO_REMEDIABLE_GATES))
        )
        log_event(
            "compat.blocked",
            level=logging.ERROR,
            table=qualified,
            exit_code=int(doctor_exit),
        )
        return _SyncResult(doctor_exit)
    if doctor_exit is ExitCode.ERROR:
        envelope.problems.append(_doctor_problem(qualified, doctor_exit, report))
        return _SyncResult(doctor_exit)

    # flush stage. Writes to DuckLake, so a dry run only reports what it would
    # flush; the export cannot be planned before the rows are in Parquet.
    if dry_run:
        pending = _pending_inlined(qualified, catalog.get())
        if pending is not None:
            steps.append(
                {"step": "checkpoint", "exit_code": 0, "dry_run": True, **pending}
            )
            # still tell a monitor whether the published mirror diverged
            check = _dry_run_glue_check(
                settings, schema_name, table_name, None, steps, envelope, qualified
            )
            return _SyncResult(ExitCode.SUCCESS if check is None else check)
    else:
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
            return _SyncResult(exc.exit_code)

        # post-flush revalidation: the flush may expose a blocked type.
        if flush.wrote:
            with timing.phase("doctor"):
                revalidate_exit, report = _doctor_gate(
                    settings, qualified, at_export_target=True, con=catalog.get()
                )
            steps.append(
                {
                    "step": "doctor",
                    "exit_code": int(revalidate_exit),
                    "report": report.to_json(),
                }
            )
            if revalidate_exit is not ExitCode.SUCCESS:
                envelope.problems.append(
                    _doctor_problem(qualified, revalidate_exit, report)
                )
                return _SyncResult(revalidate_exit)

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
        return _SyncResult(exit_of(exc))
    catalog.invalidate()

    if dry_run:
        diverged = _dry_run_glue_check(
            settings,
            schema_name,
            table_name,
            result.handoff,
            steps,
            envelope,
            qualified,
        )
        return _SyncResult(result.exit_code if diverged is None else diverged, result)

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
        return _SyncResult(ExitCode.SUCCESS, result)
    except NothingToDo as exc:
        steps.append({"step": "register", "exit_code": int(exc.exit_code)})
        # already published: sync only "did something" if the export wrote
        return _SyncResult(
            result.exit_code if result.exit_code is ExitCode.SUCCESS else exc.exit_code,
            result,
        )
    except Exception as exc:
        LOG.debug("register of %s failed", qualified, exc_info=exc)
        steps.append({"step": "register", "exit_code": int(exit_of(exc))})
        envelope.problems.append(problem_from_error(exc, table=qualified))
        return _SyncResult(exit_of(exc), result)


def _pending_inlined(qualified: str, con: DuckLakeConnection) -> dict[str, int] | None:
    """Pending inlined rows and deletions of the table, or ``None``."""
    ref = parse_table_ref(qualified)
    with con.read_transaction():
        snapshot = current_snapshot(con)
        identity = resolve_table(con, ref, snapshot)
        status = detect_inlined(
            con.raw,
            con.meta,
            CheckpointTableRef(
                identity.ref.schema_name, identity.ref.table_name, identity.table_id
            ),
            snapshot.snapshot_id,
        )
    if not status.has_pending:
        return None
    return {
        "pending_inlined_rows": status.pending_data_rows,
        "pending_inlined_deletions": status.pending_delete_rows,
    }


def _dry_run_glue_check(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    handoff: ExportPointer | None,
    steps: list[dict[str, Any]],
    envelope: Envelope,
    qualified: str,
) -> ExitCode | None:
    """Read-only Glue check of a dry run: ``DIVERGED``/``ERROR`` or ``None``.

    Compares what Glue points at with the last export (the pointer) and
    records it as the ``glue`` value of the ``register`` step:

    * ``unpublished``: no export yet, the next sync registers the table;
    * ``in_sync``: Glue points at the last export;
    * ``behind``: Glue points at an ancestor of it (a previous sync failed
      before registering); the next sync advances it;
    * anything else is an external write (exit 3), the same verdict a real
      sync would reach.
    """
    if handoff is None:
        handoff = read_current_pointer(settings, table_name)
    glue: str
    if handoff is None:
        glue = "unpublished"
    else:
        try:
            with timing.phase("register"):
                run_register(
                    settings, schema_name, table_name, handoff=handoff, dry_run=True
                )
            glue = "behind"
        except NothingToDo:
            glue = "in_sync"
        except Exception as exc:
            LOG.debug("dry-run Glue check of %s failed", qualified, exc_info=exc)
            steps.append({"step": "register", "exit_code": int(exit_of(exc))})
            envelope.problems.append(problem_from_error(exc, table=qualified))
            return exit_of(exc)
    steps.append({"step": "register", "exit_code": 0, "dry_run": True, "glue": glue})
    return None


def _sync_locked(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    *,
    dry_run: bool,
    envelope: Envelope,
) -> _SyncResult:
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
        return _SyncResult(exc.exit_code)
    finally:
        catalog.invalidate()


def _publication_totals(
    export: ExportResult | None, metrics: RunMetrics
) -> dict[str, int]:
    """Publication totals from the typed export result, before presentation.

    ``files_*`` count data files; delete files go in ``delete_files_added``.
    ``bytes_referenced`` is the sum of ``data_file_size_bytes`` of the published
    snapshot; ``bytes_written`` is the metadata and delete files written.
    """
    return {
        "files_added": export.plan.added_data_files if export is not None else 0,
        "files_removed": export.plan.removed_data_files if export is not None else 0,
        "delete_files_added": export.plan.added_delete_files
        if export is not None
        else 0,
        "bytes_referenced": export.plan.total_files_size if export is not None else 0,
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
        outcome = _sync_locked(
            settings, schema_name, table_name, dry_run=dry_run, envelope=envelope
        )
    result = next(
        (item for item in reversed(envelope.results) if item.get("table") == qualified),
        None,
    )
    code = outcome.exit_code
    export = outcome.export
    source_snapshot = export.plan.ducklake_snapshot_id if export is not None else None
    iceberg_snapshot = export.snapshot_id if export is not None else None
    totals = _publication_totals(export, metrics)
    if result is not None:
        result.update(
            {
                "exit_code": int(code),
                "duration_ms": metrics.duration_ms,
                "ducklake_snapshot_id": source_snapshot,
                "iceberg_snapshot_id": iceberg_snapshot,
                "metadata_location": export.metadata_location
                if export is not None
                else None,
                **totals,
            }
        )
        metrics.summary = dict(totals)
        result["metrics"] = metrics.to_json()
    log_event(
        "sync.end",
        table=qualified,
        snapshot_id=source_snapshot,
        iceberg_snapshot_id=iceberg_snapshot,
        duration_ms=metrics.duration_ms,
        dry_run=dry_run,
        exit_code=int(code),
        **totals,
    )
    log_event("table.end", table=qualified, exit_code=int(code))
    return code
