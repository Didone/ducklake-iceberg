"""Orchestration of the `ducklake-iceberg checkpoint` command.

Runs the table checkpoint in this order: `ducklake_flush_inlined_data`
(table) in one transaction; `ducklake_expire_snapshots` (catalog),
`ducklake_merge_adjacent_files` and `ducklake_rewrite_data_files` (table) in a
second transaction -- DuckLake refuses to commit writes and maintenance
together. It never runs the global `CHECKPOINT;` nor `cleanup_old_files` or
`delete_orphaned_files`: replaced or expired files stay in
`ducklake_files_scheduled_for_deletion` and remain readable by Iceberg.

Sequence: READ_ONLY phase (scope, fingerprint, detection); write phase on a
new connection without `READ_ONLY`, rechecking the fingerprint; snapshot
re-read on another new connection.
"""

from __future__ import annotations

import dataclasses
import logging
import random
import time
from collections.abc import Callable
from typing import Any, Literal, TypeVar, cast, overload

import duckdb

from ducklake_iceberg.catalog.connection import (
    DuckLakeConnection,
    metadata_catalog,
    open_catalog,
)
from ducklake_iceberg.catalog.reader import current_snapshot, parse_table_ref
from ducklake_iceberg.checkpoint.detect import detect_inlined
from ducklake_iceberg.checkpoint.dry_run import (
    DryRunReport,
    dry_run_checkpoint,
    effective_target_file_size,
)
from ducklake_iceberg.checkpoint.model import (
    CatalogFingerprint,
    CheckpointError,
    CheckpointRunReport,
    CompactionStep,
    Phase,
    ScopeOrigin,
    TableCheckpointResult,
    TableRef,
    TableScope,
)
from ducklake_iceberg.checkpoint.partitions import (
    DEFAULT_CLOSED_AFTER_SECONDS,
    file_size_distribution,
    partition_size_report,
)
from ducklake_iceberg.checkpoint.report import build_snapshot_report
from ducklake_iceberg.checkpoint.revalidate import RevalidationRequest, revalidate_many
from ducklake_iceberg.checkpoint.variant import (
    VariantColumn,
    find_nested_variants,
    find_variant_columns,
    nested_variant_error,
    translate_checkpoint_error,
)
from ducklake_iceberg.config import DuckLakeIcebergSettings
from ducklake_iceberg.errors import (
    CatalogError,
    ConfigError,
    DivergenceError,
    DuckLakeIcebergError,
    ExitCode,
    OperationAborted,
)
from ducklake_iceberg.output import worse_exit
from ducklake_iceberg.retry import RetryPolicy

LOG = logging.getLogger("ducklake_iceberg")

T = TypeVar("T")

#: DuckLake commit conflict with another writer of the same table (e.g. a
#: consumer that commits every minute): the rolled-back transaction left no
#: effect on the catalog and is redone from scratch, against the new snapshot.
_CONFLICT = "transaction conflict"
CONFLICT_RETRY = RetryPolicy(
    max_attempts=5, base_delay_s=1.0, factor=2.0, max_delay_s=8.0
)


def _retry_on_conflict(fn: Callable[[], T], label: str) -> T:
    policy = CONFLICT_RETRY
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return fn()
        except duckdb.Error as exc:
            if _CONFLICT not in str(exc).lower() or attempt == policy.max_attempts:
                raise
            nominal = min(
                policy.max_delay_s, policy.base_delay_s * policy.factor ** (attempt - 1)
            )
            delay = random.uniform(0, nominal)  # noqa: S311 - jitter, not crypto
            LOG.info(
                "%s: conflict with another writer (attempt %d of %d); "
                "retrying in %.1f s",
                label,
                attempt,
                policy.max_attempts,
                delay,
            )
            time.sleep(delay)
    raise AssertionError("unreachable")


def _lit(value: str) -> str:
    """SQL literal in single quotes, escaped by doubling.

    The names come from `ducklake_schema`/`ducklake_table`, never from the
    command line; the escaping is defense in depth.
    """
    return "'" + value.replace("'", "''") + "'"


def _rows(
    con: DuckLakeConnection, sql: str, params: Any = None
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = con.query_arrow(sql, params).to_pylist()
    return rows


# -- scope -----------------------------------------------------------------


def _scope_sql(meta: str) -> str:
    return (
        "SELECT t.table_id, s.schema_name, t.table_name "
        f'FROM "{meta}".ducklake_table AS t '
        f'JOIN "{meta}".ducklake_schema AS s '
        "  ON s.schema_id = t.schema_id "
        " AND $snapshot_id >= s.begin_snapshot "
        " AND (s.end_snapshot IS NULL OR $snapshot_id < s.end_snapshot) "
        "WHERE $snapshot_id >= t.begin_snapshot "
        "  AND (t.end_snapshot IS NULL OR $snapshot_id < t.end_snapshot) "
        "  AND ($schema_name IS NULL OR s.schema_name = $schema_name) "
        "  AND ($table_name  IS NULL OR t.table_name  = $table_name) "
        "ORDER BY s.schema_name, t.table_name"
    )


def _to_refs(rows: list[dict[str, Any]]) -> tuple[TableRef, ...]:
    return tuple(
        TableRef(
            schema_name=str(r["schema_name"]),
            table_name=str(r["table_name"]),
            table_id=int(r["table_id"]),
        )
        for r in rows
    )


def resolve_scope(
    con: DuckLakeConnection,
    *,
    table: str | None,
    schema: str | None,
    all_tables: bool,
) -> TableScope:
    """Resolve the scope against the current snapshot."""
    if table and all_tables:
        raise ConfigError("--table and --all-tables are mutually exclusive")
    if not table and not all_tables:
        raise ConfigError("provide --table or --all-tables")
    if schema and not all_tables:
        raise ConfigError("--schema only applies to --all-tables")

    snapshot_id = current_snapshot(con).snapshot_id

    if table:
        if "." not in table:
            raise ConfigError(
                f"--table requires a qualified name: <schema>.<table> (got {table!r})"
            )
        ref = parse_table_ref(table)
        found = _to_refs(
            _rows(
                con,
                _scope_sql(con.meta),
                {
                    "snapshot_id": snapshot_id,
                    "schema_name": ref.schema_name,
                    "table_name": ref.table_name,
                },
            )
        )
        if not found:
            raise ConfigError(
                f"table {ref.schema_name}.{ref.table_name} does not exist in "
                f"snapshot {snapshot_id}",
                hint="check the name with 'ducklake-iceberg doctor'",
            )
        return TableScope(tables=found[:1], origin="table")

    refs = _to_refs(
        _rows(
            con,
            _scope_sql(con.meta),
            {"snapshot_id": snapshot_id, "schema_name": schema, "table_name": None},
        )
    )
    origin: ScopeOrigin = "schema" if schema else "all"
    return TableScope(tables=refs, origin=origin, schema_filter=schema)


# -- safeguards -----------------------------------------------------------


def read_fingerprint(con: DuckLakeConnection) -> CatalogFingerprint:
    """Catalog identity fingerprint."""
    meta = con.meta
    meta_rows = _rows(
        con,
        "SELECT "
        "max(CASE WHEN key = 'data_path' AND scope IS NULL THEN value END) "
        "    AS data_path, "
        "max(CASE WHEN key = 'version' AND scope IS NULL THEN value END) "
        "    AS format_version "
        f'FROM "{meta}".ducklake_metadata',
    )
    snap_rows = _rows(
        con, f'SELECT max(snapshot_id) AS s FROM "{meta}".ducklake_snapshot'
    )
    count_rows = _rows(
        con,
        f'SELECT count(*) AS n FROM "{meta}".ducklake_table WHERE end_snapshot IS NULL',
    )
    head = meta_rows[0] if meta_rows else {}
    snap = snap_rows[0]["s"] if snap_rows else None
    return CatalogFingerprint(
        data_path=_opt_str(head.get("data_path")),
        format_version=_opt_str(head.get("format_version")),
        max_snapshot_id=int(snap) if snap is not None else None,
        table_count=int(count_rows[0]["n"]) if count_rows else 0,
    )


def _opt_str(value: object) -> str | None:
    return None if value is None else str(value)


def _auto_compact_disabled(con: DuckLakeConnection, scope: TableScope) -> set[int]:
    """`table_id`s with `auto_compact = false` at table scope.

    Only table scope is evaluated here; global/schema scope disables all
    tables and shows up as "nothing to do" for all of them.
    """
    try:
        rows = _rows(
            con,
            "SELECT scope_id FROM "
            f'"{con.meta}".ducklake_metadata '
            "WHERE key = 'auto_compact' AND lower(value) = 'false' "
            "  AND scope = 'table'",
        )
    except CatalogError:
        return set()
    ids = {int(r["scope_id"]) for r in rows if r["scope_id"] is not None}
    return ids & {t.table_id for t in scope.tables}


# -- execution ---------------------------------------------------------------


def _max_snapshot(con: DuckLakeConnection) -> int:
    rows = _rows(
        con, f'SELECT max(snapshot_id) AS s FROM "{con.meta}".ducklake_snapshot'
    )
    return int(rows[0]["s"])


def _snapshot_head(con: DuckLakeConnection) -> tuple[int, int, Any, int]:
    """(`snapshot_id`, `next_file_id`, `snapshot_time`, `schema_version`) of
    the current snapshot. The time and `schema_version` feed the snapshot
    report (classification of retired files and detection of concurrent DDL)."""
    rows = _rows(
        con,
        "SELECT snapshot_id, next_file_id, "
        "CAST(snapshot_time AS TIMESTAMPTZ) AS snapshot_time, schema_version "
        f'FROM "{con.meta}".ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1',
    )
    head = rows[0]
    return (
        int(head["snapshot_id"]),
        int(head["next_file_id"]),
        head["snapshot_time"],
        int(head["schema_version"]),
    )


def _call(con: DuckLakeConnection, sql: str) -> list[dict[str, Any]]:
    """Run a `CALL` on the raw connection; `duckdb.Error` is not translated."""
    relation = cast("Any", con.raw).sql(sql)
    if relation is None:
        return []
    rows: list[dict[str, Any]] = relation.to_arrow_table().to_pylist()
    return rows


def _step(rows: list[dict[str, Any]]) -> CompactionStep:
    return CompactionStep(
        files_processed=sum(int(r["files_processed"]) for r in rows),
        files_created=sum(int(r["files_created"]) for r in rows),
    )


def _translate(
    table: TableRef,
    phase: Phase,
    exc: duckdb.Error,
    variants: tuple[VariantColumn, ...],
    *,
    snapshot_before: int,
    con: DuckLakeConnection,
) -> CheckpointError:
    """Translate the `CALL` failure, filling `snapshot_created` when the write
    left a new snapshot behind."""
    LOG.debug(
        "checkpoint of %s failed in phase %s", table.qualified, phase, exc_info=exc
    )
    after = _max_snapshot(con)
    created = after if after > snapshot_before else None
    return translate_checkpoint_error(
        exc, table=table, phase=phase, variants=variants, snapshot_created=created
    )


def checkpoint_table(
    con: DuckLakeConnection,
    alias: str,
    table: TableRef,
    variants: tuple[VariantColumn, ...] = (),
    *,
    compact: bool = True,
    target_file_size_bytes: int | None = None,
    rewrite_delete_threshold: float | None = None,
    closed_after_seconds: int = DEFAULT_CLOSED_AFTER_SECONDS,
) -> TableCheckpointResult:
    """Checkpoint of one table, in a fixed order:

    1. `ducklake_flush_inlined_data` (table scope) -- transaction 1;
    2. `ducklake_expire_snapshots` (**catalog** scope: the function does not
       accept a table), `ducklake_merge_adjacent_files` and
       `ducklake_rewrite_data_files` (table scope) -- transaction 2, a single
       snapshot.

    DuckLake refuses to commit a write (flush) and maintenance in the same
    transaction. `variants` are the `variant` columns read in the pre-check;
    they are used to translate a possible `VARIANT` failure of the `CALL`.
    With ``compact=False`` (``sync`` without ``--compact``) only transaction 1
    runs. `rewrite_delete_threshold` becomes the rewrite's `delete_threshold`
    argument (for this call only); `None` keeps DuckLake's
    `rewrite_delete_threshold` option.
    """
    before, next_file_id, before_time, schema_version = _snapshot_head(con)
    lit_alias, schema, name = (
        _lit(alias),
        _lit(table.schema_name),
        _lit(table.table_name),
    )

    # transaction 1 -- write (autocommit)
    try:
        flushed = _retry_on_conflict(
            lambda: _call(
                con,
                f"CALL ducklake_flush_inlined_data({lit_alias}, "
                f"schema_name => {schema}, table_name => {name})",
            ),
            f"flush of {table.schema_name}.{table.table_name}",
        )
    except duckdb.Error as exc:
        return TableCheckpointResult(
            table,
            before,
            _max_snapshot(con),
            error=_translate(
                table, "flush", exc, variants, snapshot_before=before, con=con
            ),
            next_file_id_before=next_file_id,
            snapshot_before_time=before_time,
            schema_version_before=schema_version,
        )
    rows_flushed = sum(int(r["rows_flushed"]) for r in flushed)

    # transaction 2 -- expire, merge, rewrite (in this order)
    expired: tuple[int, ...] = ()
    merge = rewrite = CompactionStep()
    if not compact:
        return TableCheckpointResult(
            table,
            before,
            _max_snapshot(con),
            rows_flushed=rows_flushed,
            next_file_id_before=next_file_id,
            snapshot_before_time=before_time,
            schema_version_before=schema_version,
        )
    phase: Phase = "expire"
    threshold_arg = (
        f", delete_threshold => {float(rewrite_delete_threshold)!r}"
        if rewrite_delete_threshold is not None
        else ""
    )

    def maintenance() -> tuple[tuple[int, ...], CompactionStep, CompactionStep]:
        nonlocal phase
        phase = "expire"
        try:
            if target_file_size_bytes is not None:
                con.raw.execute(
                    f"SET ducklake_target_file_size = '{target_file_size_bytes}B'"
                )
            con.raw.execute("BEGIN TRANSACTION")
            expired = tuple(
                int(r["snapshot_id"])
                for r in _call(con, f"CALL ducklake_expire_snapshots({lit_alias})")
            )
            phase = "merge"
            merge = _step(
                _call(
                    con,
                    f"CALL ducklake_merge_adjacent_files({lit_alias}, {name}, "
                    f"schema => {schema})",
                )
            )
            phase = "rewrite"
            rewrite = _step(
                _call(
                    con,
                    f"CALL ducklake_rewrite_data_files({lit_alias}, {name}, "
                    f"schema => {schema}{threshold_arg})",
                )
            )
            con.raw.execute("COMMIT")
        except duckdb.Error:
            try:
                con.raw.execute("ROLLBACK")
            except duckdb.Error:
                LOG.debug(
                    "ROLLBACK with no active transaction (commit already aborted)"
                )
            raise
        return expired, merge, rewrite

    try:
        expired, merge, rewrite = _retry_on_conflict(
            maintenance, f"compaction of {table.schema_name}.{table.table_name}"
        )
    except duckdb.Error as exc:
        snapshot_after = _max_snapshot(con)
        return TableCheckpointResult(
            table,
            before,
            snapshot_after,
            rows_flushed=rows_flushed,
            error=_translate(
                table, phase, exc, variants, snapshot_before=before, con=con
            ),
            next_file_id_before=next_file_id,
            snapshot_before_time=before_time,
            schema_version_before=schema_version,
        )

    snapshot_after = _max_snapshot(con)
    effective_target = effective_target_file_size(con, table, target_file_size_bytes)
    try:
        partitions = partition_size_report(
            con, table, snapshot_after, effective_target, closed_after_seconds
        )
    except (CatalogError, duckdb.Error) as exc:
        # optional report: maintenance already committed and must not become an error
        LOG.debug("per-partition distribution of %s failed", table, exc_info=exc)
        partitions = None
    return TableCheckpointResult(
        table,
        before,
        snapshot_after,
        rows_flushed=rows_flushed,
        snapshots_expired=expired,
        merge=merge,
        rewrite=rewrite,
        file_size_distribution=file_size_distribution(
            con, table, snapshot_after, effective_target
        ),
        partition_distribution=partitions,
        next_file_id_before=next_file_id,
        snapshot_before_time=before_time,
        schema_version_before=schema_version,
    )


def _run_writes(
    cfg: DuckLakeIcebergSettings,
    scope: TableScope,
    fp_ro: CatalogFingerprint,
    variants: dict[int, tuple[VariantColumn, ...]],
    *,
    compact: bool = True,
    target_file_size_bytes: int | None = None,
    rewrite_delete_threshold: float | None = None,
    closed_after_seconds: int = DEFAULT_CLOSED_AFTER_SECONDS,
) -> tuple[tuple[TableCheckpointResult, ...], tuple[str, ...]]:
    """Write connection: rechecks the fingerprint and runs the checkpoints."""
    warnings: list[str] = []
    con = open_catalog(cfg, read_only=False)
    try:
        fp_rw = read_fingerprint(con)
        divergence = fp_ro.divergence(fp_rw)
        if divergence is not None:
            raise DivergenceError(
                "the catalog opened for writing is not the one read in "
                f"READ_ONLY ({divergence}) -- nothing was written",
                hint="review the DSN; the DuckLake may have been created by mistake",
            )
        if fp_rw.table_count != fp_ro.table_count:
            warnings.append(
                f"table count changed between reads "
                f"({fp_ro.table_count} -> {fp_rw.table_count}): concurrent DDL"
            )
        if fp_rw.max_snapshot_id != fp_ro.max_snapshot_id:
            warnings.append(
                f"snapshot advanced between reads ({fp_ro.max_snapshot_id} -> "
                f"{fp_rw.max_snapshot_id}): concurrent write"
            )
        results = tuple(
            checkpoint_table(
                con,
                con.alias,
                t,
                variants.get(t.table_id, ()),
                compact=compact,
                target_file_size_bytes=target_file_size_bytes,
                rewrite_delete_threshold=rewrite_delete_threshold,
                closed_after_seconds=closed_after_seconds,
            )
            for t in scope.tables
        )
    finally:
        con.close()
    return results, tuple(warnings)


def _aggregate_exit(results: tuple[TableCheckpointResult, ...]) -> ExitCode:
    """Errors dominate; then revalidation (remaining inlined data 3, blocker 2);
    with none of those, 0 if any table changed, otherwise 4."""
    worst = (
        ExitCode.SUCCESS if any(r.changed for r in results) else ExitCode.NOTHING_TO_DO
    )
    for r in results:
        if r.error is not None:
            worst = worse_exit(worst, r.error.exit_code)
        if r.revalidation_error is not None:
            worst = worse_exit(worst, ExitCode.ERROR)
        if r.revalidation is not None and r.revalidation.exit_code != ExitCode.SUCCESS:
            worst = worse_exit(worst, r.revalidation.exit_code)
    return worst


def _revalidate(
    cfg: DuckLakeIcebergSettings, results: tuple[TableCheckpointResult, ...]
) -> tuple[TableCheckpointResult, ...]:
    """Revalidate the tables without a phase error.

    A revalidation failure does not discard the checkpoint result: it already
    committed and is reported with the revalidation error alongside.
    """
    eligible = [r for r in results if r.error is None]
    requests = [
        RevalidationRequest(
            r.table, r.snapshot_before, r.snapshot_after, r.next_file_id_before
        )
        for r in eligible
    ]
    try:
        revalidations = revalidate_many(cfg, requests)
    except (DuckLakeIcebergError, duckdb.Error) as exc:
        LOG.debug("revalidation failed", exc_info=exc)
        message = getattr(exc, "message", str(exc))
        return tuple(
            dataclasses.replace(r, revalidation_error=f"revalidation failed: {message}")
            if r.error is None
            else r
            for r in results
        )
    by_id = {rev.table.table_id: rev for rev in revalidations}
    return tuple(
        dataclasses.replace(r, revalidation=by_id.get(r.table.table_id))
        if r.error is None
        else r
        for r in results
    )


@overload
def run_checkpoint(
    cfg: DuckLakeIcebergSettings,
    *,
    table: str | None = ...,
    schema: str | None = ...,
    all_tables: bool = ...,
    dry_run: Literal[False] = ...,
    assume_yes: bool = ...,
    confirm: Callable[[int], bool] | None = ...,
    con: DuckLakeConnection | None = ...,
    compact: bool = ...,
    target_file_size_bytes: int | None = ...,
    rewrite_delete_threshold: float | None = ...,
    closed_after_seconds: int = ...,
) -> CheckpointRunReport: ...


@overload
def run_checkpoint(
    cfg: DuckLakeIcebergSettings,
    *,
    table: str | None = ...,
    schema: str | None = ...,
    all_tables: bool = ...,
    dry_run: Literal[True],
    assume_yes: bool = ...,
    confirm: Callable[[int], bool] | None = ...,
    con: DuckLakeConnection | None = ...,
    compact: bool = ...,
    target_file_size_bytes: int | None = ...,
    rewrite_delete_threshold: float | None = ...,
    closed_after_seconds: int = ...,
) -> DryRunReport: ...


def run_checkpoint(
    cfg: DuckLakeIcebergSettings,
    *,
    table: str | None = None,
    schema: str | None = None,
    all_tables: bool = False,
    dry_run: bool = False,
    assume_yes: bool = False,
    confirm: Callable[[int], bool] | None = None,
    con: DuckLakeConnection | None = None,
    compact: bool = True,
    target_file_size_bytes: int | None = None,
    rewrite_delete_threshold: float | None = None,
    closed_after_seconds: int = DEFAULT_CLOSED_AFTER_SECONDS,
) -> CheckpointRunReport | DryRunReport:
    """Orchestrate the command. Never opens a write connection in `dry_run`.

    `confirm` is called with the number of tables in scope when
    `--all-tables` is used without `--yes`; if it returns False, the run aborts
    without writing. `None` skips the prompt (non-interactive run).

    `con` is the READ_ONLY connection lent by `sync` for the read phase (it is
    not closed here). With `compact=False` (`sync` without `--compact`), only
    tables with pending inlined data go to the write phase, and only the flush
    runs; with nothing pending, no write connection is opened.

    `rewrite_delete_threshold` applies to the rewrite for this run only, and
    `closed_after_seconds` is the window that classifies a partition as closed
    in the report."""
    if target_file_size_bytes is not None:
        if target_file_size_bytes <= 0:
            raise ConfigError("--target-file-size must be greater than zero")
        if not compact:
            raise ConfigError("--target-file-size requires checkpoint with --compact")
    if rewrite_delete_threshold is not None:
        if not 0.0 <= rewrite_delete_threshold <= 1.0:
            raise ConfigError("--rewrite-delete-threshold must be between 0 and 1")
        if not compact:
            raise ConfigError(
                "--rewrite-delete-threshold requires checkpoint with --compact"
            )
    if closed_after_seconds < 0:
        raise ConfigError("--closed-after cannot be negative")
    warnings: list[str] = []

    # --- READ_ONLY phase ---------------------------------------------------
    with metadata_catalog(cfg, con) as ro, ro.read_transaction():
        snapshot_before = current_snapshot(ro).snapshot_id
        scope = resolve_scope(ro, table=table, schema=schema, all_tables=all_tables)

        # --- dry-run, no writes -------------------------------------------
        # `dry_run_checkpoint` receives the RO connection and never reopens
        # it: the no-write guarantee is structural, it does not depend on the
        # flow below.
        if dry_run:
            return dry_run_checkpoint(
                ro,
                scope,
                snapshot_before,
                target_file_size_bytes,
                rewrite_delete_threshold=rewrite_delete_threshold,
                closed_after_seconds=closed_after_seconds,
            )

        fp_ro = read_fingerprint(ro)
        statuses = {
            t.table_id: detect_inlined(ro.raw, ro.meta, t, snapshot_before)
            for t in scope.tables
        }
        variants = {
            t.table_id: find_variant_columns(ro.raw, ro.meta, t, snapshot_before)
            for t in scope.tables
        }
        disabled = _auto_compact_disabled(ro, scope)

    for t in scope.tables:
        warnings.extend(f"{t.qualified}: {w}" for w in statuses[t.table_id].warnings)
        if t.table_id in disabled:
            warnings.append(f"auto_compact is off for {t.qualified}")

    # --- nested VARIANT blocks before any write ---------------------------
    # Evaluated per table and BEFORE "nothing to do": a table with a nested
    # variant is exit 2 even without pending inlined data.
    nested_blocked: dict[int, TableCheckpointResult] = {}
    for t in scope.tables:
        nested = find_nested_variants(variants[t.table_id])
        if nested:
            nested_blocked[t.table_id] = TableCheckpointResult(
                t,
                snapshot_before,
                snapshot_before,
                error=nested_variant_error(t, nested, detected="pre_check"),
            )
    runnable = tuple(t for t in scope.tables if t.table_id not in nested_blocked)
    # without compaction, a table with no pending inlined data has nothing to write
    untouched: dict[int, TableCheckpointResult] = {}
    if not compact:
        untouched = {
            t.table_id: TableCheckpointResult(t, snapshot_before, snapshot_before)
            for t in runnable
            if not statuses[t.table_id].has_pending
        }
        runnable = tuple(t for t in runnable if t.table_id not in untouched)

    # --- interactive confirmation of --all-tables --------------------------
    # The prompt only blocks on an interactive terminal; `--yes` skips it;
    # non-interactive runs proceed (the real barrier is the fingerprint, not
    # the prompt).
    if (
        runnable
        and scope.origin == "all"
        and not assume_yes
        and confirm is not None
        and not confirm(len(runnable))
    ):
        raise OperationAborted(
            f"--all-tables checkpoint canceled by the user "
            f"({len(runnable)} table(s) in scope); nothing was written",
            hint="use --yes to skip the confirmation",
        )

    # --- writes only for tables without a nested variant -------------------
    if runnable:
        runnable_scope = TableScope(
            tables=runnable, origin=scope.origin, schema_filter=scope.schema_filter
        )
        written, write_warnings = _run_writes(
            cfg,
            runnable_scope,
            fp_ro,
            variants,
            compact=compact,
            target_file_size_bytes=target_file_size_bytes,
            rewrite_delete_threshold=rewrite_delete_threshold,
            closed_after_seconds=closed_after_seconds,
        )
        warnings.extend(write_warnings)

        # --- re-read on a new connection ----------------------------------
        with open_catalog(cfg, read_only=True, s3=False) as con:
            snapshot_after = current_snapshot(con).snapshot_id
            # --- report on the snapshots created by the checkpoint ---------
            written = tuple(
                dataclasses.replace(
                    r,
                    snapshots=build_snapshot_report(
                        con,
                        r.table,
                        snapshot_before=r.snapshot_before,
                        snapshot_before_time=r.snapshot_before_time,
                        schema_version_before=r.schema_version_before,
                    ),
                )
                for r in written
            )

        # --- doctor revalidation on the target snapshot -------------------
        written = _revalidate(cfg, written)
    else:
        written = ()
        snapshot_after = snapshot_before

    by_id = {r.table.table_id: r for r in written}
    results = tuple(
        nested_blocked.get(t.table_id) or untouched.get(t.table_id) or by_id[t.table_id]
        for t in scope.tables
    )

    return CheckpointRunReport(
        version=1,
        dry_run=False,
        catalog_alias=cfg.catalog_alias,
        metadata_catalog=cfg.metadata_catalog,
        snapshot_before=snapshot_before,
        snapshot_after=snapshot_after,
        results=results,
        wrote=any(r.changed for r in results),
        exit_code=_aggregate_exit(results),
        warnings=tuple(warnings),
    )
