"""--materialize-deletes.

Operational escape hatch that eliminates the delete problem at the source by
compacting the table in DuckLake before export, rewriting the affected data
files with the deleted rows already removed. It deliberately violates the
zero-copy principle.
"""

from __future__ import annotations

import dataclasses
import logging
import time
from typing import TYPE_CHECKING

from ducklake_iceberg.doctor.model import quote_ident
from ducklake_iceberg.errors import DuckLakeIcebergError, NothingToDo

if TYPE_CHECKING:
    from ducklake_iceberg.catalog.connection import DuckLakeConnection
    from ducklake_iceberg.config import DuckLakeIcebergSettings

LOG = logging.getLogger(__name__)


# -- exceptions ---------------------------------------------------------------


class MaterializeError(DuckLakeIcebergError):
    """Error while materializing deletes."""


class ReadOnlyDatabaseError(MaterializeError):
    """Tried to materialize on a READ_ONLY connection."""

    def __init__(self) -> None:
        super().__init__(
            "--materialize-deletes requires a write connection to DuckLake",
            hint="Internal bug: the command must reopen without READ_ONLY",
        )


class CompactionConflictError(MaterializeError):
    """Transaction conflict with a concurrent write."""

    def __init__(self) -> None:
        super().__init__(
            "compaction conflicted with a concurrent write",
            hint="Retry outside the write window",
        )


class NoDeletesError(NothingToDo):
    """No deletions to materialize."""

    def __init__(self, snapshot_id: int) -> None:
        super().__init__(
            f"no deletions to materialize in snapshot {snapshot_id}",
            hint="Export without the flag",
        )


class NoFilesProcessedError(NothingToDo):
    """No file reached delete_threshold."""

    def __init__(self, threshold: float) -> None:
        super().__init__(
            f"no file reached delete_threshold={threshold}",
            hint="Use --delete-threshold 0.0 or export without the flag",
        )


# -- data contracts -----------------------------------------------------


@dataclasses.dataclass(frozen=True, slots=True)
class MaterializePlan:
    """Materialization estimate, produced in READ_ONLY.

    All numbers are approximate: delete_count includes rows from future
    snapshots in a partial file, and the function's internal criterion is
    not documented.
    """

    table_id: int
    snapshot_id: int
    delete_files: int
    deleted_rows: int
    data_files_affected: int
    bytes_to_rewrite: int
    max_deleted_fraction: float
    has_puffin: bool

    @property
    def is_empty(self) -> bool:
        """Return True if there is nothing to materialize."""
        return self.delete_files == 0


@dataclasses.dataclass(frozen=True, slots=True)
class MaterializeResult:
    """Materialization result."""

    schema_name: str
    table_name: str
    files_processed: int
    files_created: int
    snapshot_before: int
    snapshot_after: int
    duration_ms: int

    @property
    def no_op(self) -> bool:
        """Return True if no file was processed."""
        return self.files_processed == 0


# -- main functions -----------------------------------------------------


def plan_materialize(
    con: DuckLakeConnection,
    table_id: int,
    snapshot_id: int,
) -> MaterializePlan:
    """Produce a materialization estimate without executing it.

    Runs in READ_ONLY. Numbers are approximate.

    Args:
        con: DuckDB connection (READ_ONLY)
        table_id: Table ID
        snapshot_id: Target snapshot

    Returns:
        MaterializePlan with the estimates

    Raises:
        NoDeletesError: if there are no live delete files
    """
    meta = quote_ident(con.meta)
    sql = f"""
        SELECT
            count(*)                                      AS delete_files,
            coalesce(sum(del.delete_count), 0)           AS deleted_rows,
            count(DISTINCT del.data_file_id)             AS data_files_affected,
            coalesce(sum(df.file_size_bytes), 0)         AS bytes_to_rewrite,
            coalesce(
                max(del.delete_count * 1.0 / nullif(df.record_count, 0)),
                0.0
            )                                             AS max_deleted_fraction,
            coalesce(bool_or(lower(del.format) = 'puffin'), false) AS has_puffin
        FROM {meta}.ducklake_delete_file AS del
        JOIN {meta}.ducklake_data_file   AS df
          ON df.data_file_id = del.data_file_id
         AND df.table_id     = del.table_id
         AND $snapshot_id >= df.begin_snapshot
         AND (df.end_snapshot IS NULL OR $snapshot_id < df.end_snapshot)
        WHERE del.table_id = $table_id
          AND $snapshot_id >= del.begin_snapshot
          AND (del.end_snapshot IS NULL OR $snapshot_id < del.end_snapshot);
    """

    params = {"table_id": table_id, "snapshot_id": snapshot_id}
    rows = con.query_arrow(sql, params).to_pylist()
    if not rows or not rows[0]["delete_files"]:
        raise NoDeletesError(snapshot_id)

    row = rows[0]
    plan = MaterializePlan(
        table_id=table_id,
        snapshot_id=snapshot_id,
        delete_files=int(row["delete_files"]),
        deleted_rows=int(row["deleted_rows"]),
        data_files_affected=int(row["data_files_affected"]),
        bytes_to_rewrite=int(row["bytes_to_rewrite"]),
        max_deleted_fraction=float(row["max_deleted_fraction"]),
        has_puffin=bool(row["has_puffin"]),
    )

    LOG.info(
        "Materialization plan: %d delete files, %d affected data files, ~%.2f MB",
        plan.delete_files,
        plan.data_files_affected,
        plan.bytes_to_rewrite / (1024 * 1024),
    )

    return plan


def materialize_deletes(
    con: DuckLakeConnection,
    *,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    snapshot_before: int,
    delete_threshold: float = 0.0,
    max_compacted_files: int | None = None,
) -> MaterializeResult:
    """Materialize deletes by rewriting data files.

    Requires a connection WITHOUT READ_ONLY. Never called under --dry-run.

    Args:
        con: DuckDB connection (NOT READ_ONLY)
        catalog_name: DuckLake catalog name
        schema_name: Schema name
        table_name: Table name
        snapshot_before: Snapshot before the operation
        delete_threshold: Deletion threshold (0.0 = all files)
        max_compacted_files: File limit (None = no limit)

    Returns:
        MaterializeResult with the results

    Raises:
        NoFilesProcessedError: if no file was processed
        ReadOnlyDatabaseError: if the connection is READ_ONLY
        CompactionConflictError: if there was a transaction conflict
    """
    start_time = time.time()

    # Names go through named parameters: never interpolated into the SQL.
    params: dict[str, object] = {
        "catalog": catalog_name,
        "table": table_name,
        "schema": schema_name,
        "threshold": float(delete_threshold),
    }
    limit = ""
    if max_compacted_files is not None:
        limit = ", max_compacted_files => $max_files"
        params["max_files"] = int(max_compacted_files)
    sql = (
        "CALL ducklake_rewrite_data_files($catalog, $table, schema => $schema, "
        f"delete_threshold => $threshold{limit})"
    )

    LOG.info(
        "Materializing deletes: table=%s.%s, threshold=%.2f",
        schema_name,
        table_name,
        delete_threshold,
    )

    try:
        rows = con.query_arrow(sql, params).to_pylist()
    except Exception as e:
        error_msg = str(e).lower()
        if any(t in error_msg for t in ("readonly", "read-only", "read_only")):
            raise ReadOnlyDatabaseError() from e
        if "conflict" in error_msg or "concurrent" in error_msg:
            raise CompactionConflictError() from e
        raise MaterializeError(f"Failed to materialize deletes: {e}") from e

    # One row per rewritten group; no rows means no file reached the threshold.
    files_processed = sum(int(r["files_processed"]) for r in rows)
    if files_processed == 0:
        raise NoFilesProcessedError(delete_threshold)

    snapshot_after = int(
        con.query_arrow(
            f"SELECT max(snapshot_id) AS s FROM {quote_ident(con.meta)}"
            ".ducklake_snapshot"
        ).to_pylist()[0]["s"]
    )
    duration_ms = int((time.time() - start_time) * 1000)

    materialize_result = MaterializeResult(
        schema_name=schema_name,
        table_name=table_name,
        files_processed=files_processed,
        files_created=sum(int(r["files_created"]) for r in rows),
        snapshot_before=snapshot_before,
        snapshot_after=snapshot_after,
        duration_ms=duration_ms,
    )

    LOG.info(
        "Materialization finished: %d files processed, %d created, "
        "snapshot %d → %d (%.2fs)",
        materialize_result.files_processed,
        materialize_result.files_created,
        snapshot_before,
        snapshot_after,
        duration_ms / 1000,
    )

    return materialize_result


def format_plan(plan: MaterializePlan) -> str:
    """Format a materialization plan for display.

    Args:
        plan: Materialization plan

    Returns:
        Formatted string for display
    """
    lines = [
        "Materialization estimate:",
        f"  Delete files: {plan.delete_files}",
        f"  Deleted rows: ~{plan.deleted_rows:,}",
        f"  Affected data files: {plan.data_files_affected}",
        f"  Bytes to rewrite: ~{plan.bytes_to_rewrite:,} "
        f"({plan.bytes_to_rewrite / (1024 * 1024):.2f} MB)",
        f"  Max. deleted fraction: {plan.max_deleted_fraction:.2%}",
    ]

    if plan.has_puffin:
        lines.append("  ⚠️  Contains deletes in puffin format")

    lines.extend(
        [
            "",
            "Note: Numbers are estimates. delete_count includes rows from",
            "      future snapshots in partial files.",
        ]
    )

    return "\n".join(lines)


def format_result(result: MaterializeResult) -> str:
    """Format a materialization result for display.

    Args:
        result: Materialization result

    Returns:
        Formatted string for display
    """
    return (
        f"{result.files_processed} files rewritten, "
        f"{result.files_created} created; "
        f"snapshot {result.snapshot_before} → {result.snapshot_after} "
        f"({result.duration_ms / 1000:.2f}s)"
    )


def run_materialize(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    *,
    dry_run: bool = False,
    delete_threshold: float = 0.0,
) -> dict[str, object]:
    """``--materialize-deletes`` for one table: a plan under ``--dry-run``,
    otherwise a rewrite in DuckLake. Returns the step's JSON block;
    ``NothingToDo`` (exit 4) when there is no deletion or no file reaches the
    threshold."""
    from ducklake_iceberg.catalog.connection import open_catalog
    from ducklake_iceberg.catalog.reader import (
        current_snapshot,
        parse_table_ref,
        resolve_table,
    )

    ref = parse_table_ref(f"{schema_name}.{table_name}")
    with open_catalog(settings, read_only=True) as con, con.read_transaction():
        snapshot = current_snapshot(con)
        identity = resolve_table(con, ref, snapshot)
        plan = plan_materialize(con, identity.table_id, snapshot.snapshot_id)
    if dry_run:
        return {"plan": dataclasses.asdict(plan), "dry_run": True}

    with open_catalog(settings, read_only=False) as con:
        result = materialize_deletes(
            con,
            catalog_name=con.alias,
            schema_name=identity.ref.schema_name,
            table_name=identity.ref.table_name,
            snapshot_before=snapshot.snapshot_id,
            delete_threshold=delete_threshold,
        )
    return {"plan": dataclasses.asdict(plan), "result": dataclasses.asdict(result)}
