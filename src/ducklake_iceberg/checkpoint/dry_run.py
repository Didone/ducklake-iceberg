"""`--dry-run` of the checkpoint.

Reports, per table and without executing anything, what the checkpoint would
do: inlined rows the flush would materialize and whether compaction would
happen (an estimate, since `rewrite`/`merge` have no native `dry_run`). It
blocks on nested `VARIANT`, because reporting "3 rows would be moved" when the
checkpoint would fail is worse than not reporting.

The central guarantee is structural: `dry_run_checkpoint` **receives** the
already open READ_ONLY connection and has no access to the factory -- there is
no way to open a write connection, not even by mistake. The count and the
snapshot come from the same read transaction, opened by the caller.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Final, Literal

from ducklake_iceberg.catalog.connection import DuckLakeConnection
from ducklake_iceberg.checkpoint.detect import (
    InlinedCount,
    InlinedStatus,
    detect_inlined,
)
from ducklake_iceberg.checkpoint.model import (
    FileSizeDistribution,
    PartitionSizeReport,
    TableRef,
    TableScope,
)
from ducklake_iceberg.checkpoint.partitions import (
    DEFAULT_CLOSED_AFTER_SECONDS,
    file_size_distribution,
    partition_size_report,
)
from ducklake_iceberg.checkpoint.variant import (
    find_nested_variants,
    find_variant_columns,
)
from ducklake_iceberg.doctor.model import quote_ident
from ducklake_iceberg.errors import ExitCode

#: Documented extension defaults, used when the option is not in
#: `ducklake_metadata`. To verify: exact values per version -- the report
#: prints the value actually used in the estimate.
_DEFAULT_TARGET_FILE_SIZE: Final[int] = 100_000_000  # ~100 MB (verified)
_DEFAULT_REWRITE_DELETE_THRESHOLD: Final[float] = 0.95


@dataclass(frozen=True, slots=True)
class TableDryRun:
    table: TableRef
    rows_pending: int
    data_rows: int
    delete_rows: int
    sources: tuple[InlinedCount, ...]
    blocked_by: tuple[str, ...] = ()
    merge_candidates: int = 0
    rewrite_candidates: int = 0
    auto_compact: bool = True
    target_file_size_bytes: int | None = None
    rewrite_delete_threshold: float | None = None
    #: current state (before the checkpoint)
    file_size_distribution: FileSizeDistribution | None = None
    partition_distribution: PartitionSizeReport | None = None

    @property
    def would_flush(self) -> bool:
        return self.rows_pending > 0 and not self.blocked_by

    @property
    def would_compact(self) -> bool:
        """Estimate: merge needs >= 2 candidates; rewrite needs >= 1."""
        return (
            self.auto_compact
            and not self.blocked_by
            and (self.merge_candidates >= 2 or self.rewrite_candidates >= 1)
        )

    @property
    def would_change(self) -> bool:
        return self.would_flush or self.would_compact

    def to_json(self) -> dict[str, Any]:
        return {
            # minimal keys; ``after`` is the dry-run prediction
            "table": self.table.qualified,
            "inlined_rows_before": self.rows_pending,
            "inlined_rows_after": 0 if self.would_flush else self.rows_pending,
            "schema_name": self.table.schema_name,
            "table_name": self.table.table_name,
            "table_id": self.table.table_id,
            "rows_pending": self.rows_pending,
            "data_rows": self.data_rows,
            "delete_rows": self.delete_rows,
            "merge_candidates": self.merge_candidates,
            "rewrite_candidates": self.rewrite_candidates,
            "auto_compact": self.auto_compact,
            "target_file_size_bytes": self.target_file_size_bytes,
            "rewrite_delete_threshold": self.rewrite_delete_threshold,
            "file_size_distribution": (
                self.file_size_distribution.to_json()
                if self.file_size_distribution is not None
                else None
            ),
            "partition_distribution": (
                self.partition_distribution.to_json()
                if self.partition_distribution is not None
                else None
            ),
            "would_flush": self.would_flush,
            "would_compact": self.would_compact,
            "blocked_by": list(self.blocked_by),
            "sources": [c.to_json() for c in self.sources],
        }


@dataclass(frozen=True, slots=True)
class DryRunReport:
    version: Literal[1]
    snapshot_id: int
    catalog_alias: str
    metadata_catalog: str
    tables: tuple[TableDryRun, ...]
    warnings: tuple[str, ...] = field(default_factory=tuple)
    wrote: bool = False  # invariant: always False

    @property
    def rows_pending(self) -> int:
        return sum(t.rows_pending for t in self.tables)

    @property
    def tables_with_pending(self) -> int:
        return sum(1 for t in self.tables if t.rows_pending > 0)

    @property
    def tables_with_work(self) -> int:
        return sum(1 for t in self.tables if t.would_change)

    @property
    def blocked(self) -> bool:
        return any(t.blocked_by for t in self.tables)

    @property
    def exit_code(self) -> ExitCode:
        """Nested VARIANT 2 · estimated work 0 · nothing to do 4."""
        if self.blocked:
            return ExitCode.BLOCKED
        if any(t.would_change for t in self.tables):
            return ExitCode.SUCCESS
        return ExitCode.NOTHING_TO_DO

    def to_json(self) -> dict[str, Any]:
        """Envelope compatible with the real run (`dry_run: true`, `wrote: false`)."""
        return {
            "version": self.version,
            "command": "checkpoint",
            "dry_run": True,
            "wrote": False,
            "catalog": {
                "alias": self.catalog_alias,
                "metadata_catalog": self.metadata_catalog,
            },
            "snapshot_id": self.snapshot_id,
            "snapshot_before": self.snapshot_id,
            "snapshot_after": None,
            "tables": [t.to_json() for t in self.tables],
            "totals": {
                "tables_in_scope": len(self.tables),
                "tables_with_pending": self.tables_with_pending,
                "tables_with_work": self.tables_with_work,
                "rows_pending": self.rows_pending,
            },
            "warnings": list(self.warnings),
            "exit_code": int(self.exit_code),
        }


# -- compaction estimate --------------------------------------


def _option(con: DuckLakeConnection, key: str, table: TableRef) -> str | None:
    """Option value resolved by scope (table > schema > global).

    Read from `ducklake_metadata`; None when not configured (use the default).
    """
    meta = quote_ident(con.meta)
    rows = con.query_arrow(
        f"SELECT value, scope, scope_id FROM {meta}.ducklake_metadata WHERE key = $key",
        {"key": key},
    ).to_pylist()
    if not rows:
        return None
    # precedence: table > schema > global
    by_table = [
        r for r in rows if r["scope"] == "table" and r["scope_id"] == table.table_id
    ]
    if by_table:
        return str(by_table[0]["value"])
    schema_rows = con.query_arrow(
        f"SELECT schema_id FROM {meta}.ducklake_table WHERE table_id = $table_id",
        {"table_id": table.table_id},
    ).to_pylist()
    schema_id = schema_rows[0]["schema_id"] if schema_rows else None
    by_schema = [
        r for r in rows if r["scope"] == "schema" and r["scope_id"] == schema_id
    ]
    if by_schema:
        return str(by_schema[0]["value"])
    glob = [r for r in rows if r["scope"] is None]
    if glob:
        return str(glob[0]["value"])
    return str(rows[0]["value"])


def effective_target_file_size(
    con: DuckLakeConnection,
    table: TableRef,
    override_bytes: int | None = None,
) -> int:
    if override_bytes is not None:
        return override_bytes
    target_raw = _option(con, "target_file_size", table)
    return int(target_raw) if target_raw else _DEFAULT_TARGET_FILE_SIZE


def _compaction_estimate(
    con: DuckLakeConnection,
    table: TableRef,
    snapshot_id: int,
    target_file_size_bytes: int | None = None,
    rewrite_delete_threshold: float | None = None,
) -> tuple[int, int, int, float]:
    """(`merge_candidates`, `rewrite_candidates`, target_file_size, threshold)."""
    target = effective_target_file_size(con, table, target_file_size_bytes)
    if rewrite_delete_threshold is not None:
        threshold = rewrite_delete_threshold
    else:
        thr_raw = _option(con, "rewrite_delete_threshold", table)
        threshold = float(thr_raw) if thr_raw else _DEFAULT_REWRITE_DELETE_THRESHOLD

    meta = quote_ident(con.meta)
    rows = con.query_arrow(
        f"SELECT count(*) FILTER (WHERE df.file_size_bytes < $target) "
        f"    AS merge_candidates, "
        f"count(*) FILTER (WHERE del.delete_count IS NOT NULL "
        f"    AND df.record_count > 0 "
        f"    AND del.delete_count::DOUBLE / df.record_count >= $threshold) "
        f"    AS rewrite_candidates "
        f"FROM {meta}.ducklake_data_file AS df "
        f"LEFT JOIN {meta}.ducklake_delete_file AS del "
        f"  ON del.data_file_id = df.data_file_id "
        f" AND $snapshot_id >= del.begin_snapshot "
        f" AND (del.end_snapshot IS NULL OR $snapshot_id < del.end_snapshot) "
        f"WHERE df.table_id = $table_id "
        f"  AND $snapshot_id >= df.begin_snapshot "
        f"  AND (df.end_snapshot IS NULL OR $snapshot_id < df.end_snapshot)",
        {
            "target": target,
            "threshold": threshold,
            "snapshot_id": snapshot_id,
            "table_id": table.table_id,
        },
    ).to_pylist()
    merge = int(rows[0]["merge_candidates"]) if rows else 0
    rewrite = int(rows[0]["rewrite_candidates"]) if rows else 0
    return merge, rewrite, target, threshold


def _auto_compact_enabled(con: DuckLakeConnection, table: TableRef) -> bool:
    value = _option(con, "auto_compact", table)
    return value is None or value.lower() != "false"


def _one(
    con: DuckLakeConnection,
    table: TableRef,
    status: InlinedStatus,
    snapshot_id: int,
    warnings: list[str],
    target_file_size_bytes: int | None,
    rewrite_delete_threshold: float | None = None,
    closed_after_seconds: int = DEFAULT_CLOSED_AFTER_SECONDS,
) -> TableDryRun:
    variants = find_variant_columns(con.raw, con.meta, table, snapshot_id)
    nested = find_nested_variants(variants)
    blocked: tuple[str, ...] = ("E-VARIANT-NESTED",) if nested else ()

    auto_compact = _auto_compact_enabled(con, table)
    effective_target = effective_target_file_size(con, table, target_file_size_bytes)
    if auto_compact and not nested:
        merge, rewrite, target, threshold = _compaction_estimate(
            con, table, snapshot_id, target_file_size_bytes, rewrite_delete_threshold
        )
        if merge >= 2 or rewrite >= 1:
            warnings.append(
                f"{table.qualified}: compaction estimate with "
                f"target_file_size={target}, rewrite_delete_threshold={threshold:g}"
            )
    else:
        merge = rewrite = 0
        if not auto_compact:
            warnings.append(f"auto_compact is off for {table.qualified}")
    for w in status.warnings:
        warnings.append(f"{table.qualified}: {w}")

    return TableDryRun(
        table=table,
        rows_pending=status.pending_rows,
        data_rows=status.pending_data_rows,
        delete_rows=status.pending_delete_rows,
        sources=status.counts,
        blocked_by=blocked,
        merge_candidates=merge,
        rewrite_candidates=rewrite,
        auto_compact=auto_compact,
        target_file_size_bytes=effective_target,
        rewrite_delete_threshold=rewrite_delete_threshold,
        file_size_distribution=file_size_distribution(
            con, table, snapshot_id, effective_target
        ),
        partition_distribution=partition_size_report(
            con, table, snapshot_id, effective_target, closed_after_seconds
        ),
    )


def dry_run_checkpoint(
    con: DuckLakeConnection,
    scope: TableScope,
    snapshot_id: int,
    target_file_size_bytes: int | None = None,
    *,
    rewrite_delete_threshold: float | None = None,
    closed_after_seconds: int = DEFAULT_CLOSED_AFTER_SECONDS,
) -> DryRunReport:
    """Runs reads only. Receives the connection already opened in READ_ONLY
    and never reopens it."""
    warnings: list[str] = []
    tables = tuple(
        _one(
            con,
            t,
            detect_inlined(con.raw, con.meta, t, snapshot_id),
            snapshot_id,
            warnings,
            target_file_size_bytes,
            rewrite_delete_threshold,
            closed_after_seconds,
        )
        for t in scope.tables
    )
    return DryRunReport(
        version=1,
        snapshot_id=snapshot_id,
        catalog_alias=con.alias,
        metadata_catalog=con.meta,
        tables=tables,
        warnings=tuple(warnings),
    )
