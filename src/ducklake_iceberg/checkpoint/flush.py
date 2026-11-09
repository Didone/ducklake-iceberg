"""Flush of inlined data: the only code that writes to DuckLake.

DuckLake keeps small inserts and deletes inlined in the metadata catalog.
Iceberg needs Parquet files, so ``sync`` flushes the pending inlined data of
the table (``ducklake_flush_inlined_data``) before exporting. It never runs
compaction, ``expire_snapshots`` or file cleanup: replaced files stay readable
by Iceberg readers, and physical cleanup is DuckLake's job.

Sequence: READ_ONLY phase (detection, nested ``VARIANT`` pre-check, catalog
fingerprint); write phase on a new connection without ``READ_ONLY``, rechecking
the fingerprint, so a mistyped DSN never creates an empty lake.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar, cast

import duckdb

from ducklake_iceberg.catalog.connection import (
    DuckLakeConnection,
    metadata_catalog,
    open_catalog,
)
from ducklake_iceberg.catalog.reader import current_snapshot, parse_table_ref
from ducklake_iceberg.checkpoint.detect import detect_inlined
from ducklake_iceberg.checkpoint.model import CatalogFingerprint, TableRef
from ducklake_iceberg.checkpoint.variant import (
    find_nested_variants,
    find_variant_columns,
    nested_variant_error,
    translate_checkpoint_error,
)
from ducklake_iceberg.errors import ConfigError, DivergenceError, ExitCode
from ducklake_iceberg.retry import RetryPolicy

if TYPE_CHECKING:
    from ducklake_iceberg.checkpoint.variant import VariantColumn
    from ducklake_iceberg.config import DuckLakeIcebergSettings

LOG = logging.getLogger("ducklake_iceberg")

T = TypeVar("T")

#: DuckLake commit conflict with another writer of the same table: the
#: rolled-back transaction left no effect on the catalog and is redone from
#: scratch, against the new snapshot.
_CONFLICT = "transaction conflict"
CONFLICT_RETRY = RetryPolicy(
    max_attempts=5, base_delay_s=1.0, factor=2.0, max_delay_s=8.0
)


@dataclass(frozen=True, slots=True)
class FlushResult:
    """Outcome of the flush of one table."""

    table: TableRef
    rows_flushed: int
    snapshot_before: int
    snapshot_after: int

    @property
    def wrote(self) -> bool:
        return self.snapshot_after != self.snapshot_before or self.rows_flushed > 0

    @property
    def exit_code(self) -> ExitCode:
        return ExitCode.SUCCESS if self.wrote else ExitCode.NOTHING_TO_DO


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

    The names come from ``ducklake_schema``/``ducklake_table``, never from the
    command line; the escaping is defense in depth.
    """
    return "'" + value.replace("'", "''") + "'"


def _rows(
    con: DuckLakeConnection, sql: str, params: Any = None
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = con.query_arrow(sql, params).to_pylist()
    return rows


def _resolve_table(
    con: DuckLakeConnection, qualified: str, snapshot_id: int
) -> TableRef:
    if "." not in qualified:
        raise ConfigError(
            f"--table requires a qualified name: <schema>.<table> (got {qualified!r})"
        )
    ref = parse_table_ref(qualified)
    rows = _rows(
        con,
        "SELECT t.table_id FROM "
        f'"{con.meta}".ducklake_table AS t '
        f'JOIN "{con.meta}".ducklake_schema AS s ON s.schema_id = t.schema_id '
        "WHERE s.schema_name = $schema_name AND t.table_name = $table_name "
        "AND $snapshot_id >= t.begin_snapshot "
        "AND ($snapshot_id < t.end_snapshot OR t.end_snapshot IS NULL) "
        "AND $snapshot_id >= s.begin_snapshot "
        "AND ($snapshot_id < s.end_snapshot OR s.end_snapshot IS NULL)",
        {
            "snapshot_id": snapshot_id,
            "schema_name": ref.schema_name,
            "table_name": ref.table_name,
        },
    )
    if not rows:
        raise ConfigError(
            f"table {ref.schema_name}.{ref.table_name} does not exist in "
            f"snapshot {snapshot_id}"
        )
    return TableRef(ref.schema_name, ref.table_name, int(rows[0]["table_id"]))


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

    def opt(value: object) -> str | None:
        return None if value is None else str(value)

    return CatalogFingerprint(
        data_path=opt(head.get("data_path")),
        format_version=opt(head.get("format_version")),
        max_snapshot_id=int(snap) if snap is not None else None,
        table_count=int(count_rows[0]["n"]) if count_rows else 0,
    )


def _max_snapshot(con: DuckLakeConnection) -> int:
    rows = _rows(
        con, f'SELECT max(snapshot_id) AS s FROM "{con.meta}".ducklake_snapshot'
    )
    return int(rows[0]["s"])


def _flush_call(con: DuckLakeConnection, table: TableRef) -> int:
    sql = (
        f"CALL ducklake_flush_inlined_data({_lit(con.alias)}, "
        f"schema_name => {_lit(table.schema_name)}, "
        f"table_name => {_lit(table.table_name)})"
    )
    relation = cast("Any", con.raw).sql(sql)
    if relation is None:
        return 0
    rows: list[dict[str, Any]] = relation.to_arrow_table().to_pylist()
    return sum(int(r["rows_flushed"]) for r in rows)


def flush_inlined(
    cfg: DuckLakeIcebergSettings,
    qualified: str,
    *,
    con: DuckLakeConnection | None = None,
) -> FlushResult:
    """Flush the pending inlined data of ``schema.table``.

    ``con`` is the READ_ONLY connection lent by ``sync`` for the read phase (it
    is not closed here). With nothing pending, no write connection is opened.

    Raises:
        NestedVariantError: a ``VARIANT`` nested in a struct/list/map blocks
            the flush (exit 2), detected before any write.
        CheckpointError: the DuckLake call failed (exit 1).
        DivergenceError: the catalog opened for writing is not the one read.
    """
    with metadata_catalog(cfg, con) as ro, ro.read_transaction():
        snapshot_before = current_snapshot(ro).snapshot_id
        table = _resolve_table(ro, qualified, snapshot_before)
        fingerprint = read_fingerprint(ro)
        status = detect_inlined(ro.raw, ro.meta, table, snapshot_before)
        variants: tuple[VariantColumn, ...] = find_variant_columns(
            ro.raw, ro.meta, table, snapshot_before
        )
    for warning in status.warnings:
        LOG.warning("%s: %s", table.qualified, warning)

    # Evaluated BEFORE "nothing to do": a nested variant blocks even without
    # pending inlined data.
    nested = find_nested_variants(variants)
    if nested:
        raise nested_variant_error(table, nested, detected="pre_check")
    if not status.has_pending:
        return FlushResult(table, 0, snapshot_before, snapshot_before)

    write = open_catalog(cfg, read_only=False)
    try:
        divergence = fingerprint.divergence(read_fingerprint(write))
        if divergence is not None:
            raise DivergenceError(
                "the catalog opened for writing is not the one read in "
                f"READ_ONLY ({divergence}) -- nothing was written",
                hint="review the DSN; the DuckLake may have been created by mistake",
            )
        try:
            rows_flushed = _retry_on_conflict(
                lambda: _flush_call(write, table), f"flush of {table.qualified}"
            )
        except duckdb.Error as exc:
            LOG.debug("flush of %s failed", table.qualified, exc_info=exc)
            after = _max_snapshot(write)
            raise translate_checkpoint_error(
                exc,
                table=table,
                phase="flush",
                variants=variants,
                snapshot_created=after if after > snapshot_before else None,
            ) from exc
        snapshot_after = _max_snapshot(write)
    finally:
        write.close()
    return FlushResult(table, rows_flushed, snapshot_before, snapshot_after)
