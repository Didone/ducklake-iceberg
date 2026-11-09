"""Up-front detection of inlined data.

Answers, before any write and before any file enumeration, "does this table
have pending inlined rows, and how many?". Detection is not satisfied with the
**presence** of a record in `ducklake_inlined_data_tables`: it counts the rows
**visible in the current snapshot** in the physical inlining tables, because
only the count tells "there is work to do" apart from "it was already done".

The pending decision is based on `rows_visible`, never on the presence of a
record nor on `rows_total` -- this is what makes `flush` idempotent and exit
code 4 reliable. The physical names are dynamic: they are validated against
`_PHYSICAL_NAME` and checked against the `table_id` of the queried table
before being interpolated; a name outside the pattern becomes a warning,
never SQL.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, cast

import duckdb

from ducklake_iceberg.checkpoint.model import TableRef
from ducklake_iceberg.doctor.model import quote_ident

LOG = logging.getLogger("ducklake_iceberg")

InlinedKind = Literal["data", "delete"]

#: Physical name of an inlining table. Anchored regex: `LIKE 'ducklake_%'`
#: would match all 29 catalog tables, not just the inlining ones.
_PHYSICAL_NAME = re.compile(r"^ducklake_inlined_(?:data_(\d+)_(\d+)|delete_(\d+))$")

#: Visibility per physical table kind. Data tables have begin and end columns
#: with the `_ducklake_` prefix; delete tables (`file_id, row_id,
#: begin_snapshot`) only have the begin: the deletion holds from there on.
_BEGIN = {"data": "_ducklake_begin_snapshot", "delete": "begin_snapshot"}
_VISIBLE = {
    "data": (
        "$snapshot_id >= _ducklake_begin_snapshot"
        " AND (_ducklake_end_snapshot IS NULL OR $snapshot_id < _ducklake_end_snapshot)"
    ),
    "delete": "$snapshot_id >= begin_snapshot",
}


@dataclass(frozen=True, slots=True)
class InlinedPhysicalTable:
    """Physical inlining table, in the catalog metadata database."""

    physical_name: str  # ducklake_inlined_data_5_0
    kind: InlinedKind
    table_id: int
    schema_version: int | None  # None for delete tables
    registered: bool  # listed in ducklake_inlined_data_tables
    exists: bool  # physically exists in the catalog

    def to_json(self) -> dict[str, Any]:
        return {
            "physical_name": self.physical_name,
            "kind": self.kind,
            "table_id": self.table_id,
            "schema_version": self.schema_version,
            "registered": self.registered,
            "exists": self.exists,
        }


@dataclass(frozen=True, slots=True)
class InlinedCount:
    source: InlinedPhysicalTable
    rows_visible: int  # visible in the evaluated snapshot
    rows_total: int  # all physical rows, visible or not
    visibility_filtered: bool  # False => temporal columns missing
    #: smallest `_ducklake_begin_snapshot` among the visible rows (None without
    #: visible rows or without the temporal columns)
    min_visible_begin: int | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "physical_name": self.source.physical_name,
            "kind": self.source.kind,
            "schema_version": self.source.schema_version,
            "rows_visible": self.rows_visible,
            "rows_total": self.rows_total,
            "registered": self.source.registered,
            "exists": self.source.exists,
            "visibility_filtered": self.visibility_filtered,
            "min_visible_begin": self.min_visible_begin,
        }


@dataclass(frozen=True, slots=True)
class InlinedStatus:
    table: TableRef
    snapshot_id: int
    counts: tuple[InlinedCount, ...] = ()
    warnings: tuple[str, ...] = field(default_factory=tuple)

    @property
    def pending_rows(self) -> int:
        """Sum of `rows_visible` across all physical tables."""
        return sum(c.rows_visible for c in self.counts)

    @property
    def pending_data_rows(self) -> int:
        return sum(c.rows_visible for c in self.counts if c.source.kind == "data")

    @property
    def pending_delete_rows(self) -> int:
        return sum(c.rows_visible for c in self.counts if c.source.kind == "delete")

    @property
    def has_pending(self) -> bool:
        return self.pending_rows > 0

    @property
    def earliest_pending_begin(self) -> int | None:
        """Smallest snapshot that created a pending row; None if unknown."""
        begins = []
        for c in self.counts:
            if c.rows_visible == 0:
                continue
            if c.min_visible_begin is None:
                return None
            begins.append(c.min_visible_begin)
        return min(begins) if begins else None

    def to_json(self) -> dict[str, Any]:
        return {
            "table_id": self.table.table_id,
            "snapshot_id": self.snapshot_id,
            "pending_rows": self.pending_rows,
            "pending_data_rows": self.pending_data_rows,
            "pending_delete_rows": self.pending_delete_rows,
            "sources": [c.to_json() for c in self.counts],
            "warnings": list(self.warnings),
        }


# -- DuckDB access -------------------------------------------------------


def _q(
    con: duckdb.DuckDBPyConnection, sql: str, params: dict[str, Any]
) -> list[dict[str, Any]]:
    used = {name: value for name, value in params.items() if f"${name}" in sql}
    relation = cast("Any", con).sql(sql, params=used)
    table = relation.to_arrow_table()
    rows: list[dict[str, Any]] = table.to_pylist()
    return rows


# -- detection steps -----------------------------------------------------


def _registered_tables(
    con: duckdb.DuckDBPyConnection, meta: str, table_id: int
) -> dict[str, int | None]:
    """Step 1: records in `ducklake_inlined_data_tables`.

    No snapshot filter: the table has no `begin_snapshot`/`end_snapshot`.
    Maps `physical_name -> schema_version`.
    """
    meta_q = quote_ident(meta)
    rows = _q(
        con,
        f"SELECT table_name AS physical_name, schema_version "
        f"FROM {meta_q}.ducklake_inlined_data_tables "
        f"WHERE table_id = $table_id "
        f"ORDER BY schema_version",
        {"table_id": table_id},
    )
    out: dict[str, int | None] = {}
    for row in rows:
        name = str(row["physical_name"])
        ver = row["schema_version"]
        out[name] = int(ver) if ver is not None else None
    return out


def _introspected_tables(con: duckdb.DuckDBPyConnection, meta: str) -> set[str]:
    """Step 2: physical inlining names, by introspection.

    Uses `duckdb_tables()` with an anchored regex; if it does not list the
    attached catalog (Postgres via METADATA_CATALOG), falls back to
    `information_schema`.
    """
    pattern = r"^ducklake_inlined_(data_[0-9]+_[0-9]+|delete_[0-9]+)$"
    try:
        rows = _q(
            con,
            "SELECT table_name FROM duckdb_tables() "
            "WHERE database_name = $meta_catalog "
            "  AND regexp_matches(table_name, $pattern)",
            {"meta_catalog": meta, "pattern": pattern},
        )
    except duckdb.Error:
        rows = []
    if not rows:
        meta_q = quote_ident(meta)
        try:
            rows = _q(
                con,
                f"SELECT table_name FROM {meta_q}.information_schema.tables "
                "WHERE regexp_matches(table_name, $pattern)",
                {"pattern": pattern},
            )
        except duckdb.Error:
            rows = []
    return {str(r["table_name"]) for r in rows}


def _classify(
    physical_name: str,
    table_id: int,
    registered: bool,
    exists: bool,
) -> InlinedPhysicalTable | None:
    """Validate the name, check the `table_id` and build the source.

    Returns None (the caller is responsible for the warning) when the name does
    not match the pattern or the embedded `table_id` differs from the queried
    one.
    """
    match = _PHYSICAL_NAME.match(physical_name)
    if match is None:
        return None
    data_id, data_ver, delete_id = match.groups()
    if data_id is not None:
        if int(data_id) != table_id:
            return None
        return InlinedPhysicalTable(
            physical_name=physical_name,
            kind="data",
            table_id=table_id,
            schema_version=int(data_ver),
            registered=registered,
            exists=exists,
        )
    # delete
    if delete_id is None or int(delete_id) != table_id:
        return None
    return InlinedPhysicalTable(
        physical_name=physical_name,
        kind="delete",
        table_id=table_id,
        schema_version=None,
        registered=registered,
        exists=exists,
    )


def _reconcile(
    registered: dict[str, int | None],
    introspected: set[str],
    table_id: int,
    warnings: list[str],
) -> list[InlinedPhysicalTable]:
    """Reconcile registration and introspection across the 4 possible cases."""
    sources: list[InlinedPhysicalTable] = []
    all_names = set(registered) | introspected
    for name in sorted(all_names):
        is_registered = name in registered
        exists = name in introspected
        physical = _classify(name, table_id, is_registered, exists)
        if physical is None:
            # Introspection is catalog-wide: a well-formed name that belongs to
            # another table (and is not registered for this one) is expected.
            if not is_registered and _PHYSICAL_NAME.match(name) is not None:
                continue
            # Name outside the pattern or mismatched table_id: never interpolate.
            if is_registered or exists:
                warnings.append(
                    f"ignoring table {name!r} with a name outside the inlining "
                    f"pattern (or mismatched table_id)"
                )
            continue
        if is_registered and not exists:
            warnings.append(f"inlining record without a physical table: {name}")
        sources.append(physical)
    return sources


def _count_source(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    source: InlinedPhysicalTable,
    snapshot_id: int,
    warnings: list[str],
) -> InlinedCount:
    """Step 3: visible and total row counts of a physical table."""
    if not source.exists:
        # Orphan record: no physical table, zero visible rows.
        return InlinedCount(
            source=source, rows_visible=0, rows_total=0, visibility_filtered=True
        )
    meta_q = quote_ident(meta)
    name = source.physical_name  # already validated by _classify; safe to interpolate
    predicate, begin_col = _VISIBLE[source.kind], _BEGIN[source.kind]
    try:
        rows = _q(
            con,
            f"SELECT count(*) FILTER (WHERE {predicate}) AS rows_visible, "
            f"min({begin_col}) FILTER (WHERE {predicate}) AS min_begin, "
            f"count(*) AS rows_total "
            f'FROM {meta_q}."{name}"',
            {"snapshot_id": snapshot_id},
        )
        visible = int(rows[0]["rows_visible"]) if rows else 0
        total = int(rows[0]["rows_total"]) if rows else 0
        min_begin = rows[0]["min_begin"] if rows else None
        return InlinedCount(
            source=source,
            rows_visible=visible,
            rows_total=total,
            visibility_filtered=True,
            min_visible_begin=int(min_begin) if min_begin is not None else None,
        )
    except duckdb.Error:
        # Visibility columns missing: fall back to count(*).
        total_rows = _q(
            con, f'SELECT count(*) AS rows_total FROM {meta_q}."{name}"', {}
        )
        total = int(total_rows[0]["rows_total"]) if total_rows else 0
        warnings.append(f"count without visibility filter on {name}")
        return InlinedCount(
            source=source,
            rows_visible=total,
            rows_total=total,
            visibility_filtered=False,
        )


def _cross_check(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    table_id: int,
    snapshot_id: int,
    pending_rows: int,
    warnings: list[str],
) -> None:
    """Optional cross-check: a warning, never a blocker here."""
    meta_q = quote_ident(meta)
    try:
        rows = _q(
            con,
            f"SELECT ts.record_count AS catalog_rows, "
            f"(SELECT count(*) FROM {meta_q}.ducklake_data_file df "
            f" WHERE df.table_id = $table_id "
            f"   AND $snapshot_id >= df.begin_snapshot "
            f"   AND (df.end_snapshot IS NULL "
            f"        OR $snapshot_id < df.end_snapshot)) AS live_files "
            f"FROM {meta_q}.ducklake_table_stats ts "
            f"WHERE ts.table_id = $table_id",
            {"table_id": table_id, "snapshot_id": snapshot_id},
        )
    except duckdb.Error:
        return
    if not rows:
        return
    catalog_rows = rows[0]["catalog_rows"]
    live_files = rows[0]["live_files"]
    if catalog_rows is None or live_files is None:
        return
    if int(live_files) == 0 and int(catalog_rows) > 0 and pending_rows == 0:
        warnings.append(
            f"table with record_count={int(catalog_rows)} > 0, no live files "
            f"and no pending inlining -- investigate before exporting"
        )


def detect_inlined(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    table: TableRef,
    snapshot_id: int,
) -> InlinedStatus:
    """Full detection for one table. Must be called inside the read
    transaction opened by the caller."""
    warnings: list[str] = []
    registered = _registered_tables(con, meta, table.table_id)
    introspected = {
        name
        for name in _introspected_tables(con, meta)
        if _PHYSICAL_NAME.match(name) is not None
    }
    sources = _reconcile(registered, introspected, table.table_id, warnings)

    counts = tuple(
        _count_source(con, meta, src, snapshot_id, warnings) for src in sources
    )
    pending = sum(c.rows_visible for c in counts)
    _cross_check(con, meta, table.table_id, snapshot_id, pending, warnings)

    return InlinedStatus(
        table=table,
        snapshot_id=snapshot_id,
        counts=counts,
        warnings=tuple(warnings),
    )


def _last_flush(
    con: duckdb.DuckDBPyConnection, meta: str, table_id: int, snapshot_id: int
) -> int | None:
    """Latest snapshot <= ``snapshot_id`` that flushed the table's inlining."""
    rows = _q(
        con,
        f"SELECT max(snapshot_id) AS flush "
        f"FROM {quote_ident(meta)}.ducklake_snapshot_changes "
        f"WHERE snapshot_id <= $snapshot_id "
        f"  AND list_contains(string_split(changes_made, ','), $token)",
        {"snapshot_id": snapshot_id, "token": f"inline_flush:{table_id}"},
    )
    flush = rows[0]["flush"] if rows else None
    return int(flush) if flush is not None else None


def clean_snapshot(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    table: TableRef,
    snapshot_id: int,
) -> int:
    """Most recent snapshot <= ``snapshot_id`` with no visible inlined rows.

    It is the export target under concurrent writes: an inlined commit that
    arrives after the flush leaves pending rows in the current snapshot, but
    the snapshot before it already has everything the flush wrote. Each step
    moves to just before the oldest pending row, without going past the
    table's last flush. With no clean snapshot in that range (never flushed,
    row older than the flush, temporal columns missing), returns
    ``snapshot_id``: the caller's inlined guard blocks as before.
    """
    floor = _last_flush(con, meta, table.table_id, snapshot_id)
    if floor is None:
        return snapshot_id
    target = snapshot_id
    while target >= floor:
        status = detect_inlined(con, meta, table, target)
        if not status.has_pending:
            return target
        begin = status.earliest_pending_begin
        if begin is None:
            break
        target = begin - 1
    return snapshot_id


def detect_inlined_many(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    tables: Sequence[TableRef],
    snapshot_id: int,
) -> dict[int, InlinedStatus]:
    """Batch version for `--all-tables`, keyed by `table_id`."""
    return {t.table_id: detect_inlined(con, meta, t, snapshot_id) for t in tables}
