from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Final
from uuid import UUID

from ducklake_iceberg.errors import CatalogError, ConfigError, IdentityError

if TYPE_CHECKING:
    import pyarrow as pa

    from ducklake_iceberg.catalog.connection import DuckLakeConnection

LOG = logging.getLogger(__name__)

DEFAULT_SCHEMA: Final[str] = "main"

#: Snapshot columns, normalizing snapshot_time with CAST.
_SNAPSHOT_COLUMNS: Final[str] = (
    "snapshot_id, "
    "CAST(snapshot_time AS TIMESTAMPTZ) AS snapshot_time, "
    "schema_version, next_catalog_id, next_file_id"
)


@dataclass(frozen=True, slots=True)
class Snapshot:
    snapshot_id: int
    snapshot_time: datetime
    schema_version: int
    next_catalog_id: int
    next_file_id: int


@dataclass(frozen=True, slots=True)
class TableRef:
    schema_name: str
    table_name: str

    def __str__(self) -> str:
        return f"{self.schema_name}.{self.table_name}"


@dataclass(frozen=True, slots=True)
class TableIdentity:
    ref: TableRef
    table_id: int
    table_uuid: UUID
    schema_id: int
    schema_version: int
    table_path: str | None
    table_path_is_relative: bool
    schema_path: str | None
    schema_path_is_relative: bool
    snapshot: Snapshot


@dataclass(frozen=True, slots=True)
class InlinedDataTable:
    table_id: int
    physical_name: str  # ducklake_inlined_data_<table_id>_<schema_version>
    schema_version: int
    row_count: int  # 0 after flush; > 0 blocks the export


def _meta(con: DuckLakeConnection) -> str:
    """Name of the METADATA_CATALOG, the only interpolated element (validated)."""
    return con.meta


def _rows(table: pa.Table) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = table.to_pylist()
    return rows


def _int(value: object) -> int:
    if isinstance(value, bool):  # bool is a subtype of int; make the intent explicit
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    raise CatalogError(f"unexpected numeric value: {value!r}")


def _as_uuid(value: object) -> UUID:
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _row_to_snapshot(row: dict[str, object]) -> Snapshot:
    return Snapshot(
        snapshot_id=_int(row["snapshot_id"]),
        snapshot_time=_as_datetime(row["snapshot_time"]),
        schema_version=_int(row["schema_version"]),
        next_catalog_id=_int(row["next_catalog_id"]),
        next_file_id=_int(row["next_file_id"]),
    )


def _as_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    raise CatalogError(f"unexpected snapshot_time: {value!r}")


def parse_table_ref(ref: str, *, default_schema: str = DEFAULT_SCHEMA) -> TableRef:
    """'sales.orders' -> TableRef('sales','orders'); 'orders' uses the default."""
    parts = _split_identifier(ref)
    if len(parts) == 1:
        (table_name,) = parts
        schema_name = default_schema
    elif len(parts) == 2:  # schema.table
        schema_name, table_name = parts
    else:
        raise ConfigError(f"invalid reference {ref!r}")

    if not schema_name or not table_name:
        raise ConfigError(f"invalid reference {ref!r}")
    return TableRef(schema_name=schema_name, table_name=table_name)


def _split_identifier(ref: str) -> list[str]:
    """Split on `.` honoring double quotes, as DuckDB does."""
    if not ref:
        raise ConfigError("invalid reference ''")
    parts: list[str] = []
    current: list[str] = []
    in_quotes = False
    i = 0
    while i < len(ref):
        char = ref[i]
        if char == '"':
            if in_quotes and i + 1 < len(ref) and ref[i + 1] == '"':
                current.append('"')
                i += 2
                continue
            in_quotes = not in_quotes
            i += 1
            continue
        if char == "." and not in_quotes:
            parts.append("".join(current))
            current = []
            i += 1
            continue
        current.append(char)
        i += 1
    if in_quotes:
        raise ConfigError(f"invalid reference {ref!r}")
    parts.append("".join(current))
    return parts


def current_snapshot(con: DuckLakeConnection) -> Snapshot:
    """Highest snapshot_id in the catalog. IdentityError if there is none."""
    meta = _meta(con)
    table = con.query_arrow(
        f"SELECT {_SNAPSHOT_COLUMNS} "
        f"FROM {meta}.ducklake_snapshot "
        "ORDER BY snapshot_id DESC LIMIT 1"
    )
    rows = _rows(table)
    if not rows:
        raise IdentityError(
            "catalog has no snapshots",
            hint="does the DSN point to an empty DuckLake? check catalog_dsn",
        )
    return _row_to_snapshot(rows[0])


def snapshot_at(con: DuckLakeConnection, snapshot_id: int) -> Snapshot:
    """A specific snapshot; used by status/sync."""
    meta = _meta(con)
    table = con.query_arrow(
        f"SELECT {_SNAPSHOT_COLUMNS} "
        f"FROM {meta}.ducklake_snapshot "
        "WHERE snapshot_id = $snapshot_id",
        {"snapshot_id": snapshot_id},
    )
    rows = _rows(table)
    if not rows:
        raise IdentityError(
            f"snapshot {snapshot_id} does not exist or has expired",
            hint="review the DuckLake snapshot retention",
        )
    return _row_to_snapshot(rows[0])


def resolve_table(
    con: DuckLakeConnection, ref: TableRef, snapshot: Snapshot
) -> TableIdentity:
    """Identity of the table visible at the snapshot. IdentityError if absent."""
    meta = _meta(con)
    sql = (
        "SELECT t.table_id, t.table_uuid, t.table_name, "
        "t.path AS table_path, t.path_is_relative AS table_path_is_relative, "
        "s.schema_id, s.schema_name, "
        "s.path AS schema_path, s.path_is_relative AS schema_path_is_relative "
        f"FROM {meta}.ducklake_table t "
        f"JOIN {meta}.ducklake_schema s "
        "  ON s.schema_id = t.schema_id "
        " AND $snapshot_id >= s.begin_snapshot "
        " AND (s.end_snapshot IS NULL OR $snapshot_id < s.end_snapshot) "
        "WHERE s.schema_name = $schema_name "
        "  AND t.table_name = $table_name "
        "  AND $snapshot_id >= t.begin_snapshot "
        "  AND (t.end_snapshot IS NULL OR $snapshot_id < t.end_snapshot)"
    )
    params = {
        "snapshot_id": snapshot.snapshot_id,
        "schema_name": ref.schema_name,
        "table_name": ref.table_name,
    }
    rows = _rows(con.query_arrow(sql, params))

    if len(rows) > 1:
        raise CatalogError(
            f"ambiguous identity for {ref} at snapshot {snapshot.snapshot_id}",
            hint="interval predicate bug; open an issue with the snapshot",
        )
    if not rows:
        raise _absent_table_error(con, ref, snapshot)

    row = rows[0]
    table_id = _int(row["table_id"])
    schema_version = table_schema_version(con, table_id, snapshot)
    return TableIdentity(
        ref=ref,
        table_id=table_id,
        table_uuid=_as_uuid(row["table_uuid"]),
        schema_id=_int(row["schema_id"]),
        schema_version=schema_version,
        table_path=_opt_str(row["table_path"]),
        table_path_is_relative=bool(row["table_path_is_relative"]),
        schema_path=_opt_str(row["schema_path"]),
        schema_path_is_relative=bool(row["schema_path_is_relative"]),
        snapshot=snapshot,
    )


def _opt_str(value: object) -> str | None:
    return None if value is None else str(value)


def _absent_table_error(
    con: DuckLakeConnection, ref: TableRef, snapshot: Snapshot
) -> IdentityError:
    """Build the absence message, distinguishing a missing schema from a table."""
    schemas = _live_schema_names(con, snapshot)
    if ref.schema_name not in schemas:
        return IdentityError(
            f"schema {ref.schema_name!r} does not exist at snapshot "
            f"{snapshot.snapshot_id}",
            hint="check --schema",
        )
    live = list_tables(con, snapshot, schema_name=ref.schema_name)
    names = ", ".join(str(t) for t in live) or "(none)"
    qualified = str(ref)
    return IdentityError(
        f"table {qualified!r} does not exist at snapshot {snapshot.snapshot_id}",
        hint=f"live tables in {ref.schema_name}: {names}",
    )


def _live_schema_names(con: DuckLakeConnection, snapshot: Snapshot) -> frozenset[str]:
    meta = _meta(con)
    rows = _rows(
        con.query_arrow(
            "SELECT schema_name "
            f"FROM {meta}.ducklake_schema "
            "WHERE $snapshot_id >= begin_snapshot "
            "  AND (end_snapshot IS NULL OR $snapshot_id < end_snapshot)",
            {"snapshot_id": snapshot.snapshot_id},
        )
    )
    return frozenset(str(r["schema_name"]) for r in rows)


def list_tables(
    con: DuckLakeConnection,
    snapshot: Snapshot,
    *,
    schema_name: str | None = None,
) -> tuple[TableRef, ...]:
    """Live tables at the snapshot; filtered by schema when given."""
    meta = _meta(con)
    sql = (
        "SELECT s.schema_name, t.table_name "
        f"FROM {meta}.ducklake_table t "
        f"JOIN {meta}.ducklake_schema s "
        "  ON s.schema_id = t.schema_id "
        " AND $snapshot_id >= s.begin_snapshot "
        " AND (s.end_snapshot IS NULL OR $snapshot_id < s.end_snapshot) "
        "WHERE $snapshot_id >= t.begin_snapshot "
        "  AND (t.end_snapshot IS NULL OR $snapshot_id < t.end_snapshot) "
        "  AND ($schema_name IS NULL OR s.schema_name = $schema_name) "
        "ORDER BY s.schema_name, t.table_name"
    )
    rows = _rows(
        con.query_arrow(
            sql,
            {"snapshot_id": snapshot.snapshot_id, "schema_name": schema_name},
        )
    )
    return tuple(TableRef(str(r["schema_name"]), str(r["table_name"])) for r in rows)


def table_schema_version(
    con: DuckLakeConnection, table_id: int, snapshot: Snapshot
) -> int:
    """Schema version of the table at the snapshot (ducklake_schema_versions)."""
    meta = _meta(con)
    rows = _rows(
        con.query_arrow(
            "SELECT schema_version "
            f"FROM {meta}.ducklake_schema_versions "
            "WHERE table_id = $table_id AND begin_snapshot <= $snapshot_id "
            "ORDER BY begin_snapshot DESC LIMIT 1",
            {"table_id": table_id, "snapshot_id": snapshot.snapshot_id},
        )
    )
    if not rows:
        LOG.debug(
            "no row in ducklake_schema_versions for table_id=%s; "
            "using the snapshot schema_version (%s)",
            table_id,
            snapshot.schema_version,
        )
        return snapshot.schema_version
    return _int(rows[0]["schema_version"])


def inlined_data_table(
    con: DuckLakeConnection, identity: TableIdentity
) -> InlinedDataTable | None:
    """Record and row count of inlined data; None if the table never had any."""
    meta = _meta(con)
    rows = _rows(
        con.query_arrow(
            "SELECT table_id, table_name, schema_version "
            f"FROM {meta}.ducklake_inlined_data_tables "
            "WHERE table_id = $table_id",
            {"table_id": identity.table_id},
        )
    )
    if not rows:
        return None

    # There may be one row per schema_version; pick the current schema's,
    # or the highest schema_version as a fallback.
    row = _select_inlined_row(rows, identity.schema_version)
    physical_name = str(row["table_name"])
    schema_version = _int(row["schema_version"])

    expected = f"ducklake_inlined_data_{identity.table_id}_{schema_version}"
    if physical_name != expected:
        raise CatalogError(
            f"unexpected inlined data naming convention: {physical_name}",
            hint=f"expected {expected}; check the DuckLake version",
        )

    count_rows = _rows(
        con.query_arrow(f'SELECT count(*) AS n FROM {meta}."{physical_name}"')
    )
    row_count = _int(count_rows[0]["n"])
    return InlinedDataTable(
        table_id=identity.table_id,
        physical_name=physical_name,
        schema_version=schema_version,
        row_count=row_count,
    )


def _select_inlined_row(
    rows: list[dict[str, object]], schema_version: int
) -> dict[str, object]:
    for row in rows:
        if _int(row["schema_version"]) == schema_version:
            return row
    return max(rows, key=lambda r: _int(r["schema_version"]))


def catalog_data_path(con: DuckLakeConnection) -> str:
    """Global ducklake_metadata.data_path (scope IS NULL)."""
    meta = _meta(con)
    rows = _rows(
        con.query_arrow(
            "SELECT value "
            f"FROM {meta}.ducklake_metadata "
            "WHERE key = $key AND scope IS NULL",
            {"key": "data_path"},
        )
    )
    if not rows:
        raise CatalogError(
            "catalog has no global data_path",
            hint="inspect ducklake_metadata",
        )
    if len(rows) > 1:
        raise CatalogError("catalog has a duplicate global data_path")
    value = str(rows[0]["value"])
    return value.rstrip("/") + "/"
