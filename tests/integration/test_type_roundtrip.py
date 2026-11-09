"""Types whose Iceberg reading could be silently wrong, read back by two engines.

DuckLake writes the Parquet files and the tool only references them, so the
Iceberg type must match what is physically in the file:

* ``timestamptz``: an INT64 ``Timestamp(isAdjustedToUTC=true, MICROS)``, i.e.
  an instant in UTC, whatever the session time zone of the writer;
* ``uuid``: a 16-byte ``FIXED_LEN_BYTE_ARRAY`` with the ``UUID`` annotation
  (big-endian, RFC 4122 byte order);
* ``tinyint``/``smallint`` (and their unsigned variants): an INT32 with the
  ``INT(8|16)`` annotation, published as Iceberg ``int``.

Each test writes edge values with DuckLake, exports, and compares the values,
the column bounds in the manifest and a filtered scan (which prunes on those
bounds) against DuckLake, through PyIceberg and the DuckDB ``iceberg``
extension.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import duckdb
import pyarrow.parquet as pq
import pytest
from pyiceberg.conversions import from_bytes
from pyiceberg.expressions import BooleanExpression
from pyiceberg.expressions import EqualTo as _EqualTo
from pyiceberg.expressions import GreaterThanOrEqual as _GreaterThanOrEqual
from pyiceberg.table import StaticTable
from pyiceberg.types import (
    IntegerType,
    PrimitiveType,
    TimestamptzType,
    UUIDType,
)

from ducklake_iceberg.export import ExportResult

from .conftest import CatalogHarness
from .test_incremental import Lake

pytestmark = pytest.mark.integration

UTC = dt.UTC


def _expr(kind: Any, column: str, value: Any) -> BooleanExpression:
    return cast("BooleanExpression", cast("Any", kind)(column, value))


@pytest.fixture
def lake(catalog: CatalogHarness, tmp_path: Path) -> Lake:
    prefix = tmp_path / "iceberg"
    prefix.mkdir()
    return Lake(catalog, prefix)


@pytest.fixture
def reader() -> Iterator[duckdb.DuckDBPyConnection]:
    con = duckdb.connect(":memory:")
    try:
        con.execute("LOAD iceberg")
    except duckdb.Error:
        con.close()
        pytest.skip("DuckDB iceberg extension unavailable offline")
    try:
        yield con
    finally:
        con.close()


def _table(result: ExportResult) -> StaticTable:
    return StaticTable.from_metadata("file://" + result.plan.metadata_location)


def _bounds(table: StaticTable, column: str) -> tuple[Any, Any]:
    """Min lower and max upper bound of ``column`` over the live data files.

    Read from the plan (the manifests), not from ``inspect``: PyIceberg's
    ``inspect.data_files`` cannot build its Arrow table for ``uuid`` bounds.
    """
    field = table.schema().find_field(column)
    field_type = cast("PrimitiveType", field.field_type)
    lower: Any = None
    upper: Any = None
    for task in table.scan().plan_files():
        lo = (task.file.lower_bounds or {}).get(field.field_id)
        hi = (task.file.upper_bounds or {}).get(field.field_id)
        if lo is not None:
            value = from_bytes(field_type, lo)
            lower = value if lower is None else min(lower, value)
        if hi is not None:
            value = from_bytes(field_type, hi)
            upper = value if upper is None else max(upper, value)
    return lower, upper


def _parquet_files(lake: Lake) -> list[Path]:
    return sorted(Path(lake.settings.data_path).rglob("*.parquet"))


# -- timestamptz -------------------------------------------------------------
def test_timestamptz_is_an_utc_instant(
    lake: Lake, reader: duckdb.DuckDBPyConnection
) -> None:
    # written from a session in a non-UTC time zone, with offsets in the literals
    lake.write(
        "SET TimeZone = 'America/Sao_Paulo'",
        "CREATE TABLE lake.main.t(id INTEGER, ts TIMESTAMPTZ)",
    )
    lake.catalog.write(
        "SET TimeZone = 'America/Sao_Paulo'; "
        "INSERT INTO lake.main.t VALUES "
        "(1, TIMESTAMPTZ '2024-06-01 12:00:00.123456-03'), "
        "(2, TIMESTAMPTZ '2024-01-01 00:00:00+00'), "
        "(3, TIMESTAMPTZ '1969-12-31 23:59:59.999999+00'), "
        "(4, NULL)"
    )
    expected = {
        1: dt.datetime(2024, 6, 1, 15, 0, 0, 123456, tzinfo=UTC),
        2: dt.datetime(2024, 1, 1, tzinfo=UTC),
        3: dt.datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
        4: None,
    }
    for path in _parquet_files(lake):
        ts_type = pq.read_schema(path).field("ts").type
        assert str(ts_type) == "timestamp[us, tz=UTC]"

    result = lake.export()
    table = _table(result)
    assert isinstance(table.schema().find_field("ts").field_type, TimestamptzType)
    got = {r["id"]: r["ts"] for r in table.scan().to_arrow().to_pylist()}
    assert {k: (v.astimezone(UTC) if v else None) for k, v in got.items()} == expected

    # bounds are instants too, so pruning by an UTC instant is correct
    lower, upper = _bounds(table, "ts")  # microseconds since the epoch, in UTC
    assert (lower, upper) == (
        int(expected[3].timestamp() * 1_000_000) if expected[3] else None,
        int(expected[1].timestamp() * 1_000_000) if expected[1] else None,
    )
    since = dt.datetime(2024, 1, 1, tzinfo=UTC).isoformat()
    filtered = table.scan(row_filter=_expr(_GreaterThanOrEqual, "ts", since))
    assert sorted(r["id"] for r in filtered.to_arrow().to_pylist()) == [1, 2]

    reader.execute("SET TimeZone = 'UTC'")
    rows = reader.execute(
        f"SELECT id, epoch_us(ts) FROM iceberg_scan('{result.plan.metadata_location}') "
        "ORDER BY id"
    ).fetchall()
    assert rows == [
        (i, int(v.timestamp() * 1_000_000) if v else None) for i, v in expected.items()
    ]


# -- uuid --------------------------------------------------------------------
def test_uuid_bytes_are_rfc4122_big_endian(
    lake: Lake, reader: duckdb.DuckDBPyConnection
) -> None:
    values = [
        uuid.UUID("00112233-4455-6677-8899-aabbccddeeff"),
        uuid.UUID("ffeeddcc-bbaa-9988-7766-554433221100"),
        uuid.UUID("80000000-0000-0000-0000-000000000001"),
    ]
    rows = ", ".join(f"({i}, '{v}')" for i, v in enumerate(values, start=1))
    lake.write(
        "CREATE TABLE lake.main.t(id INTEGER, u UUID)",
        f"INSERT INTO lake.main.t VALUES {rows}, (4, NULL)",
    )
    for path in _parquet_files(lake):
        column = pq.ParquetFile(path).schema.column(1)
        assert column.physical_type == "FIXED_LEN_BYTE_ARRAY"
        assert str(column.logical_type) == "UUID"
        assert column.length == 16
    # the 16 stored bytes are in RFC 4122 (big-endian) order
    relation = duckdb.sql(
        "SELECT hex(u::BLOB) FROM read_parquet("
        f"{[str(p) for p in _parquet_files(lake)]}, binary_as_string = false) "
        "ORDER BY id"
    )
    assert relation is not None
    raw = relation.fetchall()
    assert [r[0] for r in raw] == [v.hex.upper() for v in values] + [None]

    result = lake.export()
    table = _table(result)
    assert isinstance(table.schema().find_field("u").field_type, UUIDType)
    got = {r["id"]: r["u"] for r in table.scan().to_arrow().to_pylist()}
    as_uuid = {
        k: (uuid.UUID(bytes=v) if isinstance(v, bytes) else v) for k, v in got.items()
    }
    assert as_uuid == {1: values[0], 2: values[1], 3: values[2], 4: None}

    # uuid bounds are deliberately not published (no exact conversion from the
    # catalog text): pruning is lost, the filtered scan stays correct
    assert _bounds(table, "u") == (None, None)
    hit = table.scan(row_filter=_expr(_EqualTo, "u", values[2]))
    assert [r["id"] for r in hit.to_arrow().to_pylist()] == [3]

    rows_duckdb = reader.execute(
        f"SELECT id, u::VARCHAR FROM iceberg_scan('{result.plan.metadata_location}') "
        "ORDER BY id"
    ).fetchall()
    assert rows_duckdb == [
        (1, str(values[0])),
        (2, str(values[1])),
        (3, str(values[2])),
        (4, None),
    ]


# -- tinyint / smallint ------------------------------------------------------
def test_small_integers_widen_to_int_without_changing_values(
    lake: Lake, reader: duckdb.DuckDBPyConnection
) -> None:
    lake.write(
        "CREATE TABLE lake.main.t(id INTEGER, i8 TINYINT, i16 SMALLINT, "
        "u8 UTINYINT, u16 USMALLINT)",
        "INSERT INTO lake.main.t VALUES "
        "(1, -128, -32768, 0, 0), (2, 127, 32767, 255, 65535), "
        "(3, 0, 0, 128, 32768), (4, NULL, NULL, NULL, NULL)",
    )
    for path in _parquet_files(lake):
        schema = pq.ParquetFile(path).schema
        logical = {
            schema.column(i).name: str(schema.column(i).logical_type) for i in range(5)
        }
        assert logical["i8"] == "Int(bitWidth=8, isSigned=true)"
        assert logical["u16"] == "Int(bitWidth=16, isSigned=false)"

    result = lake.export()
    table = _table(result)
    for column in ("i8", "i16", "u8", "u16"):
        assert isinstance(table.schema().find_field(column).field_type, IntegerType)
    expected = [
        (1, -128, -32768, 0, 0),
        (2, 127, 32767, 255, 65535),
        (3, 0, 0, 128, 32768),
        (4, None, None, None, None),
    ]
    got = sorted(tuple(r.values()) for r in table.scan().to_arrow().to_pylist())
    assert got == expected
    assert lake.rows() == expected

    # unsigned values above the signed range keep their value in the bounds
    assert _bounds(table, "i8") == (-128, 127)
    assert _bounds(table, "u8") == (0, 255)
    assert _bounds(table, "u16") == (0, 65535)
    for column, value, ids in (
        ("u8", 255, [2]),
        ("u16", 32768, [3]),
        ("i8", -128, [1]),
    ):
        hit = table.scan(row_filter=_expr(_EqualTo, column, value))
        assert sorted(r["id"] for r in hit.to_arrow().to_pylist()) == ids

    rows_duckdb = reader.execute(
        f"SELECT * FROM iceberg_scan('{result.plan.metadata_location}') ORDER BY id"
    ).fetchall()
    assert rows_duckdb == expected
