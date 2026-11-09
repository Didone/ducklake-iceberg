"""Published output validated by a second engine, independent of PyIceberg.

The reader is the DuckDB ``iceberg`` extension (C++): ``iceberg_scan`` over the
published ``metadata.json``, without going through PyIceberg or the Glue
catalog. Each scenario compares the count, per-column aggregates, a selective
predicate and the row set against DuckLake **at the published snapshot**
(``AT (VERSION => ducklake_snapshot_id)``), not at the current snapshot.

The scenarios exercise what position-delete consolidation changes: one delete
file per spec and partition covering several data files, several partitions
and specs (partition evolution), incremental cycles with DELETE/UPDATE/MERGE,
and DuckLake compaction between exports.

Without the extension in the local cache (offline environment), the module is
skipped.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import duckdb
import pytest

from ducklake_iceberg.errors import ExitCode
from ducklake_iceberg.export import ExportResult

from .conftest import CatalogHarness
from .test_incremental import Lake

pytestmark = pytest.mark.integration

#: scenario table columns; ``p`` is the partition column
COLUMNS = ("id", "p", "v", "s")

#: selective predicate: spans partitions and hits rows touched by UPDATE
PREDICATE = "p IN (1, 3) AND id % 5 = 2"

#: builds the query from the relation (DuckLake ``AT`` or ``iceberg_scan``)
Query = Callable[[str], str]


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


@pytest.fixture
def lake(catalog: CatalogHarness, tmp_path: Path) -> Lake:
    prefix = tmp_path / "iceberg"
    prefix.mkdir()
    return Lake(catalog, prefix)


def _aggregates_sql(relation: str, where: str = "") -> str:
    parts = ["count(*)"]
    for col in COLUMNS:
        parts += [
            f"count({col})",
            f"count(DISTINCT {col})",
            f"min({col})",
            f"max({col})",
        ]
        if col != "s":
            parts.append(f"sum({col})")
    clause = f" WHERE {where}" if where else ""
    return f"SELECT {', '.join(parts)} FROM {relation}{clause}"


def _ducklake(lake: Lake, sql_for: Query, snapshot_id: int) -> list[tuple[Any, ...]]:
    con = duckdb.connect(":memory:")
    try:
        for ext in ("ducklake", lake.catalog.backend):
            con.execute(f"LOAD {ext}")
        con.execute(
            f"ATTACH '{lake.catalog.dsn}' AS lake "
            f"(METADATA_CATALOG 'lake_meta', READ_ONLY)"
        )
        relation = f"lake.main.t AT (VERSION => {int(snapshot_id)})"
        return con.execute(sql_for(relation)).fetchall()
    finally:
        con.close()


def _iceberg(
    reader: duckdb.DuckDBPyConnection, sql_for: Query, location: str
) -> list[tuple[Any, ...]]:
    relation = f"iceberg_scan('{location}')"
    return reader.execute(sql_for(relation)).fetchall()


def assert_second_reader_agrees(
    lake: Lake, reader: duckdb.DuckDBPyConnection, result: ExportResult
) -> None:
    """Compare ``iceberg_scan`` with DuckLake at the published snapshot."""
    assert result.exit_code is ExitCode.SUCCESS
    location = result.plan.metadata_location
    snapshot_id = result.plan.ducklake_snapshot_id
    queries: list[Query] = [
        lambda rel: f"SELECT * FROM {rel} ORDER BY ALL",
        lambda rel: _aggregates_sql(rel),
        lambda rel: _aggregates_sql(rel, PREDICATE),
        lambda rel: f"SELECT id, v, s FROM {rel} WHERE {PREDICATE} ORDER BY id",
    ]
    for query in queries:
        expected = _ducklake(lake, query, snapshot_id)
        assert expected, "an empty scenario proves nothing"
        got = _iceberg(reader, query, location)
        assert got == expected, query(f"<snapshot {snapshot_id}>")


def _delete_files(reader: duckdb.DuckDBPyConnection, location: str) -> int:
    """Live delete files in the snapshot, as counted by DuckDB itself."""
    row = reader.execute(
        "SELECT count(*) FROM iceberg_metadata($loc) "
        "WHERE manifest_content = 'DELETE' AND status <> 'DELETED'",
        {"loc": location},
    ).fetchone()
    assert row is not None
    return int(row[0])


def _insert(lo: int, hi: int) -> str:
    return (
        "INSERT INTO lake.main.t SELECT i, i % 4, i * 10, 'r' || i "
        f"FROM range({lo}, {hi}) r(i)"
    )


def test_consolidated_deletes_across_partitions(
    lake: Lake, reader: duckdb.DuckDBPyConnection
) -> None:
    """Several data files per partition, one consolidated delete file each."""
    lake.write(
        "CREATE TABLE lake.main.t (id INTEGER, p INTEGER, v BIGINT, s VARCHAR)",
        "ALTER TABLE lake.main.t SET PARTITIONED BY (p)",
        _insert(0, 40),
        _insert(40, 80),
        _insert(80, 120),
        "DELETE FROM lake.main.t WHERE id % 7 = 0",
    )
    result = lake.export()
    assert_second_reader_agrees(lake, reader, result)
    # 3 data files per partition, 4 partitions with deletes
    assert _delete_files(reader, result.plan.metadata_location) == 4


def test_incremental_cycles_with_specs_and_compaction(
    lake: Lake, reader: duckdb.DuckDBPyConnection
) -> None:
    """DELETE/UPDATE/MERGE cycles, spec evolution and compaction between exports."""
    lake.write(
        "CREATE TABLE lake.main.t (id INTEGER, p INTEGER, v BIGINT, s VARCHAR)",
        "ALTER TABLE lake.main.t SET PARTITIONED BY (p)",
        _insert(0, 60),
    )
    assert_second_reader_agrees(lake, reader, lake.export())

    steps = [
        _insert(60, 100),
        "DELETE FROM lake.main.t WHERE id % 6 = 0",
        "UPDATE lake.main.t SET v = -v, s = 'u' || id WHERE id BETWEEN 10 AND 30",
        # second spec: the old files stay in the previous spec
        "ALTER TABLE lake.main.t SET PARTITIONED BY (bucket(3, id))",
        _insert(100, 140),
        "DELETE FROM lake.main.t WHERE id % 5 = 1",
        "MERGE INTO lake.main.t AS tgt USING (SELECT i AS id, i % 4 AS p, "
        "i * 100 AS v, 'm' || i AS s FROM range(90, 150) r(i)) src "
        "ON tgt.id = src.id "
        "WHEN MATCHED THEN UPDATE SET v = src.v, s = src.s "
        "WHEN NOT MATCHED THEN INSERT VALUES (src.id, src.p, src.v, src.s)",
        "CALL ducklake_merge_adjacent_files('lake', 't')",
        "DELETE FROM lake.main.t WHERE id % 9 = 4",
        "CALL ducklake_rewrite_data_files('lake', 't', delete_threshold => 0.2)",
        "UPDATE lake.main.t SET s = NULL WHERE id % 11 = 3",
        "DELETE FROM lake.main.t WHERE p = 2",
    ]
    for sql in steps:
        lake.write(sql)
        result = lake.export()  # ALTER also publishes (new default spec)
        assert_second_reader_agrees(lake, reader, result)
    metadata = json.loads(Path(result.plan.metadata_location).read_text())
    assert len(metadata["partition-specs"]) == 3  # unpartitioned, p, bucket(3, id)


def test_fully_deleted_data_file_with_previous_delete_file(
    lake: Lake, reader: duckdb.DuckDBPyConnection
) -> None:
    """A dangling delete file is not a divergence (``DanglingPolicy.DROP``).

    When a write removes all remaining rows of a data file, DuckLake ends the
    data file and leaves the previous delete file open; ``ducklake_list_files``
    omits it. The export used to abort with ``FileSetDivergence``.
    """
    lake.write(
        "CREATE TABLE lake.main.t (id INTEGER, p INTEGER, v BIGINT, s VARCHAR)",
        _insert(0, 10),
        _insert(10, 20),
        "DELETE FROM lake.main.t WHERE id = 1",
    )
    assert_second_reader_agrees(lake, reader, lake.export())
    lake.write("DELETE FROM lake.main.t WHERE id < 10")
    result = lake.export()
    assert_second_reader_agrees(lake, reader, result)
    lake.assert_mirrors(result)
    assert _delete_files(reader, result.plan.metadata_location) == 0
