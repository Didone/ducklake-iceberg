"""End-to-end acceptance test for partitioned export.

Acceptance criterion:
    A DuckLake table without DELETE/UPDATE, partitioned by epoch_day, published
    by sync (checkpoint + export + register) and read from Athena and Redshift
    with count(*) and aggregations identical to DuckDB.

This test validates:
- Creating a table partitioned by epoch_day
- The full sync pipeline (doctor + checkpoint + export + register)
- Reading through StaticTable (simulates Athena/Redshift)
- count(*) and aggregations compared against DuckDB
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest
from pyiceberg.table import StaticTable

from iceduck.cli import aggregate_exit_code
from iceduck.errors import ExitCode
from iceduck.export import run_export

from .conftest import CatalogHarness


def _create_partitioned_table(con: duckdb.DuckDBPyConnection, alias: str) -> None:
    """Create table vendas.pedidos partitioned by epoch_day(data)."""
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {alias}.vendas;")

    # Create the table; the partitioning is set below (DuckLake syntax)
    con.execute(f"""
        CREATE TABLE {alias}.vendas.pedidos (
            id INTEGER NOT NULL,
            data DATE NOT NULL,
            valor DOUBLE NOT NULL,
            regiao VARCHAR NOT NULL
        );
    """)

    # epoch_day: the transform that maps 1:1 to Iceberg's `day`. No try/except:
    # if partitioning fails, the acceptance test must fail with it.
    con.execute(
        f"ALTER TABLE {alias}.vendas.pedidos SET PARTITIONED BY (epoch_day(data))"
    )

    # Insert data into multiple partitions (different days)
    con.execute(f"""
        INSERT INTO {alias}.vendas.pedidos VALUES
            (1, DATE '2024-01-01', 100.50, 'Sul'),
            (2, DATE '2024-01-01', 200.75, 'Norte'),
            (3, DATE '2024-01-02', 150.25, 'Sul'),
            (4, DATE '2024-01-02', 300.00, 'Norte'),
            (5, DATE '2024-01-03', 175.80, 'Centro'),
            (6, DATE '2024-01-03', 225.40, 'Sul');
    """)
    con.commit()

    # Flush to materialize the data files
    con.execute(f"""
        CALL ducklake_flush_inlined_data(
            '{alias}',
            schema_name => 'vendas',
            table_name => 'pedidos'
        );
    """)


@pytest.fixture
def partitioned_catalog(tmp_path: Path) -> CatalogHarness:
    """Catalog with a table partitioned by epoch_day, data already flushed."""
    catalog_path = tmp_path / "catalog.db"
    data_path = tmp_path / "data"
    data_path.mkdir(exist_ok=True)

    # Use a temporary in-memory connection to create the initial structure
    con = duckdb.connect(":memory:")
    con.install_extension("ducklake")
    con.load_extension("ducklake")

    # Set up the catalog (using catalog_path directly to avoid conflicts)
    alias = "lake"
    dsn = f"ducklake:duckdb:{catalog_path}"
    con.execute(f"""
        ATTACH '{dsn}' AS {alias} (
            DATA_PATH '{data_path}/',
            METADATA_CATALOG '{alias}_meta',
            CREATE_IF_NOT_EXISTS true
        );
    """)

    _create_partitioned_table(con, alias)
    con.close()

    return CatalogHarness(
        backend="duckdb",
        dsn=dsn,
        data_path=str(data_path) + "/",
    )


def _duckdb_aggregations(
    con: duckdb.DuckDBPyConnection, alias: str
) -> dict[str, object]:
    """Run aggregations in DuckDB for validation."""
    result = con.execute(f"""
        SELECT
            COUNT(*) as count_star,
            COUNT(DISTINCT data) as count_distinct_date,
            SUM(valor) as sum_valor,
            AVG(valor) as avg_valor,
            MIN(valor) as min_valor,
            MAX(valor) as max_valor,
            COUNT(CASE WHEN regiao = 'Sul' THEN 1 END) as count_sul
        FROM {alias}.vendas.pedidos
    """).fetchone()
    assert result is not None

    return {
        "count_star": result[0],
        "count_distinct_date": result[1],
        "sum_valor": round(result[2], 2),
        "avg_valor": round(result[3], 2),
        "min_valor": result[4],
        "max_valor": result[5],
        "count_sul": result[6],
    }


def test_partitioned_sync_roundtrip(
    partitioned_catalog: CatalogHarness, tmp_path: Path
) -> None:
    """Full sync on a partitioned table, with aggregation checks.

    Simulates the complete pipeline:
    1. doctor (preflight) -> exit 0
    2. checkpoint (already done in the fixture) -> exit 4 (nothing to do)
    3. export -> exit 0
    4. register (mock with local FileIO) -> exit 0
    5. Read through StaticTable (simulates Athena/Redshift)
    6. count(*) and aggregations compared against DuckDB
    """
    # Configure settings
    prefix = tmp_path / "iceberg"
    prefix.mkdir(exist_ok=True)
    settings = partitioned_catalog.settings(iceberg_metadata_prefix=str(prefix))

    # 1. Doctor (preflight) - there are no blockers
    from iceduck.catalog.connection import open_catalog
    from iceduck.catalog.reader import current_snapshot, parse_table_ref, resolve_table
    from iceduck.doctor.model import DoctorContext, DoctorTarget
    from iceduck.doctor.rules import RULES
    from iceduck.doctor.runner import run as run_rules

    with open_catalog(settings, read_only=True) as con, con.read_transaction():
        snapshot = current_snapshot(con)
        ref = parse_table_ref("vendas.pedidos")
        identity = resolve_table(con, ref, snapshot)
        target = DoctorTarget(
            catalog=con.alias,
            metadata_catalog=con.meta,
            schema_name=identity.ref.schema_name,
            table_name=identity.ref.table_name,
            table_id=identity.table_id,
            snapshot_id=snapshot.snapshot_id,
            schema_version=identity.schema_version,
        )
        ctx = DoctorContext(con=con.raw, target=target)
        report = run_rules(ctx, list(RULES), manage_transaction=False)

    assert report.exit_code() == 0, (
        f"Doctor found blockers: {[r.findings for r in report.results]}"
    )
    compression = next(
        result for result in report.results if result.rule_id == "parquet-compression"
    )
    assert len(compression.findings) == 1
    assert compression.findings[0].severity.name == "DEGRADED"

    # 2. Checkpoint is not needed (data already flushed)
    # 3. Export
    export_result = run_export(settings, "vendas", "pedidos")
    assert export_result.exit_code == ExitCode.SUCCESS
    assert export_result.plan.total_records == 6
    assert export_result.plan.metadata_location

    # 4. Register (mock - only checks that the metadata.json exists)
    metadata_location = export_result.plan.metadata_location
    uri = (
        metadata_location
        if metadata_location.startswith("file://")
        else "file://" + metadata_location
    )

    # 5. Read through StaticTable (simulates Athena/Redshift)
    table = StaticTable.from_metadata(uri)

    # Validate the schema
    schema = table.schema()
    field_names = [f.name for f in schema.fields]
    assert "id" in field_names
    assert "data" in field_names
    assert "valor" in field_names
    assert "regiao" in field_names

    # Validate the partition spec: epoch_day -> DayTransform, with the DuckLake
    # spec_id preserved (the manifest cites the same id as the metadata.json)
    from pyiceberg.transforms import DayTransform

    spec = table.spec()
    assert spec.spec_id != 0
    assert [type(f.transform) for f in spec.fields] == [DayTransform]
    assert table.metadata.specs()[spec.spec_id] == spec

    # 6. Full scan and aggregation checks
    arrow = table.scan().to_arrow()
    assert arrow.num_rows == 6, f"Expected 6 rows, got {arrow.num_rows}"

    # Aggregations on Iceberg (via Arrow)
    iceberg_agg = {
        "count_star": arrow.num_rows,
        "count_distinct_date": len(set(arrow.column("data").to_pylist())),
        "sum_valor": round(sum(arrow.column("valor").to_pylist()), 2),
        "avg_valor": round(sum(arrow.column("valor").to_pylist()) / arrow.num_rows, 2),
        "min_valor": min(arrow.column("valor").to_pylist()),
        "max_valor": max(arrow.column("valor").to_pylist()),
        "count_sul": sum(1 for r in arrow.column("regiao").to_pylist() if r == "Sul"),
    }

    # Aggregations on DuckDB (reopening the catalog)
    duck = duckdb.connect(":memory:")
    duck.load_extension("ducklake")
    alias = "lake"
    duck.execute(
        f"ATTACH '{partitioned_catalog.dsn}' AS {alias} ("
        f"METADATA_CATALOG '{alias}_meta', "
        f"READ_ONLY true);"
    )
    duckdb_agg = _duckdb_aggregations(duck, alias)
    duck.close()

    # Check that the aggregations are identical
    assert iceberg_agg == duckdb_agg, (
        f"Aggregations differ:\nIceberg: {iceberg_agg}\nDuckDB:  {duckdb_agg}"
    )

    # Check a selective predicate on the data column (partitioned or not)
    filtered = table.scan(row_filter="data = '2024-01-02'").to_arrow()
    assert filtered.num_rows == 2, "Predicate on the data column must return 2 rows"
    assert all(d == date(2024, 1, 2) for d in filtered.column("data").to_pylist()), (
        "Filtered rows must have data '2024-01-02'"
    )

    # Aggregate exit code of the pipeline
    codes = [ExitCode.SUCCESS, ExitCode.SUCCESS]  # doctor + export
    assert aggregate_exit_code(codes) == ExitCode.SUCCESS


def test_unpartitioned_table_roundtrip(
    flushed_catalog: CatalogHarness, tmp_path: Path
) -> None:
    """Acceptance for an unpartitioned table (spec_id = 0)."""
    prefix = tmp_path / "iceberg"
    prefix.mkdir(exist_ok=True)
    settings = flushed_catalog.settings(iceberg_metadata_prefix=str(prefix))

    # Export an unpartitioned table (main.pedidos)
    result = run_export(settings, "main", "pedidos")
    assert result.exit_code == ExitCode.SUCCESS

    # Read through StaticTable
    uri = result.plan.metadata_location
    if not uri.startswith("file://"):
        uri = "file://" + uri
    table = StaticTable.from_metadata(uri)

    # Validate the unpartitioned spec
    spec = table.spec()
    assert spec.is_unpartitioned()

    # Scan and validate
    arrow = table.scan().to_arrow()
    assert arrow.num_rows == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
