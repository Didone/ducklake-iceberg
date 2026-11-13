"""Integration tests of the doctor rules against a real DuckLake catalog (SQLite).

Each fixture **asserts the catalog state it intends to create** before running
the rule; if the DuckDB DDL/DML produces a different state (different option
syntax, different default), the test reports `skip` with the observed value,
never a false green.

Cases whose DuckDB syntax is unverified (deletion vectors, mixed mapping,
encryption) are covered by the classification unit tests; only paths with
known DuckDB syntax are exercised here.
"""

from __future__ import annotations

import pytest

from ducklake_iceberg.catalog.connection import open_catalog
from ducklake_iceberg.catalog.reader import (
    current_snapshot,
    parse_table_ref,
    resolve_table,
)
from ducklake_iceberg.doctor.model import DoctorContext, DoctorTarget, Finding
from ducklake_iceberg.doctor.rules import (
    calendar_transform,
    inlined_data,
    partial_files,
    unsupported_type,
)
from tests.integration.conftest import CatalogHarness

pytestmark = pytest.mark.integration


def _run_rule(
    harness: CatalogHarness, rule: object, table: str, *, snapshot_id: int | None = None
) -> list[Finding]:
    settings = harness.settings()
    with open_catalog(settings, read_only=True) as con, con.read_transaction():
        snapshot = current_snapshot(con)
        identity = resolve_table(con, parse_table_ref(table), snapshot)
        target = DoctorTarget(
            catalog=con.alias,
            metadata_catalog=con.meta,
            schema_name=identity.ref.schema_name,
            table_name=identity.ref.table_name,
            table_id=identity.table_id,
            snapshot_id=snapshot_id
            if snapshot_id is not None
            else snapshot.snapshot_id,
            schema_version=identity.schema_version,
        )
        ctx = DoctorContext(con=con.raw, target=target)
        return list(rule.check(ctx))  # type: ignore[attr-defined]


def _meta_count(harness: CatalogHarness, sql: str) -> int:
    """Count rows in the metadata catalog through a new read connection."""
    settings = harness.settings()
    with open_catalog(settings, read_only=True) as con:
        table = con.query_arrow(sql)
        return int(table.column(0)[0].as_py()) if table.num_rows else 0


# --------------------------------------------------------------------------
# inlined-data
# --------------------------------------------------------------------------
def test_inlined_data_triggers(catalog: CatalogHarness) -> None:
    catalog.write(
        "CREATE SCHEMA IF NOT EXISTS lake.main",
        "CREATE TABLE lake.main.pedidos (id INTEGER, uf VARCHAR, valor DOUBLE)",
        "INSERT INTO lake.main.pedidos VALUES (1, 'SP', 10.5), (2, 'MG', 20.0)",
    )

    # expected state: an inlined data record exists and there is no data file
    inlined_tables = _meta_count(
        catalog,
        "SELECT count(*) FROM lake_meta.ducklake_inlined_data_tables",
    )
    data_files = _meta_count(
        catalog, "SELECT count(*) FROM lake_meta.ducklake_data_file"
    )
    if inlined_tables == 0:
        pytest.skip(
            f"inlining did not occur (data_files={data_files}); "
            "DATA_INLINING_ROW_LIMIT default differs"
        )

    findings = _run_rule(catalog, inlined_data.RULE, "main.pedidos")
    blockers = [f for f in findings if f.severity.name == "BLOCKER"]
    assert blockers, "expected an inlined data blocker"
    assert any("inlined" in f.detail for f in blockers)


def test_inlined_data_passes_after_flush(catalog: CatalogHarness) -> None:
    catalog.write(
        "CREATE SCHEMA IF NOT EXISTS lake.main",
        "CREATE TABLE lake.main.pedidos (id INTEGER, uf VARCHAR)",
        "INSERT INTO lake.main.pedidos VALUES (1, 'SP'), (2, 'MG')",
        (
            "CALL ducklake_flush_inlined_data('lake', schema_name => 'main', "
            "table_name => 'pedidos')"
        ),
    )

    data_files = _meta_count(
        catalog, "SELECT count(*) FROM lake_meta.ducklake_data_file"
    )
    if data_files == 0:
        pytest.skip("flush did not materialize a data file; flush syntax differs")

    findings = _run_rule(catalog, inlined_data.RULE, "main.pedidos")
    blockers = [f for f in findings if f.severity.name == "BLOCKER"]
    assert not blockers


# --------------------------------------------------------------------------
# partial-files: same catalog, two snapshot targets (proves the `>` bound)
# --------------------------------------------------------------------------
def _seed_partial(catalog: CatalogHarness) -> int | None:
    catalog.write(
        "CREATE SCHEMA IF NOT EXISTS lake.main",
        "CREATE TABLE lake.main.pedidos (id INTEGER, uf VARCHAR)",
        "INSERT INTO lake.main.pedidos VALUES (1, 'SP')",
        "INSERT INTO lake.main.pedidos VALUES (2, 'MG')",
        (
            "CALL ducklake_flush_inlined_data('lake', schema_name => 'main', "
            "table_name => 'pedidos')"
        ),
    )
    rows = _meta_rows(
        catalog,
        "SELECT begin_snapshot, partial_max FROM lake_meta.ducklake_data_file "
        "WHERE partial_max IS NOT NULL ORDER BY data_file_id",
    )
    if not rows:
        return None
    return int(str(rows[0]["begin_snapshot"]))


def _meta_rows(harness: CatalogHarness, sql: str) -> list[dict[str, object]]:
    settings = harness.settings()
    with open_catalog(settings, read_only=True) as con:
        result: list[dict[str, object]] = con.query_arrow(sql).to_pylist()
        return result


def test_partial_files_blocks_when_beyond_target(catalog: CatalogHarness) -> None:
    begin = _seed_partial(catalog)
    if begin is None:
        pytest.skip("flush did not produce a partial file; behavior differs")
    # target = begin_snapshot < partial_max -> blocks
    findings = _run_rule(catalog, partial_files.RULE, "main.pedidos", snapshot_id=begin)
    blockers = [f for f in findings if f.severity.name == "BLOCKER"]
    assert blockers, "partial_max > target should block"


def test_partial_files_info_when_within_target(catalog: CatalogHarness) -> None:
    begin = _seed_partial(catalog)
    if begin is None:
        pytest.skip("flush did not produce a partial file; behavior differs")
    # target = current snapshot (>= partial_max) -> INFO only
    findings = _run_rule(catalog, partial_files.RULE, "main.pedidos")
    blockers = [f for f in findings if f.severity.name == "BLOCKER"]
    infos = [f for f in findings if f.severity.name == "INFO"]
    assert not blockers
    assert len(infos) == 1


# --------------------------------------------------------------------------
# unsupported-type
# --------------------------------------------------------------------------
def test_unsupported_type_triggers(catalog: CatalogHarness) -> None:
    catalog.write(
        "CREATE SCHEMA IF NOT EXISTS lake.main",
        (
            "CREATE TABLE lake.main.pedidos ("
            "  id INTEGER, qtd UINTEGER, criado_em TIMESTAMP_MS,"
            "  medicao STRUCT(duracao INTERVAL))"
        ),
    )
    findings = _run_rule(catalog, unsupported_type.RULE, "main.pedidos")
    blockers = [f for f in findings if f.severity.name == "BLOCKER"]
    assert len(blockers) == 3
    assert any("medicao.duracao" in f.subject for f in blockers)


def test_unsupported_type_passes(catalog: CatalogHarness) -> None:
    catalog.write(
        "CREATE SCHEMA IF NOT EXISTS lake.main",
        (
            "CREATE TABLE lake.main.pedidos ("
            "  id BIGINT, criado_em TIMESTAMP, preco DECIMAL(18,2),"
            "  tags VARCHAR[], atributos MAP(VARCHAR, VARCHAR),"
            "  medicao STRUCT(duracao BIGINT))"
        ),
    )
    findings = _run_rule(catalog, unsupported_type.RULE, "main.pedidos")
    blockers = [f for f in findings if f.severity.name == "BLOCKER"]
    infos = [f for f in findings if f.severity.name == "INFO"]
    assert not blockers
    assert len(infos) == 1  # timestamp ms note


# --------------------------------------------------------------------------
# calendar-transform
# --------------------------------------------------------------------------
def test_calendar_transform_triggers(catalog: CatalogHarness) -> None:
    catalog.write(
        "CREATE SCHEMA IF NOT EXISTS lake.main",
        "CREATE TABLE lake.main.pedidos (id INTEGER, dt DATE)",
    )
    try:
        catalog.write("ALTER TABLE lake.main.pedidos SET PARTITIONED BY (year(dt))")
    except Exception as exc:
        pytest.skip(f"year() partition syntax unavailable: {exc}")
    transform_rows = _meta_rows(
        catalog,
        "SELECT lower(transform) AS t FROM lake_meta.ducklake_partition_column",
    )
    if not any(r["t"] == "year" for r in transform_rows):
        pytest.skip(f"observed transform: {[r['t'] for r in transform_rows]}")

    findings = _run_rule(catalog, calendar_transform.RULE, "main.pedidos")
    degraded = [f for f in findings if f.severity.name == "DEGRADED"]
    assert degraded, "year() in the current spec should degrade"


def test_calendar_transform_identity_passes(catalog: CatalogHarness) -> None:
    catalog.write(
        "CREATE SCHEMA IF NOT EXISTS lake.main",
        "CREATE TABLE lake.main.pedidos (id INTEGER, uf VARCHAR, dt DATE)",
    )
    try:
        catalog.write("ALTER TABLE lake.main.pedidos SET PARTITIONED BY (uf)")
    except Exception as exc:
        pytest.skip(f"identity partition syntax unavailable: {exc}")

    findings = _run_rule(catalog, calendar_transform.RULE, "main.pedidos")
    assert findings == []


# --------------------------------------------------------------------------
# mapping-divergence / encryption-key: negative path (known syntax)
# --------------------------------------------------------------------------
def test_mapping_divergence_clean_passes(catalog: CatalogHarness) -> None:
    from ducklake_iceberg.doctor.rules import mapping_divergence

    catalog.write(
        "CREATE SCHEMA IF NOT EXISTS lake.main",
        "CREATE TABLE lake.main.pedidos (id INTEGER, uf VARCHAR)",
        "INSERT INTO lake.main.pedidos VALUES (1, 'SP')",
        "INSERT INTO lake.main.pedidos VALUES (2, 'MG')",
        (
            "CALL ducklake_flush_inlined_data('lake', schema_name => 'main', "
            "table_name => 'pedidos')"
        ),
    )
    findings = _run_rule(catalog, mapping_divergence.RULE, "main.pedidos")
    assert findings == []


def test_encryption_key_clean_passes(catalog: CatalogHarness) -> None:
    from ducklake_iceberg.doctor.rules import encryption_key

    catalog.write(
        "CREATE SCHEMA IF NOT EXISTS lake.main",
        "CREATE TABLE lake.main.pedidos (id INTEGER, uf VARCHAR)",
        "INSERT INTO lake.main.pedidos VALUES (1, 'SP')",
        (
            "CALL ducklake_flush_inlined_data('lake', schema_name => 'main', "
            "table_name => 'pedidos')"
        ),
    )
    findings = _run_rule(catalog, encryption_key.RULE, "main.pedidos")
    assert findings == []
