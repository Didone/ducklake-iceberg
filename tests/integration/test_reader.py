"""Table identity resolution tests.

The SAME suite runs against SQLite and Postgres via the `catalog`/
`seeded_catalog` fixtures.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from ducklake_iceberg.catalog import reader as rdr
from ducklake_iceberg.catalog.connection import open_catalog
from ducklake_iceberg.errors import CatalogError, IdentityError
from tests.integration.conftest import CatalogFactory, CatalogHarness

pytestmark = pytest.mark.integration


# --- current snapshot -----------------------------------------------------
def test_current_snapshot(seeded_catalog: CatalogHarness) -> None:
    with open_catalog(seeded_catalog.settings()) as con, con.read_transaction():
        snap = rdr.current_snapshot(con)
        assert snap.snapshot_id >= 1
        assert isinstance(snap.snapshot_time, datetime)
        # snapshot_time is a timezone-aware datetime on both SQLite AND Postgres
        assert snap.snapshot_time.tzinfo is not None


def test_current_snapshot_empty_catalog(catalog: CatalogHarness) -> None:
    catalog.meta_exec("DELETE FROM ducklake_snapshot")
    with (
        open_catalog(catalog.settings()) as con,
        con.read_transaction(),
        pytest.raises(IdentityError, match="no snapshots"),
    ):
        rdr.current_snapshot(con)


# --- table resolution -----------------------------------------------------
def test_resolve_table(seeded_catalog: CatalogHarness) -> None:
    with open_catalog(seeded_catalog.settings()) as con, con.read_transaction():
        snap = rdr.current_snapshot(con)
        ident = rdr.resolve_table(con, rdr.TableRef("main", "pedidos"), snap)
        assert ident.table_id >= 1
        assert ident.schema_id >= 0
        assert ident.table_uuid is not None
        assert ident.table_path is not None
        assert ident.schema_path is not None
        assert ident.schema_version == rdr.table_schema_version(
            con, ident.table_id, snap
        )


# --- table created after the queried snapshot -----------------------------
def test_table_not_visible_in_earlier_snapshot(make_catalog: CatalogFactory) -> None:
    catalog = make_catalog(
        "CREATE TABLE lake.vendas.a (id INTEGER)",
        "CREATE TABLE lake.vendas.b (id INTEGER)",
    )
    with open_catalog(catalog.settings()) as con, con.read_transaction():
        current = rdr.current_snapshot(con)
        begin_a = int(
            con.query_arrow(
                "SELECT begin_snapshot FROM lake_meta.ducklake_table "
                "WHERE table_name = 'a'"
            ).to_pylist()[0]["begin_snapshot"]
        )
        earlier = rdr.snapshot_at(con, begin_a)
        with pytest.raises(IdentityError):
            rdr.resolve_table(con, rdr.TableRef("vendas", "b"), earlier)
        rdr.resolve_table(con, rdr.TableRef("vendas", "b"), current)


# --- half-open right bound of the interval --------------------------------
def test_dropped_table_right_bound(make_catalog: CatalogFactory) -> None:
    catalog = make_catalog(
        "CREATE TABLE lake.vendas.b (id INTEGER)",
        "DROP TABLE lake.vendas.b",
    )
    with open_catalog(catalog.settings()) as con, con.read_transaction():
        rows = con.query_arrow(
            "SELECT begin_snapshot, end_snapshot "
            "FROM lake_meta.ducklake_table WHERE table_name = 'b'"
        ).to_pylist()
        begin = int(rows[0]["begin_snapshot"])
        end = int(rows[0]["end_snapshot"])

        snap_live = rdr.snapshot_at(con, begin)
        ident = rdr.resolve_table(con, rdr.TableRef("vendas", "b"), snap_live)
        assert ident.ref.table_name == "b"

        snap_end = rdr.snapshot_at(con, end)
        with pytest.raises(IdentityError):
            rdr.resolve_table(con, rdr.TableRef("vendas", "b"), snap_end)


# --- ambiguous identity ---------------------------------------------------
def test_ambiguous_identity(make_catalog: CatalogFactory) -> None:
    catalog = make_catalog("CREATE TABLE lake.vendas.dup (id INTEGER)")
    catalog.meta_exec(
        "INSERT INTO ducklake_table "
        "(table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, "
        " table_name, path, path_is_relative) "
        "SELECT table_id + 1000, table_uuid, begin_snapshot, end_snapshot, "
        " schema_id, table_name, path, path_is_relative "
        "FROM ducklake_table WHERE table_name = 'dup'",
    )
    with open_catalog(catalog.settings()) as con, con.read_transaction():
        snap = rdr.current_snapshot(con)
        with pytest.raises(CatalogError, match="ambiguous"):
            rdr.resolve_table(con, rdr.TableRef("vendas", "dup"), snap)


# --- same-named tables in different schemas -------------------------------
def test_homonymous_tables(make_catalog: CatalogFactory) -> None:
    catalog = make_catalog(
        "CREATE TABLE lake.vendas.pedidos (id INTEGER)",
        "CREATE TABLE lake.main.pedidos (id INTEGER)",
    )
    with open_catalog(catalog.settings()) as con, con.read_transaction():
        snap = rdr.current_snapshot(con)
        v = rdr.resolve_table(con, rdr.TableRef("vendas", "pedidos"), snap)
        m = rdr.resolve_table(con, rdr.parse_table_ref("pedidos"), snap)
        assert v.table_id != m.table_id
        assert m.ref.schema_name == "main"


# --- listing --------------------------------------------------------------
def test_list_tables(seeded_catalog: CatalogHarness) -> None:
    with open_catalog(seeded_catalog.settings()) as con, con.read_transaction():
        snap = rdr.current_snapshot(con)
        tables = rdr.list_tables(con, snap)
        assert rdr.TableRef("main", "pedidos") in tables
        assert list(tables) == sorted(
            tables, key=lambda t: (t.schema_name, t.table_name)
        )


def test_list_tables_filtered(make_catalog: CatalogFactory) -> None:
    catalog = make_catalog(
        "CREATE TABLE lake.vendas.a (id INTEGER)",
        "CREATE TABLE lake.main.b (id INTEGER)",
    )
    with open_catalog(catalog.settings()) as con, con.read_transaction():
        snap = rdr.current_snapshot(con)
        vendas = rdr.list_tables(con, snap, schema_name="vendas")
        assert vendas == (rdr.TableRef("vendas", "a"),)


# --- inlined data before and after flush ----------------------------------
def test_inlined_rows_before_flush(seeded_catalog: CatalogHarness) -> None:
    with open_catalog(seeded_catalog.settings()) as con, con.read_transaction():
        snap = rdr.current_snapshot(con)
        ident = rdr.resolve_table(con, rdr.TableRef("main", "pedidos"), snap)
        inlined = rdr.inlined_data_table(con, ident)
        assert inlined is not None
        assert inlined.row_count == 2
        assert inlined.physical_name == (
            f"ducklake_inlined_data_{ident.table_id}_{inlined.schema_version}"
        )


def test_inlined_rows_after_flush(make_catalog: CatalogFactory) -> None:
    catalog = make_catalog(
        "CREATE TABLE lake.vendas.pedidos (id INTEGER)",
        "INSERT INTO lake.vendas.pedidos VALUES (1), (2), (3)",
        (
            "CALL ducklake_flush_inlined_data('lake', schema_name => 'vendas', "
            "table_name => 'pedidos')"
        ),
    )
    with open_catalog(catalog.settings()) as con, con.read_transaction():
        snap = rdr.current_snapshot(con)
        ident = rdr.resolve_table(con, rdr.TableRef("vendas", "pedidos"), snap)
        inlined = rdr.inlined_data_table(con, ident)
        assert inlined is not None  # the record persists
        assert inlined.row_count == 0  # but there are no pending rows


def test_inlined_none_when_never(make_catalog: CatalogFactory) -> None:
    catalog = make_catalog(
        "CREATE TABLE lake.vendas.vazia (id INTEGER)",
        (
            "CALL ducklake_flush_inlined_data('lake', schema_name => 'vendas', "
            "table_name => 'vazia')"
        ),
    )
    with open_catalog(catalog.settings()) as con, con.read_transaction():
        snap = rdr.current_snapshot(con)
        ident = rdr.resolve_table(con, rdr.TableRef("vendas", "vazia"), snap)
        result = rdr.inlined_data_table(con, ident)
        assert result is None or result.row_count == 0


# --- catalog data_path ----------------------------------------------------
def test_catalog_data_path(seeded_catalog: CatalogHarness) -> None:
    with open_catalog(seeded_catalog.settings()) as con, con.read_transaction():
        path = rdr.catalog_data_path(con)
        assert path.endswith("/")
        assert "data" in path


def test_catalog_data_path_missing(catalog: CatalogHarness) -> None:
    catalog.meta_exec("DELETE FROM ducklake_metadata WHERE key = 'data_path'")
    with (
        open_catalog(catalog.settings()) as con,
        con.read_transaction(),
        pytest.raises(CatalogError, match="no global data_path"),
    ):
        rdr.catalog_data_path(con)


# Note: the duplicate data_path branch (CatalogError) is defensive and is not
# reachable through a live connection: DuckLake itself refuses the ATTACH when
# ducklake_metadata has a divergent data_path.


# --- table_schema_version: fallback to the snapshot schema_version --------
def test_schema_version_fallback(make_catalog: CatalogFactory) -> None:
    catalog = make_catalog("CREATE TABLE lake.vendas.t (id INTEGER)")
    catalog.meta_exec("DELETE FROM ducklake_schema_versions")
    with open_catalog(catalog.settings()) as con, con.read_transaction():
        snap = rdr.current_snapshot(con)
        ident = rdr.resolve_table(con, rdr.TableRef("vendas", "t"), snap)
        assert ident.schema_version == snap.schema_version


# --- inlined: unexpected naming convention --------------------------------
def test_inlined_convention_mismatch(make_catalog: CatalogFactory) -> None:
    catalog = make_catalog(
        "CREATE TABLE lake.vendas.pedidos (id INTEGER)",
        "INSERT INTO lake.vendas.pedidos VALUES (1)",
    )
    catalog.meta_exec(
        "UPDATE ducklake_inlined_data_tables SET table_name = 'nome_fora_da_convencao'",
    )
    with open_catalog(catalog.settings()) as con, con.read_transaction():
        snap = rdr.current_snapshot(con)
        ident = rdr.resolve_table(con, rdr.TableRef("vendas", "pedidos"), snap)
        with pytest.raises(CatalogError, match="inlined data naming convention"):
            rdr.inlined_data_table(con, ident)


# --- snapshot_at: nonexistent snapshot ------------------------------------
def test_snapshot_at_missing(seeded_catalog: CatalogHarness) -> None:
    with (
        open_catalog(seeded_catalog.settings()) as con,
        con.read_transaction(),
        pytest.raises(IdentityError, match="does not exist or has expired"),
    ):
        rdr.snapshot_at(con, 999)


def test_snapshot_at_existing(seeded_catalog: CatalogHarness) -> None:
    with open_catalog(seeded_catalog.settings()) as con, con.read_transaction():
        current = rdr.current_snapshot(con)
        same = rdr.snapshot_at(con, current.snapshot_id)
        assert same.snapshot_id == current.snapshot_id
