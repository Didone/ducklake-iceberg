"""Integration tests for `detect_inlined`.

The SAME suite runs against SQLite and Postgres via the fixtures in
`tests/integration/conftest.py`. Detection decides by `rows_visible`, not by
the presence of a record nor by `rows_total`.
"""

from __future__ import annotations

import pytest

from ducklake_iceberg import timing
from ducklake_iceberg.catalog.connection import open_catalog
from ducklake_iceberg.catalog.reader import (
    current_snapshot,
    parse_table_ref,
    resolve_table,
)
from ducklake_iceberg.checkpoint.detect import (
    cache_inlined_reads,
    detect_inlined,
    detect_inlined_many,
)
from ducklake_iceberg.checkpoint.model import TableRef
from tests.integration.conftest import CatalogHarness

pytestmark = pytest.mark.integration


def _status(harness: CatalogHarness, table: str) -> object:
    settings = harness.settings()
    with open_catalog(settings, read_only=True) as con, con.read_transaction():
        snap = current_snapshot(con)
        ident = resolve_table(con, parse_table_ref(table), snap)
        ref = TableRef(ident.ref.schema_name, ident.ref.table_name, ident.table_id)
        return detect_inlined(con.raw, con.meta, ref, snap.snapshot_id)


# --- inserts below the limit -> pending --------------------------------------
def test_detect_pending(seeded_catalog: CatalogHarness) -> None:
    st = _status(seeded_catalog, "main.pedidos")
    assert st.pending_rows == 2  # type: ignore[attr-defined]
    assert st.has_pending  # type: ignore[attr-defined]
    assert len(st.counts) == 1  # type: ignore[attr-defined]
    source = st.counts[0].source  # type: ignore[attr-defined]
    assert source.registered is True
    assert source.exists is True
    assert source.kind == "data"
    assert st.counts[0].visibility_filtered is True  # type: ignore[attr-defined]


# --- after flush -> has_pending is false --------------------------------------
def test_detect_after_flush(seeded_catalog: CatalogHarness) -> None:
    seeded_catalog.write(
        "CALL ducklake_flush_inlined_data('lake', schema_name => 'main', "
        "table_name => 'pedidos')"
    )
    st = _status(seeded_catalog, "main.pedidos")
    assert st.pending_rows == 0  # type: ignore[attr-defined]
    assert not st.has_pending  # type: ignore[attr-defined]


# --- table without inlining -> no spurious warnings --------------------------
def test_detect_never_inlined(catalog: CatalogHarness) -> None:
    catalog.write(
        "CREATE TABLE lake.vendas.empty_tbl (id INTEGER)",
        (
            "CALL ducklake_flush_inlined_data('lake', schema_name => 'vendas', "
            "table_name => 'empty_tbl')"
        ),
    )
    st = _status(catalog, "vendas.empty_tbl")
    assert st.pending_rows == 0  # type: ignore[attr-defined]
    # there may be a residual record, but there must be no spurious warning
    assert all("outside the inlining pattern" not in w for w in st.warnings)  # type: ignore[attr-defined]


# --- inserted and deleted before the flush -> rows_visible 0 -----------------
def test_detect_inserted_then_deleted(catalog: CatalogHarness) -> None:
    catalog.write(
        "CREATE TABLE lake.vendas.t (id INTEGER)",
        "INSERT INTO lake.vendas.t VALUES (1)",
        "DELETE FROM lake.vendas.t WHERE id = 1",
    )
    st = _status(catalog, "vendas.t")
    # the inserted row was deleted in the same inlined state: not visible
    assert st.pending_rows == 0  # type: ignore[attr-defined]


# --- introspection does not count the 29 DuckLake spec tables ----------------
def test_introspection_excludes_spec_tables(seeded_catalog: CatalogHarness) -> None:
    # seeded_catalog has 1 dynamic physical table (inlined pedidos) + the 29.
    st = _status(seeded_catalog, "main.pedidos")
    # every source matches the inlining pattern; none is a spec table
    for count in st.counts:  # type: ignore[attr-defined]
        assert count.source.physical_name.startswith("ducklake_inlined_")


# --- detect_inlined_many is keyed by table_id --------------------------------
def test_detect_many(catalog: CatalogHarness) -> None:
    catalog.write(
        "CREATE TABLE lake.main.a (id INTEGER)",
        "INSERT INTO lake.main.a VALUES (1), (2)",
        "CREATE TABLE lake.main.b (id INTEGER)",
    )

    settings = catalog.settings()
    with open_catalog(settings, read_only=True) as con, con.read_transaction():
        snap = current_snapshot(con)
        refs = []
        for name in ("main.a", "main.b"):
            ident = resolve_table(con, parse_table_ref(name), snap)
            refs.append(
                TableRef(ident.ref.schema_name, ident.ref.table_name, ident.table_id)
            )
        statuses = detect_inlined_many(con.raw, con.meta, refs, snap.snapshot_id)

    assert len(statuses) == 2
    by_pending = {tid: st.pending_rows for tid, st in statuses.items()}
    assert sorted(by_pending.values()) == [0, 2]


def test_detection_cache_is_scoped_to_snapshot_and_read(
    seeded_catalog: CatalogHarness,
) -> None:
    with open_catalog(seeded_catalog.settings()) as con:
        with con.read_transaction(), cache_inlined_reads(con.raw):
            snap = current_snapshot(con)
            identity = resolve_table(con, parse_table_ref("main.pedidos"), snap)
            table = TableRef("main", "pedidos", identity.table_id)
            first = detect_inlined(con.raw, con.meta, table, snap.snapshot_id)
            queries = timing.counter("catalog.queries")
            assert detect_inlined(con.raw, con.meta, table, snap.snapshot_id) == first
            assert timing.counter("catalog.queries") == queries
            before = first.earliest_pending_begin
            assert before is not None
            previous = detect_inlined(con.raw, con.meta, table, before - 1)
            assert not previous.has_pending
            assert timing.counter("catalog.queries") > queries

        with con.read_transaction(), cache_inlined_reads(con.raw):
            queries = timing.counter("catalog.queries")
            assert detect_inlined(con.raw, con.meta, table, snap.snapshot_id) == first
            assert timing.counter("catalog.queries") > queries


def test_detection_cache_does_not_hide_later_inlined_rows(
    seeded_catalog: CatalogHarness,
) -> None:
    with open_catalog(seeded_catalog.settings()) as con:
        with con.read_transaction(), cache_inlined_reads(con.raw):
            snap = current_snapshot(con)
            identity = resolve_table(con, parse_table_ref("main.pedidos"), snap)
            table = TableRef("main", "pedidos", identity.table_id)
            first = detect_inlined(con.raw, con.meta, table, snap.snapshot_id)

        seeded_catalog.write("INSERT INTO lake.main.pedidos VALUES (3, 'RJ')")

        with con.read_transaction(), cache_inlined_reads(con.raw):
            snapshot = current_snapshot(con)
            current = detect_inlined(con.raw, con.meta, table, snapshot.snapshot_id)
            assert current.pending_data_rows == first.pending_data_rows + 1
