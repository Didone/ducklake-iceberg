"""Transaction cache isolation and source statistics at the enumerated snapshot."""

from __future__ import annotations

import pytest

from ducklake_iceberg import timing
from ducklake_iceberg.catalog.connection import open_catalog
from ducklake_iceberg.catalog.query import Params, query_arrow
from ducklake_iceberg.catalog.view import read_catalog
from ducklake_iceberg.errors import CatalogError
from tests.integration.conftest import CatalogFactory, CatalogHarness

pytestmark = pytest.mark.integration


def test_read_cache_bound_to_transaction_and_parameters(
    catalog: CatalogHarness,
) -> None:
    with open_catalog(catalog.settings(), s3=False) as con:
        with con.read_transaction():
            before = timing.counter("catalog.queries")
            assert con.query_arrow("SELECT $value AS v", {"value": 1}).to_pylist() == [
                {"v": 1}
            ]
            con.query_arrow("SELECT $value AS v", {"value": 1})
            assert con.query_arrow("SELECT $value AS v", {"value": 2}).to_pylist() == [
                {"v": 2}
            ]
            assert timing.counter("catalog.queries") == before + 2
            with con.borrow_read_transaction():
                con.query_arrow("SELECT $value AS v", {"value": 1})
            assert timing.counter("catalog.queries") == before + 2
            with (
                pytest.raises(CatalogError, match="not reentrant"),
                con.read_transaction(),
            ):
                pass
        with con.read_transaction():
            con.query_arrow("SELECT $value AS v", {"value": 1})
        assert timing.counter("catalog.queries") == before + 3


def test_catalog_stats_materialized_before_view_closes(
    make_catalog: CatalogFactory,
) -> None:
    catalog = make_catalog(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.frozen (id BIGINT)",
        "INSERT INTO lake.main.frozen SELECT * FROM range(100)",
    )
    with open_catalog(catalog.settings(), s3=False) as con, con.read_transaction():
        view = read_catalog(con, "main.frozen", None, include_stats=True)
    ids = {f.data_file_id for f in view.files.files}
    assert ids == set(view.file_column_stats)
    assert all(stats for stats in view.file_column_stats.values())


def test_query_cache_does_not_cross_connections(catalog: CatalogHarness) -> None:
    with (
        open_catalog(catalog.settings(), s3=False) as first,
        open_catalog(catalog.settings(), s3=False) as second,
        first.read_transaction(),
    ):
        before = timing.counter("catalog.queries")
        query_arrow(first.raw, "SELECT 1")
        query_arrow(second.raw, "SELECT 1")
        assert timing.counter("catalog.queries") == before + 2


def test_batched_reads_preserve_arrow_types_and_empty_schema(
    catalog: CatalogHarness,
) -> None:
    from ducklake_iceberg.catalog.query import prime_reads

    with open_catalog(catalog.settings(), s3=False) as con, con.read_transaction():
        queries: list[tuple[str, Params]] = [
            ("SELECT 1::DECIMAL(12,2) amount, NULL::BIGINT missing", {}),
            ("SELECT 1::BIGINT id WHERE false", {}),
        ]
        before = timing.counter("catalog.queries")
        prime_reads(con.raw, queries)
        first = query_arrow(con.raw, queries[0][0], {})
        empty = query_arrow(con.raw, queries[1][0], {})
        assert str(first.schema.field("amount").type) == "decimal128(12, 2)"
        assert empty.column_names == ["id"]
        assert empty.num_rows == 0
        assert timing.counter("catalog.queries") == before + 1


def test_read_only_attachment_cannot_flush_pending_rows(
    catalog: CatalogHarness,
) -> None:
    """A single attachment cannot preserve RO preflight and perform the flush."""
    import duckdb

    catalog.write(
        "CALL lake.set_option('data_inlining_row_limit', 1000)",
        "CREATE TABLE lake.main.readonly_flush(id BIGINT)",
        "INSERT INTO lake.main.readonly_flush VALUES (1)",
    )
    with open_catalog(catalog.settings(), s3=False) as con:
        before = con.query_arrow(
            "SELECT max(snapshot_id) n FROM lake_meta.ducklake_snapshot"
        ).to_pylist()
        with pytest.raises(duckdb.Error) as caught:
            con.raw.execute(
                "CALL ducklake_flush_inlined_data('lake', schema_name => 'main', "
                "table_name => 'readonly_flush')"
            ).fetchall()
        message = str(caught.value).lower()
        assert "read-only" in message or "read only" in message or "readonly" in message
        after = con.query_arrow(
            "SELECT max(snapshot_id) n FROM lake_meta.ducklake_snapshot"
        ).to_pylist()
        assert before == after
