"""Integration tests for the flush of inlined data (`flush_inlined`).

The SAME suite runs against SQLite and Postgres via the fixtures in
`tests/integration/conftest.py`.
"""

from __future__ import annotations

from typing import Any

import duckdb
import pytest

from ducklake_iceberg import timing
from ducklake_iceberg.catalog.connection import DuckLakeConnection, open_catalog
from ducklake_iceberg.checkpoint import flush
from ducklake_iceberg.checkpoint.flush import flush_inlined, read_fingerprint
from ducklake_iceberg.checkpoint.model import CheckpointError
from ducklake_iceberg.errors import ConfigError, DivergenceError, ExitCode
from tests.integration.conftest import CatalogHarness

pytestmark = [pytest.mark.integration]


def _scalar(harness: CatalogHarness, sql: str, params: Any = None) -> Any:
    """Read a value on a fresh READ_ONLY connection (catalog seen from outside)."""
    with open_catalog(harness.settings(), read_only=True) as con:
        return con.query_arrow(sql, params).column(0)[0].as_py()


def _max_snapshot(harness: CatalogHarness) -> int:
    return int(
        _scalar(harness, "SELECT max(snapshot_id) FROM lake_meta.ducklake_snapshot")
    )


def _live_files(harness: CatalogHarness, table: str) -> int:
    return int(
        _scalar(
            harness,
            "SELECT count(*) FROM lake_meta.ducklake_data_file f "
            "JOIN lake_meta.ducklake_table t USING (table_id) "
            "WHERE f.end_snapshot IS NULL AND t.end_snapshot IS NULL "
            "AND t.table_name = $table",
            {"table": table},
        )
    )


# --- inlined rows -> flush ---------------------------------------------------
def test_flush_inlined_rows(seeded_catalog: CatalogHarness) -> None:
    before = _max_snapshot(seeded_catalog)
    result = flush_inlined(seeded_catalog.settings(), "main.pedidos")
    assert result.rows_flushed == 2
    assert result.wrote is True
    assert result.exit_code is ExitCode.SUCCESS
    assert result.snapshot_before == before
    assert result.snapshot_after > before
    assert _max_snapshot(seeded_catalog) == result.snapshot_after
    assert _scalar(seeded_catalog, "SELECT count(*) FROM lake.main.pedidos") == 2


# --- rerun -> nothing to do, no new snapshot ---------------------------------
def test_flush_idempotent(seeded_catalog: CatalogHarness) -> None:
    settings = seeded_catalog.settings()
    first = flush_inlined(settings, "main.pedidos")
    assert first.wrote
    after_first = _max_snapshot(seeded_catalog)

    second = flush_inlined(settings, "main.pedidos")
    assert second.rows_flushed == 0
    assert second.wrote is False
    assert second.exit_code is ExitCode.NOTHING_TO_DO
    assert second.snapshot_after == second.snapshot_before == after_first
    assert _max_snapshot(seeded_catalog) == after_first


# --- the flush never compacts ------------------------------------------------
def test_flush_does_not_merge_small_files(catalog: CatalogHarness) -> None:
    catalog.write(
        "CREATE TABLE lake.main.files (id INTEGER)",
        "INSERT INTO lake.main.files SELECT range FROM range(0, 50)",
        "INSERT INTO lake.main.files SELECT range FROM range(50, 100)",
    )
    live_before = _live_files(catalog, "files")
    snapshot_before = _max_snapshot(catalog)
    result = flush_inlined(catalog.settings(), "main.files")
    assert result.exit_code is ExitCode.NOTHING_TO_DO
    assert _live_files(catalog, "files") == live_before
    assert _max_snapshot(catalog) == snapshot_before


# --- table resolution errors -------------------------------------------------
def test_flush_unqualified_table(seeded_catalog: CatalogHarness) -> None:
    with pytest.raises(ConfigError, match="qualified name"):
        flush_inlined(seeded_catalog.settings(), "pedidos")


def test_flush_missing_table(seeded_catalog: CatalogHarness) -> None:
    with pytest.raises(ConfigError, match="does not exist"):
        flush_inlined(seeded_catalog.settings(), "main.missing")


# --- diverging fingerprint -> exit 3, nothing written ------------------------
def test_flush_fingerprint_divergence(
    seeded_catalog: CatalogHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = flush.read_fingerprint
    calls: list[bool] = []

    def forged(con: DuckLakeConnection) -> Any:
        fp = real(con)
        calls.append(con.read_only)
        if not con.read_only:
            return type(fp)("/other/", fp.format_version, fp.max_snapshot_id, 0)
        return fp

    executed: list[DuckLakeConnection] = []
    real_call = flush._flush_call

    def spy(con: DuckLakeConnection, table: Any) -> int:
        executed.append(con)
        return real_call(con, table)

    monkeypatch.setattr(flush, "read_fingerprint", forged)
    monkeypatch.setattr(flush, "_flush_call", spy)
    before = _max_snapshot(seeded_catalog)
    with pytest.raises(DivergenceError, match="nothing was written") as excinfo:
        flush_inlined(seeded_catalog.settings(), "main.pedidos")
    assert excinfo.value.exit_code is ExitCode.DIVERGED
    assert calls == [True, False]
    assert executed == []
    assert _max_snapshot(seeded_catalog) == before


# --- DuckDB failure is translated and the raw text preserved -----------------
def test_flush_error_is_translated(
    seeded_catalog: CatalogHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    def failing(con: DuckLakeConnection, table: Any) -> int:
        raise duckdb.InvalidInputException("simulated")

    monkeypatch.setattr(flush, "_flush_call", failing)
    before = _max_snapshot(seeded_catalog)
    with pytest.raises(CheckpointError) as excinfo:
        flush_inlined(seeded_catalog.settings(), "main.pedidos")
    assert excinfo.value.exit_code is ExitCode.ERROR
    assert excinfo.value.phase == "flush"
    assert excinfo.value.raw == "simulated"
    assert _max_snapshot(seeded_catalog) == before


def test_read_fingerprint(seeded_catalog: CatalogHarness) -> None:
    with (
        open_catalog(seeded_catalog.settings(), read_only=True) as con,
        con.read_transaction(),
    ):
        queries = timing.counter("catalog.queries")
        fp = read_fingerprint(con)
        assert timing.counter("catalog.queries") == queries + 1
    assert fp.data_path is not None
    assert fp.format_version is not None
    assert fp.max_snapshot_id is not None
    assert fp.table_count >= 1
