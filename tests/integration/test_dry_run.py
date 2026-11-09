"""Tests for the checkpoint `--dry-run`.

They cover the no-write guarantees (SQL spy and connection factory spy), the
compaction estimate and the exit codes. They run against SQLite and Postgres.
"""

from __future__ import annotations

import json
import re
from typing import Any

import duckdb
import pytest
from typer.testing import CliRunner

from ducklake_iceberg.catalog.connection import open_catalog
from ducklake_iceberg.checkpoint.command import run_checkpoint
from ducklake_iceberg.checkpoint.dry_run import DryRunReport
from ducklake_iceberg.cli import app
from ducklake_iceberg.errors import ExitCode
from tests.integration.conftest import CatalogHarness, cli_env_for

pytestmark = [pytest.mark.integration]

runner = CliRunner()

#: Write regex: no dry-run statement may match it.
_WRITE_SQL = re.compile(
    r"(?i)\b(call\s+ducklake_(flush_inlined_data|rewrite_data_files|"
    r"merge_adjacent_files)|insert|update|delete|create|drop|alter)\b"
)


def _max_snapshot(h: CatalogHarness) -> int:
    with open_catalog(h.settings(), read_only=True) as con:
        return int(
            con.query_arrow("SELECT max(snapshot_id) FROM lake_meta.ducklake_snapshot")
            .column(0)[0]
            .as_py()
        )


def _two_small_files(h: CatalogHarness, table: str) -> None:
    target = f"lake.main.{table}"
    h.write(f"CREATE TABLE {target} (id INTEGER)")
    h.write(f"INSERT INTO {target} SELECT range FROM range(0, 50)")
    h.write(f"INSERT INTO {target} SELECT range FROM range(50, 100)")


# --- inlined rows, no writes, snapshot unchanged -----------------------------
def test_dry_run_reports_pending_without_writing(
    seeded_catalog: CatalogHarness,
) -> None:
    before = _max_snapshot(seeded_catalog)
    report = run_checkpoint(
        seeded_catalog.settings(), table="main.pedidos", dry_run=True
    )
    assert isinstance(report, DryRunReport)
    assert report.wrote is False
    assert report.exit_code == ExitCode.SUCCESS
    assert report.tables[0].rows_pending == 2
    assert report.tables[0].would_flush is True
    assert _max_snapshot(seeded_catalog) == before  # no snapshot created


# --- SQL spy: no write statement ---------------------------------------------
def test_dry_run_executes_no_write_sql(
    seeded_catalog: CatalogHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    executed: list[str] = []
    original = duckdb.DuckDBPyConnection.execute

    def spy(self: Any, sql: str, *a: Any, **k: Any) -> Any:
        executed.append(sql)
        return original(self, sql, *a, **k)

    monkeypatch.setattr(duckdb.DuckDBPyConnection, "execute", spy)
    run_checkpoint(seeded_catalog.settings(), table="main.pedidos", dry_run=True)
    offending = [s for s in executed if _WRITE_SQL.search(s)]
    assert offending == [], offending


# --- factory spy: no read_only=False connection ------------------------------
def test_dry_run_never_opens_write_connection(
    seeded_catalog: CatalogHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ducklake_iceberg.catalog.connection import open_catalog as real

    modes: list[bool] = []

    def spy(cfg: Any, *, read_only: bool = True, s3: bool = True) -> Any:
        modes.append(read_only)
        return real(cfg, read_only=read_only, s3=s3)

    # the READ_ONLY phase opens via `metadata_catalog`; the write phase directly
    monkeypatch.setattr("ducklake_iceberg.catalog.connection.open_catalog", spy)
    monkeypatch.setattr("ducklake_iceberg.checkpoint.command.open_catalog", spy)
    run_checkpoint(seeded_catalog.settings(), table="main.pedidos", dry_run=True)
    assert modes == [True]  # a single connection, and READ_ONLY


# --- table without inlined data -> exit 4, but listed in the report ----------
def test_dry_run_no_pending_lists_table(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.empty_tbl (id INTEGER)")
    catalog.write(
        "CALL ducklake_flush_inlined_data('lake', schema_name => 'main', "
        "table_name => 'empty_tbl')"
    )
    report = run_checkpoint(catalog.settings(), table="main.empty_tbl", dry_run=True)
    assert report.exit_code == ExitCode.NOTHING_TO_DO
    assert len(report.tables) == 1
    assert report.tables[0].rows_pending == 0


# --- --all-tables, 2 of 3 with pending rows ----------------------------------
def test_dry_run_all_tables(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.a (id INTEGER)")
    catalog.write("INSERT INTO lake.main.a VALUES (1)")
    catalog.write("CREATE TABLE lake.main.b (id INTEGER)")
    catalog.write("INSERT INTO lake.main.b VALUES (2)")
    catalog.write("CREATE TABLE lake.main.c (id INTEGER)")
    report = run_checkpoint(catalog.settings(), all_tables=True, dry_run=True)
    assert report.exit_code == ExitCode.SUCCESS
    assert len(report.tables) == 3
    assert report.tables_with_pending == 2


# --- dry-run after a checkpoint -> exit 4 ------------------------------------
def test_dry_run_after_checkpoint_is_nothing_to_do(
    seeded_catalog: CatalogHarness,
) -> None:
    run_checkpoint(seeded_catalog.settings(), table="main.pedidos")
    report = run_checkpoint(
        seeded_catalog.settings(), table="main.pedidos", dry_run=True
    )
    assert report.exit_code == ExitCode.NOTHING_TO_DO


# --- two schema versions add up rows_pending ---------------------------------
def test_dry_run_sums_schema_versions(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1)")
    catalog.write("ALTER TABLE lake.main.t ADD COLUMN uf VARCHAR")
    catalog.write("INSERT INTO lake.main.t VALUES (2, 'SP')")
    report = run_checkpoint(catalog.settings(), table="main.t", dry_run=True)
    assert report.tables[0].rows_pending == 2
    assert len(report.tables[0].sources) >= 1


# --- estimated compaction (2 small files, no inlined data) -------------------
def test_dry_run_estimates_merge(catalog: CatalogHarness) -> None:
    _two_small_files(catalog, "compact")
    before = _max_snapshot(catalog)
    report = run_checkpoint(catalog.settings(), table="main.compact", dry_run=True)
    row = report.tables[0]
    assert row.rows_pending == 0
    assert row.merge_candidates == 2
    assert row.would_compact is True
    assert report.exit_code == ExitCode.SUCCESS
    assert _max_snapshot(catalog) == before


# --- rewrite estimated from a deleted fraction above the threshold -----------
def test_dry_run_estimates_rewrite(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.d (id INTEGER)")
    catalog.write("INSERT INTO lake.main.d SELECT range FROM range(0, 100)")
    catalog.write("DELETE FROM lake.main.d WHERE id < 98")  # 98% > threshold 0.95
    report = run_checkpoint(catalog.settings(), table="main.d", dry_run=True)
    row = report.tables[0]
    assert row.rewrite_candidates >= 1
    assert row.would_compact is True


# --- auto_compact=false zeroes the estimate ----------------------------------
def test_dry_run_auto_compact_off(catalog: CatalogHarness) -> None:
    _two_small_files(catalog, "off")
    catalog.write(
        "CALL lake.set_option('auto_compact', false, "
        "table_name => 'off', schema => 'main')"
    )
    report = run_checkpoint(catalog.settings(), table="main.off", dry_run=True)
    row = report.tables[0]
    assert row.auto_compact is False
    assert row.would_compact is False
    assert report.exit_code == ExitCode.NOTHING_TO_DO
    assert any("auto_compact is off" in w for w in report.warnings)


# --- nested VARIANT in dry-run -> exit 2 -------------------------------------
def test_dry_run_blocks_nested_variant(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, p STRUCT(a VARIANT))")
    catalog.write("INSERT INTO lake.main.t VALUES (1, {'a': NULL})")
    report = run_checkpoint(catalog.settings(), table="main.t", dry_run=True)
    row = report.tables[0]
    assert row.blocked_by == ("E-VARIANT-NESTED",)
    assert row.would_flush is False
    assert report.exit_code == ExitCode.BLOCKED


# --- --json envelope with dry_run:true and wrote:false -----------------------
def test_dry_run_json_envelope(seeded_catalog: CatalogHarness) -> None:
    result = runner.invoke(
        app,
        ["checkpoint", "--table", "main.pedidos", "--dry-run", "--json"],
        env=cli_env_for(seeded_catalog),
    )
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)["results"][0]
    assert payload["command"] == "checkpoint"
    assert payload["dry_run"] is True
    assert payload["wrote"] is False
    assert payload["snapshot_after"] is None
    row = payload["tables"][0]
    assert "rows_pending" in row
    assert "rows_flushed" not in row  # field exclusive to the real mode


# --- --dry-run --yes: --yes is ignored, nothing is written -------------------
def test_dry_run_with_yes_ignored(seeded_catalog: CatalogHarness) -> None:
    before = _max_snapshot(seeded_catalog)
    result = runner.invoke(
        app,
        ["checkpoint", "--table", "main.pedidos", "--dry-run", "--yes"],
        env={**cli_env_for(seeded_catalog), "COLUMNS": "200"},
    )
    assert result.exit_code == 0
    assert "Nothing was written" in result.stdout
    assert _max_snapshot(seeded_catalog) == before
