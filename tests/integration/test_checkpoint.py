"""Integration tests for the `ducklake-iceberg checkpoint` command.

The SAME suite runs against SQLite and Postgres via the fixtures in
`tests/integration/conftest.py`. The checkpoint runs, in this order,
flush (transaction 1) and expire + merge + rewrite (transaction 2).
"""

from __future__ import annotations

import json
import time
from typing import Any

import duckdb
import pytest
from typer.testing import CliRunner

from ducklake_iceberg.catalog.connection import DuckLakeConnection, open_catalog
from ducklake_iceberg.checkpoint import command as cmd
from ducklake_iceberg.checkpoint.command import (
    read_fingerprint,
    resolve_scope,
    run_checkpoint,
)
from ducklake_iceberg.cli import app
from ducklake_iceberg.errors import DivergenceError, ExitCode
from tests.integration.conftest import CatalogHarness, cli_env_for

pytestmark = [pytest.mark.integration]

runner = CliRunner()


def _json(output: str) -> dict[str, Any]:
    payload: dict[str, Any] = json.loads(output)
    return payload


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


def _two_small_files(harness: CatalogHarness, table: str = "files") -> None:
    # `table` is always a literal from the test itself (fixed identifier)
    target = f"lake.main.{table}"
    harness.write(f"CREATE TABLE {target} (id INTEGER)")
    harness.write(f"INSERT INTO {target} SELECT range FROM range(0, 50)")
    harness.write(f"INSERT INTO {target} SELECT range FROM range(50, 100)")


# --- inlined rows -> flush ---------------------------------------------------
def test_checkpoint_flushes_inlined(seeded_catalog: CatalogHarness) -> None:
    before = _max_snapshot(seeded_catalog)
    result = runner.invoke(
        app,
        ["checkpoint", "--table", "main.pedidos", "--json"],
        env=cli_env_for(seeded_catalog),
    )
    assert result.exit_code == 0, result.stdout
    row = _json(result.stdout)["results"][0]["tables"][0]
    assert row["rows_flushed"] == 2
    assert row["changed"] is True
    assert row["snapshot_after"] > before
    assert _max_snapshot(seeded_catalog) == row["snapshot_after"]


# --- rerun -> exit 4, no new snapshot ----------------------------------------
def test_checkpoint_idempotent(seeded_catalog: CatalogHarness) -> None:
    env = cli_env_for(seeded_catalog)
    first = runner.invoke(app, ["checkpoint", "--table", "main.pedidos"], env=env)
    assert first.exit_code == 0, first.stdout
    after_first = _max_snapshot(seeded_catalog)

    second = runner.invoke(
        app, ["checkpoint", "--table", "main.pedidos", "--json"], env=env
    )
    assert second.exit_code == 4, second.stdout
    assert _json(second.stdout)["results"][0]["totals"]["tables_changed"] == 0
    assert _max_snapshot(seeded_catalog) == after_first


# --- small files without inlined data -> merge -------------------------------
def test_checkpoint_merges_small_files(catalog: CatalogHarness) -> None:
    _two_small_files(catalog)
    assert _live_files(catalog, "files") == 2
    report = run_checkpoint(catalog.settings(), table="main.files")
    assert report.exit_code == ExitCode.SUCCESS
    row = report.results[0]
    assert row.rows_flushed == 0
    assert row.merge.files_processed == 2
    assert row.merge.files_created == 1
    assert row.file_size_distribution is not None
    assert row.file_size_distribution.target_file_size_bytes is not None
    assert _live_files(catalog, "files") == 1
    # maintenance does not change data
    assert _scalar(catalog, "SELECT count(*) FROM lake.main.files") == 100


def test_checkpoint_target_file_size_and_distribution(
    catalog: CatalogHarness,
) -> None:
    _two_small_files(catalog, "target")
    option_before = _scalar(
        catalog,
        "SELECT count(*) FROM lake_meta.ducklake_metadata "
        "WHERE key = 'target_file_size'",
    )
    report = run_checkpoint(
        catalog.settings(),
        table="main.target",
        target_file_size_bytes=64_000_000,
    )
    distribution = report.results[0].file_size_distribution
    assert distribution is not None
    assert distribution.target_file_size_bytes == 64_000_000
    assert distribution.file_count == _live_files(catalog, "target")
    assert distribution.median_bytes is not None
    assert distribution.below_half_target is not None
    assert (
        _scalar(
            catalog,
            "SELECT count(*) FROM lake_meta.ducklake_metadata "
            "WHERE key = 'target_file_size'",
        )
        == option_before
    )


# --- inlined + small files -> two snapshots ----------------------------------
def test_checkpoint_flush_and_merge(catalog: CatalogHarness) -> None:
    _two_small_files(catalog, "mixed")
    catalog.write("INSERT INTO lake.main.mixed VALUES (1000)")  # inlined
    before = _max_snapshot(catalog)
    report = run_checkpoint(catalog.settings(), table="main.mixed")
    row = report.results[0]
    assert row.rows_flushed == 1
    assert row.merge.did_work
    assert row.snapshot_after == before + 2  # flush + maintenance
    assert report.snapshot_after == row.snapshot_after
    assert _scalar(catalog, "SELECT count(*) FROM lake.main.mixed") == 101


# --- --all-tables, only one table has work -----------------------------------
def test_checkpoint_all_tables_partial(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.with_inlined (id INTEGER)")
    catalog.write("INSERT INTO lake.main.with_inlined VALUES (1), (2), (3)")
    catalog.write("CREATE TABLE lake.main.empty (id INTEGER)")
    result = runner.invoke(
        app, ["checkpoint", "--all-tables", "--json"], env=cli_env_for(catalog)
    )
    assert result.exit_code == 0, result.stdout
    report = _json(result.stdout)["results"][0]
    assert {t["table_name"] for t in report["tables"]} >= {"with_inlined", "empty"}
    assert report["totals"]["tables_changed"] == 1
    assert report["totals"]["rows_flushed"] == 3


# --- auto_compact=false -> no effect, with a warning -------------------------
def test_checkpoint_auto_compact_off(catalog: CatalogHarness) -> None:
    _two_small_files(catalog, "disabled")
    catalog.write(
        "CALL lake.set_option('auto_compact', false, "
        "table_name => 'disabled', schema => 'main')"
    )
    report = run_checkpoint(catalog.settings(), table="main.disabled")
    assert report.exit_code == ExitCode.NOTHING_TO_DO
    assert any("auto_compact is off" in w for w in report.warnings)
    assert _live_files(catalog, "disabled") == 2


# --- mutually exclusive flags ------------------------------------------------
def test_checkpoint_mutually_exclusive(seeded_catalog: CatalogHarness) -> None:
    result = runner.invoke(
        app,
        ["checkpoint", "--table", "main.pedidos", "--all-tables", "--json"],
        env=cli_env_for(seeded_catalog),
    )
    assert result.exit_code == 1, result.stdout
    assert "mutually exclusive" in _json(result.stdout)["problems"][0]["message"]


def test_checkpoint_unqualified_table(seeded_catalog: CatalogHarness) -> None:
    result = runner.invoke(
        app,
        ["checkpoint", "--table", "pedidos", "--json"],
        env=cli_env_for(seeded_catalog),
    )
    assert result.exit_code == 1, result.stdout
    assert "qualified name" in _json(result.stdout)["problems"][0]["message"]


def test_checkpoint_missing_table(seeded_catalog: CatalogHarness) -> None:
    result = runner.invoke(
        app,
        ["checkpoint", "--table", "main.missing", "--json"],
        env=cli_env_for(seeded_catalog),
    )
    assert result.exit_code == 1, result.stdout
    assert "does not exist" in _json(result.stdout)["problems"][0]["message"]


# --- --dry-run does not write ------------------------------------------------
def test_checkpoint_dry_run(seeded_catalog: CatalogHarness) -> None:
    before = _max_snapshot(seeded_catalog)
    result = runner.invoke(
        app,
        [
            "checkpoint",
            "--table",
            "main.pedidos",
            "--dry-run",
            "--target-file-size",
            "64000000",
            "--json",
        ],
        env=cli_env_for(seeded_catalog),
    )
    assert result.exit_code == 0, result.stdout
    payload = _json(result.stdout)
    assert payload["dry_run"] is True
    assert payload["results"][0]["wrote"] is False
    assert payload["results"][0]["tables"][0]["rows_pending"] == 2
    assert payload["results"][0]["tables"][0]["target_file_size_bytes"] == 64_000_000
    assert payload["results"][0]["snapshot_after"] is None
    assert _max_snapshot(seeded_catalog) == before


# --- diverging fingerprint -> exit 3, nothing written ------------------------
def test_checkpoint_fingerprint_divergence(
    seeded_catalog: CatalogHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = cmd.read_fingerprint
    calls: list[bool] = []

    def forged(con: DuckLakeConnection) -> Any:
        fp = real(con)
        calls.append(con.read_only)
        if not con.read_only:
            return type(fp)("/other/", fp.format_version, fp.max_snapshot_id, 0)
        return fp

    executed: list[str] = []
    real_call = cmd._call

    def spy(con: DuckLakeConnection, sql: str) -> list[dict[str, Any]]:
        executed.append(sql)
        return real_call(con, sql)

    monkeypatch.setattr(cmd, "read_fingerprint", forged)
    monkeypatch.setattr(cmd, "_call", spy)
    before = _max_snapshot(seeded_catalog)
    with pytest.raises(DivergenceError, match="nothing was written"):
        run_checkpoint(seeded_catalog.settings(), table="main.pedidos")
    assert calls == [True, False]
    assert executed == []
    assert _max_snapshot(seeded_catalog) == before


# --- order of the DuckLake functions -----------------------------------------
def test_checkpoint_call_order(
    catalog: CatalogHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    _two_small_files(catalog, "ordering")
    executed: list[str] = []
    real_call = cmd._call

    def spy(con: DuckLakeConnection, sql: str) -> list[dict[str, Any]]:
        executed.append(sql.split("(")[0].removeprefix("CALL "))
        return real_call(con, sql)

    monkeypatch.setattr(cmd, "_call", spy)
    run_checkpoint(catalog.settings(), table="main.ordering")
    assert executed == [
        "ducklake_flush_inlined_data",
        "ducklake_expire_snapshots",
        "ducklake_merge_adjacent_files",
        "ducklake_rewrite_data_files",
    ]


# --- expire is governed by expire_older_than ---------------------------------
def test_checkpoint_expire_without_option_is_noop(catalog: CatalogHarness) -> None:
    _two_small_files(catalog, "no_expire")
    snapshots = _scalar(catalog, "SELECT count(*) FROM lake_meta.ducklake_snapshot")
    report = run_checkpoint(catalog.settings(), table="main.no_expire")
    assert report.results[0].snapshots_expired == ()
    # merge creates one snapshot; none was removed
    assert (
        _scalar(catalog, "SELECT count(*) FROM lake_meta.ducklake_snapshot")
        == snapshots + 1
    )


def test_checkpoint_expire_is_catalog_wide(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.other (id INTEGER)")
    catalog.write("INSERT INTO lake.main.other VALUES (7), (8)")  # inlined
    _two_small_files(catalog, "target")
    catalog.write("CALL lake.set_option('expire_older_than', '1 second')")
    time.sleep(1.2)

    report = run_checkpoint(catalog.settings(), table="main.target")
    row = report.results[0]
    assert report.exit_code == ExitCode.SUCCESS
    assert len(row.snapshots_expired) > 0
    assert row.merge.did_work
    # other table: data intact and inlined rows still pending
    assert _scalar(catalog, "SELECT count(*) FROM lake.main.other") == 2
    assert _scalar(catalog, "SELECT count(*) FROM lake.main.target") == 100
    remaining = _scalar(
        catalog,
        "SELECT count(*) FROM lake_meta.ducklake_snapshot "
        "WHERE list_contains($ids, snapshot_id)",
        {"ids": list(row.snapshots_expired)},
    )
    assert remaining == 0


# --- scope and fingerprint helpers -------------------------------------------
def test_resolve_scope_table(seeded_catalog: CatalogHarness) -> None:
    with (
        open_catalog(seeded_catalog.settings(), read_only=True) as con,
        con.read_transaction(),
    ):
        scope = resolve_scope(con, table="main.pedidos", schema=None, all_tables=False)
    assert scope.origin == "table"
    assert scope.tables[0].qualified == "main.pedidos"
    assert scope.tables[0].table_id > 0


def test_read_fingerprint(seeded_catalog: CatalogHarness) -> None:
    with (
        open_catalog(seeded_catalog.settings(), read_only=True) as con,
        con.read_transaction(),
    ):
        fp = read_fingerprint(con)
    assert fp.data_path is not None
    assert fp.format_version is not None
    assert fp.max_snapshot_id is not None
    assert fp.table_count >= 1


def test_phase_error_is_reported(
    catalog: CatalogHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    _two_small_files(catalog, "failing")
    real_call = cmd._call

    def failing(con: DuckLakeConnection, sql: str) -> list[dict[str, Any]]:
        if "merge_adjacent" in sql:
            raise duckdb.InvalidInputException("simulated")
        return real_call(con, sql)

    monkeypatch.setattr(cmd, "_call", failing)
    report = run_checkpoint(catalog.settings(), table="main.failing")
    assert report.exit_code == ExitCode.ERROR
    error = report.results[0].error
    assert error is not None
    assert error.phase == "merge"
    assert error.raw == "simulated"
    assert _live_files(catalog, "failing") == 2  # maintenance was rolled back
