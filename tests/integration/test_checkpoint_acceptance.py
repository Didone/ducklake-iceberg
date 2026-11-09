"""Acceptance tests for checkpoint edge cases.

Covers the interactive confirmation of `--all-tables`, the exit 4 message and
the degraded-detection warning reaching `--json` under `warnings`.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from typer.testing import CliRunner

from ducklake_iceberg.checkpoint.command import run_checkpoint
from ducklake_iceberg.cli import app
from ducklake_iceberg.errors import ExitCode, OperationAborted
from tests.integration.conftest import CatalogHarness, cli_env_for

pytestmark = [pytest.mark.integration]

runner = CliRunner()


def _seed_all_tables(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.a (id INTEGER)")
    catalog.write("INSERT INTO lake.main.a VALUES (1), (2)")
    catalog.write("CREATE TABLE lake.main.b (id INTEGER)")
    catalog.write("INSERT INTO lake.main.b VALUES (3), (4)")


def _max_snapshot(catalog: CatalogHarness) -> int:
    from ducklake_iceberg.catalog.connection import open_catalog

    with open_catalog(catalog.settings(), read_only=True) as con:
        return int(
            con.query_arrow("SELECT max(snapshot_id) FROM lake_meta.ducklake_snapshot")
            .column(0)[0]
            .as_py()
        )


# --- interactive confirmation of --all-tables --------------------------------
def test_all_tables_confirm_declined_aborts(catalog: CatalogHarness) -> None:
    _seed_all_tables(catalog)
    before = _max_snapshot(catalog)
    with pytest.raises(OperationAborted, match="nothing was written"):
        run_checkpoint(catalog.settings(), all_tables=True, confirm=lambda _n: False)
    assert _max_snapshot(catalog) == before  # nothing written


def test_all_tables_confirm_accepted_proceeds(catalog: CatalogHarness) -> None:
    _seed_all_tables(catalog)
    calls: list[int] = []

    def confirm(n: int) -> bool:
        calls.append(n)
        return True

    report = run_checkpoint(catalog.settings(), all_tables=True, confirm=confirm)
    assert calls == [2]  # called once, with the number of tables
    assert report.exit_code == ExitCode.SUCCESS
    assert report.tables_changed == 2


def test_all_tables_yes_skips_confirm(catalog: CatalogHarness) -> None:
    _seed_all_tables(catalog)
    called = False

    def confirm(_n: int) -> bool:
        nonlocal called
        called = True
        return False

    report = run_checkpoint(
        catalog.settings(), all_tables=True, assume_yes=True, confirm=confirm
    )
    assert called is False  # --yes skips the prompt
    assert report.tables_changed == 2


def test_single_table_never_confirms(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1)")
    called = False

    def confirm(_n: int) -> bool:
        nonlocal called
        called = True
        return False

    report = run_checkpoint(catalog.settings(), table="main.t", confirm=confirm)
    assert called is False  # --table does not ask for confirmation
    assert report.tables_changed == 1


def test_all_tables_no_confirm_callback_proceeds(catalog: CatalogHarness) -> None:
    # non-interactive run (confirm=None): proceeds
    _seed_all_tables(catalog)
    report = run_checkpoint(catalog.settings(), all_tables=True, confirm=None)
    assert report.tables_changed == 2


# --- exit 4 message ----------------------------------------------------------
def test_exit_4_message_in_rich(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t SELECT range FROM range(0, 100)")
    env = {**cli_env_for(catalog), "COLUMNS": "200"}
    runner.invoke(app, ["checkpoint", "--table", "main.t"], env=env)  # compacts
    result = runner.invoke(app, ["checkpoint", "--table", "main.t"], env=env)
    assert result.exit_code == 4
    assert "nothing to do: checkpoint had no effect in scope" in result.stdout


# --- degraded-detection warning in --json under warnings ---------------------
def _force_degraded_inlined(catalog: CatalogHarness) -> None:
    """Recreate the physical inlining table without the visibility columns,
    forcing the degraded detection mode."""
    from ducklake_iceberg.catalog.connection import open_catalog
    from ducklake_iceberg.catalog.reader import current_snapshot

    catalog.write("CREATE TABLE lake.main.deg (id INTEGER)")
    with open_catalog(catalog.settings(), read_only=True) as con:
        tid = int(
            con.query_arrow(
                "SELECT table_id FROM lake_meta.ducklake_table WHERE table_name = 'deg'"
            )
            .column(0)[0]
            .as_py()
        )
        sv = current_snapshot(con).schema_version
    name = f"ducklake_inlined_data_{tid}_{sv}"
    catalog.meta_exec(f'DROP TABLE IF EXISTS "{name}"')
    catalog.meta_exec(f'CREATE TABLE "{name}" (id INTEGER)')
    catalog.meta_exec(f'INSERT INTO "{name}" VALUES (1), (2)')
    catalog.meta_exec(
        "INSERT INTO ducklake_inlined_data_tables "
        f"(table_id, table_name, schema_version) VALUES ({tid}, '{name}', {sv})"
    )


def test_degraded_detection_warning_in_json(catalog: CatalogHarness) -> None:
    _force_degraded_inlined(catalog)
    result = runner.invoke(
        app,
        ["checkpoint", "--table", "main.deg", "--dry-run", "--json"],
        env=cli_env_for(catalog),
    )
    assert result.exit_code == 0, result.stdout
    payload: dict[str, Any] = json.loads(result.stdout)["results"][0]
    warnings = payload["warnings"]
    assert any("without visibility filter" in w for w in warnings), warnings
    # the degraded count includes all physical rows (overestimates)
    assert payload["tables"][0]["rows_pending"] == 2
    assert payload["tables"][0]["sources"][0]["visibility_filtered"] is False
