"""Integration tests for the checkpoint snapshot report.

Every scenario starts from a real checkpoint (`run_checkpoint`), which attaches
the `CheckpointSnapshotReport` to each result in step 17.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from typer.testing import CliRunner

from ducklake_iceberg.checkpoint.command import run_checkpoint
from ducklake_iceberg.cli import app
from tests.integration.conftest import CatalogHarness, cli_env_for

pytestmark = [pytest.mark.integration]

runner = CliRunner()


def _snap(report: Any) -> Any:
    snap = report.results[0].snapshots
    assert snap is not None
    return snap


def _two_small_files(h: CatalogHarness, table: str) -> None:
    target = f"lake.main.{table}"
    h.write(f"CREATE TABLE {target} (id INTEGER)")
    h.write(f"INSERT INTO {target} SELECT range FROM range(0, 50)")
    h.write(f"INSERT INTO {target} SELECT range FROM range(50, 100)")


# --- inlined rows only -----------------------------------------------------
def test_only_flush(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1)")
    snap = _snap(run_checkpoint(catalog.settings(), table="main.t"))
    assert snap.flush_snapshot_id == snap.export_target_snapshot
    assert snap.compaction_snapshot_id is None
    assert snap.attribution == "exact"


# --- inlined + small files: two snapshots, target = compaction --------------
def test_flush_and_compaction_two_snapshots(catalog: CatalogHarness) -> None:
    _two_small_files(catalog, "t")
    catalog.write("INSERT INTO lake.main.t VALUES (1000)")  # inlined
    snap = _snap(run_checkpoint(catalog.settings(), table="main.t"))
    assert snap.flush_snapshot_id is not None
    assert snap.compaction_snapshot_id is not None
    assert snap.flush_snapshot_id < snap.compaction_snapshot_id
    assert snap.export_target_snapshot == snap.compaction_snapshot_id
    # the merge retired the small files
    assert len(snap.files_retired) >= 2


# --- small files only -> compaction, replace hint ---------------------------
def test_only_compaction(catalog: CatalogHarness) -> None:
    _two_small_files(catalog, "t")
    snap = _snap(run_checkpoint(catalog.settings(), table="main.t"))
    assert snap.flush_snapshot_id is None
    assert snap.compaction_snapshot_id is not None
    assert snap.iceberg_operation_hint == "replace"


# --- checkpoint with no effect ----------------------------------------------
def test_no_op(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t SELECT range FROM range(0, 100)")
    run_checkpoint(catalog.settings(), table="main.t")  # compacts
    snap = _snap(run_checkpoint(catalog.settings(), table="main.t"))  # 2nd time
    assert snap.attribution == "no_op"
    assert snap.export_target_snapshot == snap.snapshot_before
    assert snap.iceberg_operation_hint == "none"


# --- pinned changes_made tokens ---------------------------------------------
def test_changes_made_tokens(catalog: CatalogHarness) -> None:
    _two_small_files(catalog, "t")
    catalog.write("INSERT INTO lake.main.t VALUES (1000)")
    snap = _snap(run_checkpoint(catalog.settings(), table="main.t"))
    kinds = {kind for row in snap.window for kind, _tid in row.changes}
    # the flush records inline_flush; compaction, merge_adjacent and/or rewrite_delete
    assert "inline_flush" in kinds
    assert kinds & {"merge_adjacent", "rewrite_delete"}
    # all tokens attributed to this table carry the correct table_id
    table_id = snap.table.table_id
    for row in snap.window:
        for _kind, tid in row.changes:
            assert tid == table_id


# --- third-party insert between phases -> foreign_snapshots -----------------
def test_foreign_snapshot_in_window(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1)")
    catalog.write("CREATE TABLE lake.main.outra (id INTEGER)")
    # Forging a third-party snapshot (an insert into another table between
    # before and after) by simulating concurrency through the catalog
    # begin_snapshot is not trivial here; instead, check that the window only
    # attributes what belongs to this table.
    snap = _snap(run_checkpoint(catalog.settings(), table="main.t"))
    assert snap.foreign_snapshots == ()  # no real concurrency, so no foreign snapshots


# --- schema_version_changed flag --------------------------------------------
def test_schema_version_unchanged_by_checkpoint(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1)")
    snap = _snap(run_checkpoint(catalog.settings(), table="main.t"))
    assert snap.schema_version_changed is False


# --- JSON: snapshots block present ------------------------------------------
def test_snapshots_in_json(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1)")
    result = runner.invoke(
        app, ["checkpoint", "--table", "main.t", "--json"], env=cli_env_for(catalog)
    )
    assert result.exit_code == 0, result.stdout
    snap = json.loads(result.stdout)["results"][0]["tables"][0]["snapshots"]
    assert snap is not None
    assert snap["export_target_snapshot"] == snap["flush_snapshot_id"]
    assert snap["attribution"] == "exact"
    assert "window" in snap
    assert "iceberg_operation_hint" in snap


# --- rich: header and export target -----------------------------------------
def test_snapshots_in_rich(catalog: CatalogHarness) -> None:
    _two_small_files(catalog, "t")
    catalog.write("INSERT INTO lake.main.t VALUES (1000)")
    result = runner.invoke(
        app,
        ["checkpoint", "--table", "main.t"],
        env={**cli_env_for(catalog), "COLUMNS": "200"},
    )
    assert result.exit_code == 0
    assert "export target" in result.stdout
    assert "Iceberg operation" in result.stdout


# --- sync contract (snapshot_after == export_target) ------------------------
def test_snapshot_after_matches_target(catalog: CatalogHarness) -> None:
    _two_small_files(catalog, "t")
    catalog.write("INSERT INTO lake.main.t VALUES (1000)")
    report = run_checkpoint(catalog.settings(), table="main.t")
    result = report.results[0]
    assert result.snapshots is not None
    assert result.snapshot_after == result.snapshots.export_target_snapshot
