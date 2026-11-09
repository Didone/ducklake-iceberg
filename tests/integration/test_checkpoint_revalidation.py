"""Integration tests for the post-checkpoint revalidation.

The SAME suite runs against SQLite and Postgres. Every scenario starts from a
real checkpoint (`run_checkpoint`), which chains the revalidation at the end.
"""

from __future__ import annotations

import json
import re
from typing import Any

import pytest
from typer.testing import CliRunner

from ducklake_iceberg.checkpoint import revalidate as rv
from ducklake_iceberg.checkpoint.command import run_checkpoint
from ducklake_iceberg.checkpoint.revalidate import (
    REVALIDATION_RULES,
    revalidate_after_checkpoint,
)
from ducklake_iceberg.cli import app
from ducklake_iceberg.doctor.model import Severity
from ducklake_iceberg.errors import ExitCode
from tests.integration.conftest import CatalogHarness, cli_env_for

pytestmark = [pytest.mark.integration]

runner = CliRunner()

#: Terms forbidden in the output (the checkpoint cannot be undone).
_ROLLBACK_WORDS = re.compile(r"(?i)\b(undo|revert|rollback|restore)\b")


def _by_rule(rev: rv.Revalidation, rule_id: str) -> list[rv.ClassifiedFinding]:
    return [c for c in rev.findings if c.finding.rule_id == rule_id]


def _two_inlined_transactions(h: CatalogHarness, table: str = "t") -> None:
    # `table` is always a literal from the test itself
    target = f"lake.main.{table}"
    h.write(f"CREATE TABLE {target} (id INTEGER)")
    h.write(f"INSERT INTO {target} VALUES (1)")
    h.write(f"INSERT INTO {target} VALUES (2)")


# --- checkpoint partial file does not block the target snapshot --------------
def test_partial_file_does_not_block_current(catalog: CatalogHarness) -> None:
    _two_inlined_transactions(catalog)
    report = run_checkpoint(catalog.settings(), table="main.t")
    rev = report.results[0].revalidation
    assert rev is not None
    assert rev.exit_code == ExitCode.SUCCESS
    assert report.exit_code == ExitCode.SUCCESS
    partial = _by_rule(rev, "partial-files")
    assert partial, "flushing two transactions should produce a partial file"
    assert all(c.finding.severity.name == "INFO" for c in partial)
    assert partial[0].origin == "introduced"


# --- target pinned to an earlier snapshot -> partial-files blocks ------------
def test_partial_file_blocks_historical_target(catalog: CatalogHarness) -> None:
    _two_inlined_transactions(catalog)
    report = run_checkpoint(catalog.settings(), table="main.t")
    result = report.results[0]
    rev = result.revalidation
    assert rev is not None
    info = _by_rule(rev, "partial-files")[0]
    historical = info.object_begin_snapshot  # begin_snapshot < partial_max
    assert historical is not None
    old = revalidate_after_checkpoint(
        catalog.settings(),
        result.table,
        snapshot_before=result.snapshot_before,
        snapshot_after=historical,
    )
    assert old.exit_code == ExitCode.BLOCKED
    blockers = _by_rule(old, "partial-files")
    assert any(c.finding.severity.name == "BLOCKER" for c in blockers)


# --- inlined delete becomes a parquet delete file -> introduced -------------
def test_checkpoint_introduces_delete_file(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1), (2), (3)")
    catalog.write("DELETE FROM lake.main.t WHERE id = 2")
    report = run_checkpoint(catalog.settings(), table="main.t")
    rev = report.results[0].revalidation
    assert rev is not None
    deletes = _by_rule(rev, "delete-parquet")
    assert deletes
    assert deletes[0].origin == "introduced"
    # the export rewrites the parquet delete file: degradation, not a blocker
    # (the revalidation still reports it as introduced)
    assert deletes[0].finding.severity is Severity.DEGRADED
    assert rev.exit_code == ExitCode.SUCCESS
    assert not rev.introduced_blockers


# --- delete file predating the checkpoint -> pre_existing --------------------
def test_pre_existing_delete_file(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t SELECT range FROM range(0, 50)")
    catalog.write(
        "DELETE FROM lake.main.t WHERE id < 30"
    )  # above the inlining limit: goes to a file
    report = run_checkpoint(catalog.settings(), table="main.t")
    rev = report.results[0].revalidation
    assert rev is not None
    deletes = _by_rule(rev, "delete-parquet")
    assert deletes
    assert deletes[0].origin == "pre_existing"
    assert (
        deletes[0].to_json()["origin_text"] == "already existed before the checkpoint"
    )
    assert not rev.introduced_blockers


# --- clean checkpoint -> 6 rules run, 3 skipped, exit 0 ----------------------
def test_clean_checkpoint_runs_six_rules(seeded_catalog: CatalogHarness) -> None:
    report = run_checkpoint(seeded_catalog.settings(), table="main.pedidos")
    rev = report.results[0].revalidation
    assert rev is not None
    assert set(rev.rules_run) == REVALIDATION_RULES
    assert len(rev.rules_run) == 6
    # the logical-definition rules are not rerun, nor is the configuration
    # advisory (the checkpoint does not change the codec)
    assert set(rev.rules_skipped) == {
        "unsupported-type",
        "calendar-transform",
        "parquet-compression",
    }
    assert rev.exit_code == ExitCode.SUCCESS
    payload = rev.to_json()
    assert payload["reversible"] is False
    assert payload["export_target_snapshot"] == report.results[0].snapshot_after


# --- third-party inlined rows after the checkpoint ---------------------------
def test_inlined_rows_after_the_flush_move_the_export_target(
    seeded_catalog: CatalogHarness,
) -> None:
    # a third party commits inlined rows after the flush: the export target
    # falls back to the flush snapshot, which has nothing pending; the new rows
    # are left for the next checkpoint (detect.clean_snapshot)
    report = run_checkpoint(seeded_catalog.settings(), table="main.pedidos")
    result = report.results[0]
    seeded_catalog.write(
        "INSERT INTO lake.main.pedidos VALUES (9, 'RJ')"
    )  # third party
    rev = revalidate_after_checkpoint(
        seeded_catalog.settings(),
        result.table,
        snapshot_before=result.snapshot_before,
        snapshot_after=result.snapshot_after + 1,
    )
    assert rev.snapshot_after == result.snapshot_after
    assert rev.still_pending_rows == 0
    assert rev.exit_code == ExitCode.SUCCESS


# --- revalidation uses a new READ_ONLY connection ----------------------------
def test_revalidation_opens_new_read_only_connection(
    seeded_catalog: CatalogHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ducklake_iceberg.catalog.connection import open_catalog as real_open

    opened: list[bool] = []

    def spy(cfg: Any, *, read_only: bool = True, s3: bool = True) -> Any:
        opened.append(read_only)
        return real_open(cfg, read_only=read_only, s3=s3)

    monkeypatch.setattr("ducklake_iceberg.checkpoint.revalidate.open_catalog", spy)
    report = run_checkpoint(seeded_catalog.settings(), table="main.pedidos")
    assert opened == [True]
    rev = report.results[0].revalidation
    assert rev is not None
    assert rev.snapshot_after == report.results[0].snapshot_after


# --- no output suggests undoing the checkpoint -------------------------------
@pytest.mark.parametrize("scenario", ["clean", "blocked", "revalidation_error"])
def test_no_rollback_wording(
    catalog: CatalogHarness, scenario: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ducklake_iceberg.errors import CatalogError

    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1), (2), (3)")
    if scenario == "blocked":
        catalog.write("DELETE FROM lake.main.t WHERE id = 2")
    if scenario == "revalidation_error":

        def fail(*_: Any, **__: Any) -> Any:
            raise CatalogError("connection dropped")

        monkeypatch.setattr("ducklake_iceberg.checkpoint.command.revalidate_many", fail)
    env = {**cli_env_for(catalog), "COLUMNS": "200"}
    outputs = [
        runner.invoke(app, ["checkpoint", "--table", "main.t"], env=env).stdout,
        runner.invoke(
            app, ["checkpoint", "--table", "main.t", "--json"], env=env
        ).stdout,
    ]
    for out in outputs:
        assert not _ROLLBACK_WORDS.search(out), out


# --- failure to reopen the connection: exit 1, checkpoint kept ---------------
def test_revalidation_failure_keeps_checkpoint(
    seeded_catalog: CatalogHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ducklake_iceberg.errors import CatalogError

    def fail(*_: Any, **__: Any) -> Any:
        raise CatalogError("catalog unreachable")

    monkeypatch.setattr("ducklake_iceberg.checkpoint.command.revalidate_many", fail)
    report = run_checkpoint(seeded_catalog.settings(), table="main.pedidos")
    result = report.results[0]
    assert report.exit_code == ExitCode.ERROR
    assert result.rows_flushed == 2  # the checkpoint happened and is reported
    assert result.revalidation is None
    assert "revalidation failed" in (result.revalidation_error or "")


# --- --all-tables with one blocked and two ok tables -> exit 2 ---------------
def test_all_tables_aggregates_blocker(
    catalog: CatalogHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # without the position-deletes capability, the parquet delete file is a
    # blocker again: the simplest blocker to produce to test the aggregation
    from ducklake_iceberg.doctor.rules import delete_parquet

    class _NoCapability:
        POSITION_DELETES = object()

    monkeypatch.setattr(delete_parquet, "Capability", _NoCapability)
    catalog.write("CREATE TABLE lake.main.ok1 (id INTEGER)")
    catalog.write("INSERT INTO lake.main.ok1 VALUES (1)")
    catalog.write("CREATE TABLE lake.main.ok2 (id INTEGER)")
    catalog.write("CREATE TABLE lake.main.blocked (id INTEGER)")
    catalog.write("INSERT INTO lake.main.blocked VALUES (1), (2), (3)")
    catalog.write("DELETE FROM lake.main.blocked WHERE id = 2")
    result = runner.invoke(
        app, ["checkpoint", "--all-tables", "--json"], env=cli_env_for(catalog)
    )
    assert result.exit_code == 2, result.stdout
    tables = json.loads(result.stdout)["results"][0]["tables"]
    assert len(tables) == 3
    by_name = {t["table_name"]: t["revalidation"]["exit_code"] for t in tables}
    assert by_name == {"blocked": 2, "ok1": 0, "ok2": 0}


# --- compaction of old files -> new object, no blocker -----------------------
def test_merge_only_classified_by_file_id(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t SELECT range FROM range(0, 50)")
    catalog.write("INSERT INTO lake.main.t SELECT range FROM range(50, 100)")
    report = run_checkpoint(catalog.settings(), table="main.t")
    result = report.results[0]
    assert result.merge.did_work
    rev = result.revalidation
    assert rev is not None
    partial = _by_rule(rev, "partial-files")
    assert partial
    # the merged file inherits the smallest begin_snapshot (before the window),
    # but it is new: classification by file_id recognizes it as having
    # appeared within the window
    assert partial[0].object_begin_snapshot is not None
    assert partial[0].object_begin_snapshot <= result.snapshot_before
    assert partial[0].origin == "introduced"
    assert rev.exit_code == ExitCode.SUCCESS
