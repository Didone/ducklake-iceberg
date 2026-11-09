"""Tests for the `rich` rendering of the checkpoint report."""

from __future__ import annotations

from typing import Any

import pytest

from iceduck.checkpoint.render import render_checkpoint


@pytest.fixture(autouse=True)
def _wide_console(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rich wraps cells at 80 columns; the table has 8 columns."""
    monkeypatch.setenv("COLUMNS", "200")


def _row(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "schema_name": "vendas",
        "table_name": "pedidos",
        "table_id": 5,
        "snapshot_before": 41,
        "snapshot_after": 43,
        "rows_flushed": 3,
        "snapshots_expired": [12, 13],
        "merge": {"files_processed": 4, "files_created": 1},
        "rewrite": {"files_processed": 0, "files_created": 0},
        "changed": True,
        "error": None,
    }
    base.update(over)
    return base


def _report(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "version": 1,
        "command": "checkpoint",
        "dry_run": False,
        "wrote": True,
        "catalog": {"alias": "lake", "metadata_catalog": "lake_meta"},
        "snapshot_before": 41,
        "snapshot_after": 43,
        "tables": [
            _row(),
            _row(
                table_name="itens",
                table_id=6,
                rows_flushed=0,
                snapshots_expired=[],
                merge={"files_processed": 0, "files_created": 0},
                changed=False,
                snapshot_after=43,
            ),
        ],
        "totals": {
            "tables_in_scope": 2,
            "tables_changed": 1,
            "rows_flushed": 3,
            "snapshots_expired": 2,
            "files_processed": 4,
            "files_created": 1,
        },
        "warnings": [],
        "exit_code": 0,
    }
    base.update(over)
    return base


def test_render_real(capsys: pytest.CaptureFixture[str]) -> None:
    render_checkpoint(_report())
    out = capsys.readouterr().out
    assert "iceduck checkpoint" in out
    assert "lake_meta" in out
    assert "41 -> 43" in out
    assert "4 → 1" in out
    assert "nothing to do" in out
    assert "2 snapshot(s) expired" in out


def test_render_dry_run(capsys: pytest.CaptureFixture[str]) -> None:
    report = {
        "version": 1,
        "command": "checkpoint",
        "dry_run": True,
        "wrote": False,
        "catalog": {"alias": "lake", "metadata_catalog": "lake_meta"},
        "snapshot_id": 41,
        "snapshot_before": 41,
        "snapshot_after": None,
        "tables": [
            {
                "schema_name": "vendas",
                "table_name": "pedidos",
                "table_id": 5,
                "rows_pending": 3,
                "data_rows": 3,
                "delete_rows": 0,
                "would_flush": True,
                "would_compact": True,
                "blocked_by": [],
            },
            {
                "schema_name": "vendas",
                "table_name": "notas",
                "table_id": 7,
                "rows_pending": 2,
                "data_rows": 2,
                "delete_rows": 0,
                "would_flush": False,
                "would_compact": False,
                "blocked_by": ["E-VARIANT-NESTED"],
            },
        ],
        "totals": {
            "tables_in_scope": 2,
            "tables_with_pending": 2,
            "tables_with_work": 1,
            "rows_pending": 5,
        },
        "warnings": [],
        "exit_code": 2,
    }
    render_checkpoint(report)
    out = capsys.readouterr().out
    assert "checkpoint --dry-run" in out
    assert "flush + compaction" in out
    assert "blocked (VARIANT)" in out
    assert "Nothing was written" in out
    assert "Run without --dry-run" in out
    assert "5 inlined row(s) in 2 table(s) at snapshot 41" in out


def test_render_error_and_warning(capsys: pytest.CaptureFixture[str]) -> None:
    report = _report(warnings=["auto_compact is off for vendas.itens"])
    report["tables"][0]["error"] = {"code": "E-CHECKPOINT-UNKNOWN", "phase": "merge"}
    render_checkpoint(report)
    out = capsys.readouterr().out
    assert "error" in out
    assert "merge" in out
    assert "auto_compact is off" in out


def _revalidation(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "snapshot_before": 41,
        "snapshot_after": 43,
        "rules_run": ["inlined-data", "partial-files", "delete-parquet"],
        "rules_skipped": ["unsupported-type", "calendar-transform"],
        "rules_errored": [],
        "still_pending_rows": 0,
        "findings": [],
        "warnings": [],
        "export_target_snapshot": 43,
        "exit_code": 0,
        "reversible": False,
    }
    base.update(over)
    return base


def test_render_revalidation_ok(capsys: pytest.CaptureFixture[str]) -> None:
    report = _report()
    report["tables"][0]["revalidation"] = _revalidation()
    render_checkpoint(report)
    out = capsys.readouterr().out
    assert "revalidation at snapshot 43" in out
    assert "3 rules rerun, 2 skipped" in out
    assert "ready for export" in out


def test_render_revalidation_blocked(capsys: pytest.CaptureFixture[str]) -> None:
    report = _report()
    report["tables"][0]["revalidation"] = _revalidation(
        exit_code=2,
        findings=[
            {
                "rule_id": "delete-parquet",
                "severity": "blocker",
                "origin": "introduced",
                "detail": "1 delete files in 'parquet' format",
                "remediation": "wait for the writer to support it",
            },
            {"rule_id": "partial-files", "severity": "info", "origin": "introduced"},
        ],
    )
    render_checkpoint(report)
    out = capsys.readouterr().out
    assert out.index("table(s) checkpointed") < out.index("delete-parquet")
    assert "this window" in out
    assert "not reversible" in out
    assert "wait for the writer to support it" in out


def test_render_revalidation_pending_and_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    report = _report()
    report["tables"][0]["revalidation"] = _revalidation(
        exit_code=3, still_pending_rows=2
    )
    report["tables"][1]["revalidation_error"] = "revalidation failed: connection lost"
    render_checkpoint(report)
    out = capsys.readouterr().out
    assert "still pending after the checkpoint: 2" in out
    assert "checkpoint completed" in out
    assert "revalidation failed: connection lost" in out


def test_render_nested_variant_error(capsys: pytest.CaptureFixture[str]) -> None:
    report = _report(exit_code=2)
    report["tables"][0]["error"] = {
        "code": "E-VARIANT-NESTED",
        "columns": [
            {
                "column_id": 7,
                "path": "payload.attrs",
                "parent_column_id": 5,
                "parent_type": "struct",
            }
        ],
        "snapshot_created": None,
        "remediation": "change the type at the source",
    }
    report["tables"][0]["changed"] = False
    render_checkpoint(report)
    out = capsys.readouterr().out
    assert "nested VARIANT" in out
    assert "payload.attrs" in out
    assert "column_id 7" in out
    assert "nothing was written to the catalog" in out
    assert "change the type at the source" in out


def test_render_variant_error_with_snapshot_created(
    capsys: pytest.CaptureFixture[str],
) -> None:
    report = _report(exit_code=2)
    report["tables"][0]["error"] = {
        "code": "E-VARIANT-NESTED",
        "columns": [
            {
                "column_id": 7,
                "path": "p.a",
                "parent_column_id": 5,
                "parent_type": "struct",
            }
        ],
        "snapshot_created": 42,
        "remediation": "x",
    }
    render_checkpoint(report)
    out = capsys.readouterr().out
    assert "created snapshot 42" in out


def test_render_snapshots_block(capsys: pytest.CaptureFixture[str]) -> None:
    report = _report()
    report["tables"][0]["snapshots"] = {
        "snapshot_before": 41,
        "export_target_snapshot": 43,
        "flush_snapshot_id": 42,
        "compaction_snapshot_id": 43,
        "attribution": "exact",
        "iceberg_operation_hint": "overwrite",
        "schema_version_changed": False,
        "foreign_snapshots": [],
        "window": [],
        "files_created": [],
        "files_retired": [],
        "warnings": [],
    }
    render_checkpoint(report)
    out = capsys.readouterr().out
    assert "snapshot 41 -> 43" in out
    assert "exact attribution" in out
    assert "flush ........ snapshot 42" in out
    assert "compaction ... snapshot 43" in out
    assert "Iceberg operation: overwrite" in out


def test_render_snapshots_ambiguous_warns(capsys: pytest.CaptureFixture[str]) -> None:
    report = _report()
    report["tables"][0]["snapshots"] = {
        "snapshot_before": 41,
        "export_target_snapshot": 44,
        "flush_snapshot_id": 42,
        "compaction_snapshot_id": 44,
        "attribution": "ambiguous",
        "iceberg_operation_hint": "overwrite",
        "schema_version_changed": False,
        "foreign_snapshots": [43],
        "window": [],
        "files_created": [],
        "files_retired": [],
        "warnings": ["could not attribute the checkpoint snapshots (3 in the window)"],
    }
    render_checkpoint(report)
    out = capsys.readouterr().out
    assert "ambiguous attribution" in out
    assert "another writer committed" in out
