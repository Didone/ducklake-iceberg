"""Status matrix (pure function) and rendering (rich/JSON)."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from rich.console import Console

from iceduck import report
from iceduck.status import (
    Finding,
    GlueSide,
    Observed,
    Pending,
    Severity,
    StatusCode,
    TableStatus,
    build_snapshot_path,
    classify,
)
from iceduck.sync.state import StateMismatch

I_HAT = 7714
SNAPS: dict[int, tuple[int | None, str | None, dict[str, str]]] = {
    I_HAT: (None, "append", {"added-data-files": "1"}),
    8821: (I_HAT, "append", {"added-data-files": "3"}),
    9012: (8821, "overwrite", {"deleted-data-files": "12"}),
}
ZERO = Pending(0, 0, 0, 0, 0, 0)
CHANGED = Pending(2, 1, 0, 10, 0, 100)


def _ok(**kw: object) -> Observed:
    base = Observed(
        glue=GlueSide.OK,
        d_snapshot_id=10,
        d_schema_version=1,
        oldest_retained=0,
        i_snapshot_id=I_HAT,
        p_snapshot_id=10,
        i_hat_snapshot_id=I_HAT,
        p_schema_version=1,
        snapshots=SNAPS,
        pending=ZERO,
        remediation="iceduck sync --table s.t",
    )
    return replace(base, **kw)  # type: ignore[arg-type]


def _codes(findings: list[Finding]) -> list[StatusCode]:
    return [f.code for f in findings]


@pytest.mark.parametrize(
    ("obs", "status", "exit_code"),
    [
        (Observed(glue=GlueSide.ABSENT, d_snapshot_id=1), StatusCode.NEVER_SYNCED, 3),
        (
            Observed(glue=GlueSide.UNREADABLE, d_snapshot_id=1, glue_message="x"),
            StatusCode.UNREADABLE,
            1,
        ),
        (Observed(glue=GlueSide.FOREIGN, d_snapshot_id=1), StatusCode.FOREIGN, 3),
        (_ok(has_ducklake_keys=False), StatusCode.FOREIGN, 3),
        (_ok(ducklake_missing=True), StatusCode.IDENTITY_MISMATCH, 3),
        (
            _ok(
                state_mismatch=StateMismatch(
                    "IDENTITY_MISMATCH", "ducklake.table-uuid", "a", "b", "recreated"
                )
            ),
            StatusCode.IDENTITY_MISMATCH,
            3,
        ),
        (
            _ok(
                state_mismatch=StateMismatch(
                    "STATE_VERSION_AHEAD", "iceduck.state-version", "<= 1", "2", "v2"
                )
            ),
            StatusCode.UNREADABLE,
            1,
        ),
        (_ok(p_snapshot_id=None), StatusCode.UNREADABLE, 1),
        (_ok(i_snapshot_id=None), StatusCode.STATE_WITHOUT_SNAPSHOT, 3),
        (_ok(i_snapshot_id=9012), StatusCode.EXTERNAL_WRITE, 3),
        (_ok(i_snapshot_id=9012, i_hat_snapshot_id=1), StatusCode.HISTORY_REWRITTEN, 3),
        (
            _ok(i_snapshot_id=9012, i_hat_snapshot_id=None),
            StatusCode.HISTORY_REWRITTEN,
            3,
        ),
        (_ok(d_snapshot_id=8), StatusCode.CATALOG_REWOUND, 3),
        (
            _ok(d_snapshot_id=12, pending=CHANGED, oldest_retained=11),
            StatusCode.RETENTION_RISK,
            3,
        ),
        (_ok(d_snapshot_id=12, pending=CHANGED), StatusCode.BEHIND, 3),
        (_ok(d_schema_version=2), StatusCode.SCHEMA_DRIFT, 3),
        (_ok(d_snapshot_id=12, d_schema_version=2), StatusCode.SCHEMA_DRIFT, 3),
        (_ok(), StatusCode.IN_SYNC, 4),
        (_ok(d_snapshot_id=12), StatusCode.IN_SYNC, 4),
    ],
)
def test_matrix(obs: Observed, status: StatusCode, exit_code: int) -> None:
    got, severity, code, findings = classify(obs)
    assert (got, code) == (status, exit_code)
    assert findings[0].code is status
    assert findings[0].severity is severity
    assert code != 2  # status never returns 2


def test_in_sync_after_unrelated_snapshots_has_info() -> None:
    status, _sev, _code, findings = classify(_ok(d_snapshot_id=12))
    assert status is StatusCode.IN_SYNC
    assert findings[0].remediation is None
    assert findings[1].severity is Severity.INFO


def test_no_delta_uses_change_tokens() -> None:
    assert classify(_ok(d_snapshot_id=12, pending=None, table_changed=True))[0] is (
        StatusCode.BEHIND
    )
    assert classify(_ok(d_snapshot_id=12, pending=None, table_changed=False))[0] is (
        StatusCode.IN_SYNC
    )
    # no information at all: conservative
    assert classify(_ok(d_snapshot_id=12, pending=None))[0] is StatusCode.BEHIND


def test_external_write_reports_path() -> None:
    _s, _sev, _c, findings = classify(_ok(i_snapshot_id=9012))
    path = findings[0].detail["path"]
    assert path == [
        {
            "snapshot_id": 8821,
            "operation": "append",
            "added_data_files": "3",
            "deleted_data_files": None,
        },
        {
            "snapshot_id": 9012,
            "operation": "overwrite",
            "added_data_files": None,
            "deleted_data_files": "12",
        },
    ]


def test_external_write_with_schema_drift_keeps_both_findings() -> None:
    _s, _sev, _c, findings = classify(_ok(i_snapshot_id=9012, d_schema_version=3))
    assert _codes(findings) == [StatusCode.EXTERNAL_WRITE, StatusCode.SCHEMA_DRIFT]


@pytest.mark.parametrize(
    ("kw", "lake_codes"),
    [
        ({"d_snapshot_id": 12, "table_changed": True}, [StatusCode.BEHIND]),
        ({"d_snapshot_id": 8}, [StatusCode.CATALOG_REWOUND]),
        (
            {"d_snapshot_id": 12, "table_changed": True, "oldest_retained": 11},
            [StatusCode.RETENTION_RISK, StatusCode.BEHIND],
        ),
    ],
)
def test_diverged_both(kw: dict[str, object], lake_codes: list[StatusCode]) -> None:
    obs = _ok(i_snapshot_id=9012, pending=None, **kw)
    status, severity, code, findings = classify(obs)
    assert (status, severity, code) == (StatusCode.DIVERGED_BOTH, Severity.ERROR, 3)
    assert _codes(findings) == [
        StatusCode.DIVERGED_BOTH,
        StatusCode.EXTERNAL_WRITE,
        *lake_codes,
    ]


def test_behind_with_drift_and_retention_warning() -> None:
    obs = _ok(d_snapshot_id=14, pending=CHANGED, d_schema_version=2, oldest_retained=8)
    status, _sev, _code, findings = classify(obs)
    assert status is StatusCode.BEHIND
    assert _codes(findings) == [
        StatusCode.BEHIND,
        StatusCode.SCHEMA_DRIFT,
        StatusCode.RETENTION_RISK,
    ]
    assert findings[-1].severity is Severity.WARN


def test_snapshot_path_is_bounded_and_tolerates_gaps() -> None:
    chain: dict[int, tuple[int | None, str | None, dict[str, str]]] = {
        i: (i - 1, "append", {}) for i in range(1, 300)
    }
    assert len(build_snapshot_path(chain, 0, 299)) == 100
    assert build_snapshot_path({}, 1, 2) == []


def test_pending_has_changes() -> None:
    assert not ZERO.has_changes
    assert Pending(0, 0, 0, 0, 0, 0, added_delete_files=1).has_changes
    assert CHANGED.has_changes


# -- rendering ------------------------------------------------------------------


def _status(
    code: StatusCode = StatusCode.BEHIND,
    *,
    severity: Severity = Severity.WARN,
    exit_code: int = 3,
    pending: Pending | None = CHANGED,
    **kw: object,
) -> TableStatus:
    findings = classify(_ok(d_snapshot_id=12, pending=pending))[3]
    base = TableStatus(
        ducklake_schema="s",
        ducklake_table="t",
        glue_database="db",
        glue_table="t",
        d_snapshot_id=12,
        i_snapshot_id=I_HAT,
        p_snapshot_id=10,
        i_hat_snapshot_id=I_HAT,
        d_schema_version=1,
        p_schema_version=1,
        oldest_retained_snapshot=0,
        metadata_location="s3://b/p/db/t/metadata/v1.metadata.json",
        status=code,
        severity=severity,
        exit_code=exit_code,
        findings=tuple(findings),
        pending=pending,
        checked_at=datetime(2026, 3, 4, 9, 15, tzinfo=UTC),
        synced_at=datetime(2026, 3, 3, 23, 40, tzinfo=UTC),
    )
    return replace(base, **kw)  # type: ignore[arg-type]


def _console() -> Console:
    return Console(record=True, width=120, color_system=None)


def test_render_json_document() -> None:
    statuses = [
        _status(),
        _status(StatusCode.IN_SYNC, severity=Severity.OK, exit_code=4, pending=None),
        _status(
            StatusCode.UNREADABLE,
            severity=Severity.ERROR,
            exit_code=1,
            pending=None,
            i_snapshot_id=None,
            metadata_location=None,
        ),
    ]
    doc = report.render_json(statuses)
    json.dumps(doc)  # serializable
    assert doc["version"] == 1
    tables = doc["tables"]
    assert isinstance(tables, list)
    first = tables[0]
    assert first["state"]["synced_at"] == "2026-03-03T23:40:00Z"
    assert first["pending"]["added_files"] == 1
    assert first["pending"]["unpublished_changes"] == 1
    assert first["pending"]["unpublished_age_s"] > 0
    assert tables[1]["pending"] is None  # explicit null
    assert tables[2]["iceberg"]["metadata_location"] is None
    summary = doc["summary"]
    assert isinstance(summary, dict)
    assert summary["exit_code"] == 1
    assert summary["worst_severity"] == "error"
    assert summary["errors"] == 1
    assert summary["by_status"] == {"behind": 1, "in_sync": 1, "unreadable": 1}


def test_aggregate_exit_code() -> None:
    assert report.aggregate_exit_code([]) == 4
    assert report.aggregate_exit_code([_status(exit_code=4)]) == 4
    assert report.aggregate_exit_code([_status(exit_code=4), _status()]) == 3
    assert report.aggregate_exit_code([_status(), _status(exit_code=1)]) == 1


def test_render_rich_single_behind() -> None:
    console = _console()
    status = _status(pending=replace(CHANGED, removed_files=2, added_delete_files=1))
    report.render_rich([status], console=console)
    text = console.export_text()
    assert "BEHIND" in text
    assert "✓ equal to I" in text
    assert "-2 files" in text
    assert "delete files +1/-0" in text
    assert "Run: iceduck sync --table s.t" in text


def test_render_rich_single_mismatch_markers() -> None:
    console = _console()
    status = _status(
        StatusCode.EXTERNAL_WRITE,
        severity=Severity.ERROR,
        i_snapshot_id=9012,
        d_schema_version=2,
        pending=None,
        d_snapshot_id=None,
    )
    report.render_rich([status], console=console)
    text = console.export_text()
    assert "✗ differs from I" in text
    assert "(absent)" in text
    assert "Pending" not in text


def test_render_rich_in_sync_and_empty() -> None:
    console = _console()
    status = _status(
        StatusCode.IN_SYNC,
        severity=Severity.OK,
        exit_code=4,
        pending=ZERO,
        findings=(Finding(StatusCode.IN_SYNC, Severity.OK, "in sync [x]", {}, None),),
    )
    report.render_rich([status], console=console)
    report.render_rich([], console=console)
    text = console.export_text()
    assert "Nothing to do." in text
    assert "in sync [x]" in text  # markup escaped
    assert "No tables found." in text


def test_render_rich_many() -> None:
    console = _console()
    statuses = [
        _status(),
        _status(StatusCode.IN_SYNC, severity=Severity.OK, exit_code=4, pending=None),
        _status(
            StatusCode.NEVER_SYNCED,
            severity=Severity.INFO,
            pending=None,
            d_snapshot_id=None,
            p_snapshot_id=None,
        ),
    ]
    report.render_rich(statuses, console=console)
    text = console.export_text()
    assert "Total: 3 tables" in text
    assert "behind: 1" in text
    assert "+1 files" in text


def test_print_json(capsys: pytest.CaptureFixture[str]) -> None:
    report.print_json({"version": 1})
    assert json.loads(capsys.readouterr().out) == {"version": 1}
