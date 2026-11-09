"""Unit tests for the snapshot report (attribution, hint)."""

from __future__ import annotations

from ducklake_iceberg.checkpoint.model import TableRef
from ducklake_iceberg.checkpoint.report import (
    CheckpointSnapshotReport,
    FileRecord,
    SnapshotRow,
    _attribute,
    _parse_changes,
)

T = TableRef("vendas", "pedidos", 5)


def _row(sid: int, *changes: tuple[str, int | None]) -> SnapshotRow:
    return SnapshotRow(
        snapshot_id=sid, snapshot_time=None, schema_version=0, changes=tuple(changes)
    )


def test_parse_changes_single() -> None:
    assert _parse_changes("inline_flush:5") == (("inline_flush", 5),)


def test_parse_changes_multiple_any_separator() -> None:
    assert _parse_changes("rewrite_delete:5,merge_adjacent:5") == (
        ("rewrite_delete", 5),
        ("merge_adjacent", 5),
    )


def test_parse_changes_empty() -> None:
    assert _parse_changes(None) == ()
    assert _parse_changes("") == ()


# --- _attribute: the 4 cases ------------------------------------------------
def test_attribute_no_op_empty_window() -> None:
    assert _attribute((), 5) == (None, None, "no_op", ())


def test_attribute_exact_flush_and_compaction() -> None:
    window = (_row(42, ("inline_flush", 5)), _row(43, ("merge_adjacent", 5)))
    flush, comp, attribution, foreign = _attribute(window, 5)
    assert (flush, comp, attribution) == (42, 43, "exact")
    assert foreign == ()


def test_attribute_ambiguous_two_flush() -> None:
    window = (_row(42, ("inline_flush", 5)), _row(43, ("inline_flush", 5)))
    flush, _comp, attribution, _foreign = _attribute(window, 5)
    assert attribution == "ambiguous"
    assert flush == 43  # the highest


def test_attribute_absent_when_no_token_for_table() -> None:
    window = (_row(42, ("inline_flush", 9)),)  # another table
    flush, comp, attribution, foreign = _attribute(window, 5)
    assert (flush, comp, attribution) == (None, None, "absent")
    assert foreign == (42,)


def test_attribute_foreign_snapshots() -> None:
    window = (_row(42, ("inline_flush", 5)), _row(43, ("inline_flush", 9)))
    _flush, _comp, _attribution, foreign = _attribute(window, 5)
    assert foreign == (43,)


# --- iceberg_operation_hint -------------------------------------------------
def _report(
    *,
    created: tuple[FileRecord, ...] = (),
    retired: tuple[FileRecord, ...] = (),
    flush_id: int | None = None,
) -> CheckpointSnapshotReport:
    return CheckpointSnapshotReport(
        version=1,
        table=T,
        snapshot_before=41,
        export_target_snapshot=43,
        flush_snapshot_id=flush_id,
        compaction_snapshot_id=None,
        attribution="exact",
        window=(),
        foreign_snapshots=(),
        files_created=created,
        files_retired=retired,
        schema_version_changed=False,
    )


def _file(fid: int) -> FileRecord:
    return FileRecord("data", fid, f"p{fid}", True, 10, 100, None, 42, None)


def test_hint_none_when_empty() -> None:
    assert _report().iceberg_operation_hint == "none"


def test_hint_append_when_only_created() -> None:
    assert _report(created=(_file(1),)).iceberg_operation_hint == "append"


def test_hint_replace_when_only_retired() -> None:
    assert _report(retired=(_file(1),)).iceberg_operation_hint == "replace"


def test_hint_replace_when_compaction_only() -> None:
    # additions and removals without a flush => replace (same set of rows)
    r = _report(created=(_file(2),), retired=(_file(1),), flush_id=None)
    assert r.iceberg_operation_hint == "replace"


def test_hint_overwrite_when_flush_and_retired() -> None:
    r = _report(created=(_file(2),), retired=(_file(1),), flush_id=42)
    assert r.iceberg_operation_hint == "overwrite"


def test_to_json_shape() -> None:
    window = (_row(42, ("inline_flush", 5)),)
    report = CheckpointSnapshotReport(
        version=1,
        table=T,
        snapshot_before=41,
        export_target_snapshot=42,
        flush_snapshot_id=42,
        compaction_snapshot_id=None,
        attribution="exact",
        window=window,
        foreign_snapshots=(),
        files_created=(_file(1),),
        files_retired=(),
        schema_version_changed=False,
    )
    payload = report.to_json()
    assert payload["export_target_snapshot"] == 42
    assert payload["flush_snapshot_id"] == 42
    assert payload["iceberg_operation_hint"] == "append"
    assert payload["window"] == [
        {"snapshot_id": 42, "schema_version": 0, "changes": [["inline_flush", 5]]}
    ]
    assert payload["files_created"][0]["file_id"] == 1
