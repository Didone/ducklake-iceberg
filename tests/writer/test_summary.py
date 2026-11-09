"""Delete bookkeeping in the snapshot summary."""

from __future__ import annotations

from typing import Any

import pytest
from pyiceberg.manifest import DataFileContent, ManifestContent, ManifestEntryStatus
from pyiceberg.table.snapshots import (
    ADDED_DELETE_FILES,
    ADDED_POSITION_DELETE_FILES,
    ADDED_POSITION_DELETES,
    OPERATION,
    REMOVED_DELETE_FILES,
    REMOVED_POSITION_DELETE_FILES,
    REMOVED_POSITION_DELETES,
    TOTAL_DELETE_FILES,
    TOTAL_EQUALITY_DELETES,
    TOTAL_POSITION_DELETES,
)

from ducklake_iceberg.errors import ExitCode
from ducklake_iceberg.writer import summary as sm


def _entry(
    status: ManifestEntryStatus,
    content: DataFileContent | int | None,
    record_count: int,
) -> dict[str, Any]:
    data_file: dict[str, Any] = {"record_count": record_count}
    if content is not None:
        data_file["content"] = int(content)
    return {"status": int(status), "data_file": data_file}


def test_counter_spelling() -> None:
    """The constants are PyIceberg's, not project literals."""
    assert ADDED_POSITION_DELETE_FILES == "added-position-delete-files"
    assert REMOVED_POSITION_DELETE_FILES == "removed-position-delete-files"
    assert sm.REMOVED_POSITION_DELETE_FILES is REMOVED_POSITION_DELETE_FILES


# -- collect_delete_delta ---------------------------------------------------
def test_collect_delete_delta() -> None:
    delta = sm.collect_delete_delta(
        [
            _entry(ManifestEntryStatus.ADDED, DataFileContent.POSITION_DELETES, 3),
            _entry(ManifestEntryStatus.ADDED, DataFileContent.POSITION_DELETES, 2),
            _entry(ManifestEntryStatus.EXISTING, DataFileContent.POSITION_DELETES, 9),
            _entry(ManifestEntryStatus.DELETED, DataFileContent.POSITION_DELETES, 4),
            _entry(ManifestEntryStatus.ADDED, DataFileContent.DATA, 100),
            _entry(ManifestEntryStatus.ADDED, None, 100),
        ]
    )
    assert delta == sm.DeleteDelta(
        added_delete_files=2,
        added_position_deletes=5,
        removed_delete_files=1,
        removed_position_deletes=4,
    )


def test_collect_rejects_equality_and_unknown_content() -> None:
    with pytest.raises(sm.UnexpectedContentError, match="EQUALITY") as exc_info:
        sm.collect_delete_delta(
            [_entry(ManifestEntryStatus.ADDED, DataFileContent.EQUALITY_DELETES, 1)]
        )
    assert exc_info.value.exit_code == ExitCode.ERROR
    with pytest.raises(sm.UnexpectedContentError, match="7"):
        sm.collect_delete_delta([_entry(ManifestEntryStatus.ADDED, 7, 1)])


# -- propagate_delete_totals ------------------------------------------------
_DELTA = sm.DeleteDelta(
    added_delete_files=2,
    added_position_deletes=5,
    removed_delete_files=1,
    removed_position_deletes=1,
)


def test_propagate_first_commit_and_parent() -> None:
    assert sm.propagate_delete_totals(None, _DELTA) == sm.DeleteTotals(1, 4)
    parent = {TOTAL_DELETE_FILES: "3", TOTAL_POSITION_DELETES: "10"}
    assert sm.propagate_delete_totals(parent, _DELTA) == sm.DeleteTotals(4, 14)
    # a key missing from the parent counts as zero
    assert sm.propagate_delete_totals({}, _DELTA) == sm.DeleteTotals(1, 4)


@pytest.mark.parametrize(
    ("parent", "key", "value"),
    [
        (
            {TOTAL_DELETE_FILES: "x", TOTAL_POSITION_DELETES: "1"},
            TOTAL_DELETE_FILES,
            "x",
        ),
        (
            {TOTAL_DELETE_FILES: "1", TOTAL_POSITION_DELETES: "y"},
            TOTAL_POSITION_DELETES,
            "y",
        ),
    ],
)
def test_propagate_invalid_parent_names_failing_key(
    parent: dict[str, str], key: str, value: str
) -> None:
    """Regression: the message cites the key that actually failed."""
    with pytest.raises(sm.InvalidParentSummaryError) as exc_info:
        sm.propagate_delete_totals(parent, _DELTA)
    assert exc_info.value.message.endswith(f"{key}={value}")


@pytest.mark.parametrize(
    ("delta", "key"),
    [
        (sm.DeleteDelta(0, 0, 1, 0), TOTAL_DELETE_FILES),
        (sm.DeleteDelta(0, 0, 0, 1), TOTAL_POSITION_DELETES),
    ],
)
def test_propagate_negative_counter(delta: sm.DeleteDelta, key: str) -> None:
    with pytest.raises(sm.NegativeCounterError, match=key):
        sm.propagate_delete_totals(None, delta)


# -- verify_against_manifest_list -------------------------------------------
def _ml_row(content: ManifestContent | int, files: int, rows: int) -> dict[str, Any]:
    return {
        "content": int(content),
        "added_files_count": files,
        "existing_files_count": 1,
        "added_rows_count": rows,
        "existing_rows_count": 2,
    }


def test_verify_against_manifest_list() -> None:
    rows = [
        _ml_row(ManifestContent.DATA, 50, 5000),
        _ml_row(ManifestContent.DELETES, 1, 3),
        _ml_row(9, 7, 7),  # unknown content is ignored
    ]
    sm.verify_against_manifest_list(sm.DeleteTotals(2, 5), rows)
    sm.verify_against_manifest_list(sm.DeleteTotals(0, 0), [])


@pytest.mark.parametrize(
    ("totals", "match"),
    [
        (sm.DeleteTotals(3, 5), "total_delete_files=3"),
        (sm.DeleteTotals(2, 6), "total_position_deletes=6"),
    ],
)
def test_verify_mismatch(totals: sm.DeleteTotals, match: str) -> None:
    with pytest.raises(sm.SummaryMismatchError, match=match) as exc_info:
        sm.verify_against_manifest_list(
            totals, [_ml_row(ManifestContent.DELETES, 1, 3)]
        )
    assert exc_info.value.exit_code == ExitCode.DIVERGED


def test_verify_empty_delete_file() -> None:
    row = {
        "content": int(ManifestContent.DELETES),
        "added_files_count": 1,
        "added_rows_count": 0,
    }
    with pytest.raises(sm.EmptyDeleteFileError, match="total_position_deletes=0"):
        sm.verify_against_manifest_list(sm.DeleteTotals(1, 0), [row])


# -- build_summary ----------------------------------------------------------
def test_build_summary_with_deletes() -> None:
    summary = sm.build_summary(
        operation="delete",
        data_part={"total-records": "10", "added-records": "0"},
        delete_delta=sm.DeleteDelta(2, 5, 1, 4),
        delete_totals=sm.DeleteTotals(3, 9),
        extra={"ducklake_iceberg.b": "2", "ducklake_iceberg.a": "1"},
    )
    assert next(iter(summary)) == OPERATION
    assert summary[OPERATION] == "delete"
    assert summary[ADDED_DELETE_FILES] == "2"
    assert summary[ADDED_POSITION_DELETE_FILES] == "2"
    assert summary[ADDED_POSITION_DELETES] == "5"
    assert summary[REMOVED_DELETE_FILES] == "1"
    assert summary[REMOVED_POSITION_DELETE_FILES] == "1"
    assert summary[REMOVED_POSITION_DELETES] == "4"
    assert summary[TOTAL_DELETE_FILES] == "3"
    assert summary[TOTAL_POSITION_DELETES] == "9"
    assert summary[TOTAL_EQUALITY_DELETES] == "0"
    assert list(summary)[-2:] == ["ducklake_iceberg.a", "ducklake_iceberg.b"]
    assert all(isinstance(v, str) for v in summary.values())


def test_build_summary_omits_zero_deltas() -> None:
    summary = sm.build_summary(
        operation="append",
        data_part={},
        delete_delta=sm.DeleteDelta(0, 0, 0, 0),
        delete_totals=sm.DeleteTotals(0, 0),
    )
    for key in (
        ADDED_DELETE_FILES,
        ADDED_POSITION_DELETE_FILES,
        ADDED_POSITION_DELETES,
        REMOVED_DELETE_FILES,
        REMOVED_POSITION_DELETE_FILES,
        REMOVED_POSITION_DELETES,
    ):
        assert key not in summary
    assert summary[TOTAL_DELETE_FILES] == "0"


def test_build_summary_append_with_deletes_is_incoherent() -> None:
    with pytest.raises(sm.IncoherentOperationError, match="append"):
        sm.build_summary(
            operation="append",
            data_part={},
            delete_delta=sm.DeleteDelta(1, 1, 0, 0),
            delete_totals=sm.DeleteTotals(1, 1),
        )
