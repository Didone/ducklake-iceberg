"""Temporal filtering over DeleteSets assembled in memory."""

from __future__ import annotations

import dataclasses
import logging

import pyarrow as pa
import pytest

from ducklake_iceberg.deletes import visibility as vis
from ducklake_iceberg.deletes.reader import (
    POSITIONS_SCHEMA,
    DeleteOrigin,
    DeleteSet,
    DuckLakeDeleteFile,
)
from ducklake_iceberg.errors import ExitCode

_A = "/d/a.parquet"
_B = "/d/b.parquet"


def _file(
    delete_file_id: int = 10,
    data_file_id: int = 1,
    *,
    begin: int = 5,
    end: int | None = None,
    partial_max: int | None = None,
    data_file_path: str | None = _A,
) -> DuckLakeDeleteFile:
    return DuckLakeDeleteFile(
        delete_file_id=delete_file_id,
        data_file_id=data_file_id,
        path=f"/d/del-{delete_file_id}.parquet",
        file_format="parquet",
        delete_count=1,
        file_size_bytes=1,
        footer_size=None,
        row_group_count=None,
        begin_snapshot=begin,
        end_snapshot=end,
        partial_max=partial_max,
        encryption_key=None,
        data_file_path=data_file_path,
        data_record_count=100,
        data_file_live=data_file_path is not None,
        has_snapshot_column=partial_max is not None,
    )


Row = tuple[str, int, int, DeleteOrigin, int]


def _positions(rows: list[Row]) -> pa.Table:
    return pa.table(
        {
            "data_file_path": pa.array([r[0] for r in rows], type=pa.large_string()),
            "pos": pa.array([r[1] for r in rows], type=pa.int64()),
            "snapshot_id": pa.array([r[2] for r in rows], type=pa.int64()),
            "origin": pa.array(
                [r[3].value for r in rows], type=pa.dictionary(pa.int8(), pa.string())
            ),
            "source_id": pa.array([r[4] for r in rows], type=pa.int64()),
        },
        schema=POSITIONS_SCHEMA,
    )


def _ds(rows: list[Row], files: tuple[DuckLakeDeleteFile, ...]) -> DeleteSet:
    return DeleteSet(
        table_id=1,
        snapshot_id=6,
        positions=_positions(rows) if rows else POSITIONS_SCHEMA.empty_table(),
        files=files,
        inlined=None,
        warnings=(),
    )


_DF = DeleteOrigin.DELETE_FILE
_IN = DeleteOrigin.INLINED


# -- visible_at / needs_row_filter ---------------------------
@pytest.mark.parametrize(
    ("begin", "end", "snapshot", "expected"),
    [
        (5, None, 5, True),  # end NULL: included
        (5, None, 4, False),  # delete from the future
        (5, 7, 6, True),
        (5, 7, 7, False),  # half-open: ends where the next one starts
    ],
)
def test_visible_at(begin: int, end: int | None, snapshot: int, expected: bool) -> None:
    assert vis.visible_at(_file(begin=begin, end=end), snapshot) is expected


def test_needs_row_filter() -> None:
    assert vis.needs_row_filter(_file(partial_max=7), 6) is True
    assert vis.needs_row_filter(_file(partial_max=6), 6) is False
    assert vis.needs_row_filter(_file(), 6) is False


# -- filter_visible ---------------------------------------------------------
def test_empty_delete_set_reports_zero() -> None:
    ds = _ds([], (_file(partial_max=9),))
    t, report = vis.filter_visible(ds, snapshot_id=6, live_data_files={_A})
    assert t.num_rows == 0
    assert report.rows_read == 0
    assert report.files_visible == 1
    assert report.files_row_filtered == 1
    assert report.consistent


def test_future_rows_filtered_in_partial_file() -> None:
    f = _file(partial_max=7)
    ds = _ds([(_A, 1, 5, _DF, 10), (_A, 2, 7, _DF, 10)], (f,))
    t, report = vis.filter_visible(ds, snapshot_id=6, live_data_files={_A})
    assert t["pos"].to_pylist() == [1]
    assert report.files_row_filtered == 1
    assert report.rows_dropped_future == 1
    assert report.rows_applied == 1
    assert report.consistent


def test_future_rows_in_non_partial_file_is_inconsistent() -> None:
    """Regression: the origin is matched by source_id, not by data_file_path."""
    f = _file(partial_max=None)
    ds = _ds([(_A, 1, 5, _DF, 10), (_A, 2, 7, _DF, 10)], (f,))
    with pytest.raises(vis.PartialMaxInconsistencyError) as exc_info:
        vis.filter_visible(ds, snapshot_id=6, live_data_files={_A})
    assert exc_info.value.exit_code == ExitCode.DIVERGED
    assert f.path in exc_info.value.message
    assert "snapshot 7 > 6" in exc_info.value.message


def test_future_rows_of_other_file_do_not_blame_non_partial() -> None:
    clean = _file(10, 1, partial_max=None)
    partial = _file(11, 2, partial_max=7, data_file_path=_B)
    ds = _ds([(_A, 1, 5, _DF, 10), (_B, 3, 7, _DF, 11)], (clean, partial))
    t, report = vis.filter_visible(ds, snapshot_id=6, live_data_files={_A, _B})
    assert t.to_pylist()[0]["data_file_path"] == _A
    assert report.rows_dropped_future == 1


def test_dangling_drop_reports_data_file_ids(caplog: pytest.LogCaptureFixture) -> None:
    """Regression: dangling ids are data_file_id, not delete_file_id."""
    live = _file(10, 1)
    gone = _file(11, 42, data_file_path=None)
    ds = _ds(
        [
            (_A, 1, 5, _DF, 10),
            (_B, 2, 5, _DF, 11),  # delete file 11 -> data file 42
            (_B, 3, 5, _IN, 77),  # inlined: source_id is already the data_file_id
        ],
        (live, gone),
    )
    with caplog.at_level(logging.WARNING, logger="ducklake_iceberg.deletes.visibility"):
        t, report = vis.filter_visible(ds, snapshot_id=6, live_data_files={_A})
    assert t.num_rows == 1
    assert report.rows_dropped_dangling == 2
    assert report.dangling_data_file_ids == (42, 77)
    assert "dangling deletions" in caplog.text


def test_dangling_strict_raises() -> None:
    ds = _ds([(_B, 2, 5, _DF, 11)], (_file(11, 42, data_file_path=None),))
    with pytest.raises(vis.DanglingDeletesError) as exc_info:
        vis.filter_visible(
            ds, snapshot_id=6, live_data_files={_A}, policy=vis.DanglingPolicy.STRICT
        )
    assert exc_info.value.exit_code == ExitCode.DIVERGED
    assert "42" in exc_info.value.message


def test_dangling_error_truncates_ids() -> None:
    err = vis.DanglingDeletesError(12, 6, list(range(12)))
    assert "(12 total)" in err.message


def test_all_rows_future_skip_live_check() -> None:
    ds = _ds([(_A, 1, 9, _DF, 10)], (_file(partial_max=9),))
    t, report = vis.filter_visible(ds, snapshot_id=6, live_data_files=set())
    assert t.num_rows == 0
    assert report.rows_dropped_future == 1
    assert report.rows_dropped_dangling == 0
    assert report.rows_deduplicated == 0


def test_dedupe_keeps_smallest_snapshot(caplog: pytest.LogCaptureFixture) -> None:
    f = _file(partial_max=6)
    ds = _ds(
        [
            (_A, 2, 6, _DF, 10),
            (_A, 2, 5, _IN, 1),
            (_A, 1, 5, _DF, 10),
            (_B, 1, 5, _IN, 2),
        ],
        (f,),
    )
    with caplog.at_level(logging.INFO, logger="ducklake_iceberg.deletes.visibility"):
        t, report = vis.filter_visible(ds, snapshot_id=6, live_data_files={_A, _B})
    assert [
        (r["data_file_path"], r["pos"], r["snapshot_id"]) for r in t.to_pylist()
    ] == [
        (_A, 1, 5),
        (_A, 2, 5),
        (_B, 1, 5),
    ]
    assert report.rows_deduplicated == 1
    assert report.rows_applied == 3
    assert "Deduplicated" in caplog.text


def test_accounting_error_message() -> None:
    report = vis.VisibilityReport(
        snapshot_id=6,
        files_visible=0,
        files_row_filtered=0,
        rows_read=3,
        rows_applied=1,
        rows_dropped_future=0,
        rows_dropped_dangling=0,
        rows_deduplicated=0,
        dangling_data_file_ids=(),
    )
    assert not report.consistent
    err = vis.VisibilityAccountingError(report)
    assert "read=3" in err.message


def test_accounting_mismatch_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(t: pa.Table) -> tuple[pa.Table, int]:
        return t.slice(0, 0), 0  # loses rows without counting them

    monkeypatch.setattr(vis, "_drop_duplicate_positions", broken)
    ds = _ds([(_A, 1, 5, _DF, 10)], (_file(),))
    with pytest.raises(vis.VisibilityAccountingError):
        vis.filter_visible(ds, snapshot_id=6, live_data_files={_A})


def test_nonexistent_data_file_error() -> None:
    err = vis.NonexistentDataFileError(3, 99)
    assert err.exit_code == ExitCode.DIVERGED
    assert "99" in err.message


# -- delta_delete_files ----------------------------------------------
def test_delta_delete_files_classification() -> None:
    old = _file(1, 1, begin=3)
    new = _file(2, 2, begin=8)
    replaced = _file(3, 3, begin=3, partial_max=8)  # same begin, higher partial_max
    delta = vis.delta_delete_files(
        [old, new, replaced],
        previous_snapshot_id=5,
        published_delete_file_ids={1, 3, 4},
    )
    assert delta.added_or_changed == (new, replaced)
    assert delta.unchanged == (old,)
    assert delta.gone == (4,)


def test_delta_partial_max_not_beyond_previous_is_unchanged() -> None:
    f = dataclasses.replace(_file(1, 1, begin=3), partial_max=5)
    delta = vis.delta_delete_files(
        [f], previous_snapshot_id=5, published_delete_file_ids=set()
    )
    assert delta.unchanged == (f,)
    assert delta.gone == ()
