"""Reading deletes against a real DuckLake catalog.

Real Parquet delete files (partial and non-partial) and inlined deletions in
``ducklake_inlined_delete_<table_id>``; states that cannot be produced through
the API (puffin, encryption, tampered count) are forged via ``meta_exec``.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from iceduck.catalog.connection import DuckLakeConnection, open_catalog
from iceduck.catalog.reader import (
    TableRef,
    catalog_data_path,
    current_snapshot,
    resolve_table,
    snapshot_at,
)
from iceduck.deletes import reader as rd
from iceduck.deletes.visibility import DanglingPolicy, filter_visible
from iceduck.errors import ExitCode, IceduckError
from tests.integration.conftest import CatalogHarness

pytestmark = [pytest.mark.integration]

_FILE_PATH_ID = 2147483646
_POS_ID = 2147483645
_SNAPSHOT_ID = 2147483539


def _no_inlining(h: CatalogHarness) -> None:
    h.write("CALL lake.set_option('data_inlining_row_limit', 0)")


def _two_files(h: CatalogHarness) -> None:
    """Table ``main.t`` with two 10-row data files (ids 0-9 and 10-19)."""
    _no_inlining(h)
    h.write("CREATE TABLE lake.main.t (id INTEGER)")
    h.write("INSERT INTO lake.main.t SELECT range FROM range(10)")
    h.write("INSERT INTO lake.main.t SELECT range FROM range(10, 20)")


def _read_with(
    con: DuckLakeConnection,
    *,
    snapshot_id: int | None = None,
    include_inlined: bool = False,
) -> rd.DeleteSet:
    snap = (
        current_snapshot(con) if snapshot_id is None else snapshot_at(con, snapshot_id)
    )
    ident = resolve_table(con, TableRef("main", "t"), snap)
    return rd.read_deletes(
        con,
        ident.table_id,
        snap.snapshot_id,
        con.alias,
        "main",
        "t",
        catalog_data_path(con),
        ident.schema_path,
        ident.schema_path_is_relative,
        ident.table_path,
        ident.table_path_is_relative,
        include_inlined=include_inlined,
    )


def _read(
    h: CatalogHarness,
    *,
    snapshot_id: int | None = None,
    include_inlined: bool = False,
) -> rd.DeleteSet:
    with open_catalog(h.settings()) as con, con.read_transaction():
        return _read_with(con, snapshot_id=snapshot_id, include_inlined=include_inlined)


def _scalar(h: CatalogHarness, sql: str) -> object:
    with open_catalog(h.settings()) as con:
        return con.query_arrow(sql).to_pylist()[0]["v"]


def _live_data_files(h: CatalogHarness) -> set[str]:
    with open_catalog(h.settings()) as con:
        rows = con.query_arrow(
            "SELECT data_file FROM ducklake_list_files('lake', 't', schema => 'main')"
        ).to_pylist()
    return {r["data_file"] for r in rows}


def _data_file_id_of(h: CatalogHarness, row_id: int) -> int:
    """data_file_id of the file that contains the value ``id = row_id``."""
    offset = row_id // 10  # 10 rows per data file in ``_two_files``
    value = _scalar(
        h,
        "SELECT data_file_id AS v FROM lake_meta.ducklake_data_file "
        f"WHERE end_snapshot IS NULL ORDER BY data_file_id LIMIT 1 OFFSET {offset}",
    )
    assert isinstance(value, int)
    return value


def _table_id(h: CatalogHarness) -> int:
    value = _scalar(
        h, "SELECT table_id AS v FROM lake_meta.ducklake_table WHERE table_name = 't'"
    )
    assert isinstance(value, int)
    return value


# --- 2 files, DELETE of 1 row from the 2nd -----------------------------------
def test_single_delete_physical_position(catalog: CatalogHarness) -> None:
    _two_files(catalog)
    catalog.write("DELETE FROM lake.main.t WHERE id = 13")
    ds = _read(catalog)

    assert len(ds.files) == 1
    f = ds.files[0]
    assert f.file_format == "parquet"
    assert f.data_file_live is True
    assert f.has_snapshot_column is False  # non-partial file
    assert f.partial_max is None
    assert ds.positions.schema == rd.POSITIONS_SCHEMA
    rows = ds.positions.to_pylist()
    assert rows == [
        {
            "data_file_path": f.data_file_path,
            "pos": 3,  # physical position in the 2nd file, not the logical row id (13)
            "snapshot_id": f.begin_snapshot,  # non-partial: begin_snapshot
            "origin": rd.DeleteOrigin.DELETE_FILE.value,
            "source_id": f.delete_file_id,
        }
    ]
    assert ds.inlined is None
    assert not ds.warnings
    assert not ds.is_empty
    by_file = ds.positions_by_data_file()
    assert list(by_file) == [f.data_file_path]
    assert by_file[str(f.data_file_path)].num_rows == 1


# --- two consecutive DELETEs on the same data file ---------------------------
def test_partial_delete_file_and_temporal_filter(catalog: CatalogHarness) -> None:
    _two_files(catalog)
    catalog.write("DELETE FROM lake.main.t WHERE id = 3")
    catalog.write("DELETE FROM lake.main.t WHERE id = 5")
    ds = _read(catalog)

    assert len(ds.files) == 1
    f = ds.files[0]
    assert f.has_snapshot_column is True
    assert f.partial_max is not None
    assert f.partial_max > f.begin_snapshot
    snaps = sorted(ds.positions["snapshot_id"].to_pylist())
    assert len(set(snaps)) == 2
    assert snaps[1] == f.partial_max

    # Historical snapshot = begin_snapshot: the file is visible, but the row of
    # the second DELETE is from the future and gets discarded (filtered, not
    # blocked)
    historical = _read(catalog, snapshot_id=f.begin_snapshot)
    visible, report = filter_visible(
        historical,
        snapshot_id=f.begin_snapshot,
        live_data_files=_live_data_files(catalog),
    )
    assert visible["pos"].to_pylist() == [3]
    assert report.files_row_filtered == 1
    assert report.rows_dropped_future == 1
    assert report.consistent


# --- table without any delete ------------------------------------------------
def test_table_without_deletes_is_empty(catalog: CatalogHarness) -> None:
    _two_files(catalog)
    ds = _read(catalog)
    assert ds.is_empty
    assert ds.files == ()
    assert ds.positions.schema == rd.POSITIONS_SCHEMA
    assert ds.positions_by_data_file() == {}
    assert ds.inlined is None


# --- puffin, encryption, unknown format --------------------------------------
@pytest.mark.parametrize(
    ("forge", "error"),
    [
        ("format = 'puffin'", rd.PuffinDeleteFormatError),
        ("encryption_key = 'k'", rd.EncryptedDeleteFileError),
        ("format = 'orc'", rd.UnknownDeleteFormatError),
    ],
)
def test_unsupported_delete_files_block(
    catalog: CatalogHarness, forge: str, error: type[IceduckError]
) -> None:
    _two_files(catalog)
    catalog.write("DELETE FROM lake.main.t WHERE id = 3")
    catalog.meta_exec(f"UPDATE ducklake_delete_file SET {forge}")
    with pytest.raises(error) as exc_info:
        _read(catalog)
    assert exc_info.value.exit_code == ExitCode.BLOCKED
    if error is rd.PuffinDeleteFormatError:
        assert "Athena" in exc_info.value.message
        assert "--materialize-deletes" in (exc_info.value.hint or "")


# --- tampered path -> mismatch with ducklake_list_files ----------------------
def test_tampered_path_diverges(catalog: CatalogHarness) -> None:
    """A wrong composition (swapped table layer) does not match the function."""
    _two_files(catalog)
    catalog.write("DELETE FROM lake.main.t WHERE id = 3")
    with open_catalog(catalog.settings()) as con, con.read_transaction():
        snap = current_snapshot(con)
        ident = resolve_table(con, TableRef("main", "t"), snap)
        with pytest.raises(rd.DeleteFileEnumerationMismatchError) as exc_info:
            rd.read_deletes(
                con,
                ident.table_id,
                snap.snapshot_id,
                con.alias,
                "main",
                "t",
                catalog_data_path(con),
                ident.schema_path,
                ident.schema_path_is_relative,
                "other/",
                True,
            )
    assert exc_info.value.exit_code == ExitCode.DIVERGED
    assert "1 only in the catalog, 1 only in the function" in exc_info.value.message


# --- tampered delete_count ---------------------------------------------------
def test_tampered_delete_count(catalog: CatalogHarness) -> None:
    _two_files(catalog)
    catalog.write("DELETE FROM lake.main.t WHERE id = 3")
    catalog.meta_exec("UPDATE ducklake_delete_file SET delete_count = 7")
    with pytest.raises(rd.DeleteFileCountMismatchError) as exc_info:
        _read(catalog)
    assert exc_info.value.exit_code == ExitCode.DIVERGED


def test_zero_delete_count_warns_then_diverges(catalog: CatalogHarness) -> None:
    _two_files(catalog)
    catalog.write("DELETE FROM lake.main.t WHERE id = 3")
    catalog.meta_exec("UPDATE ducklake_delete_file SET delete_count = 0")
    with pytest.raises(rd.DeleteFileCountMismatchError):
        _read(catalog)


# --- data file outside the snapshot with a live delete -----------------------
def test_delete_on_data_file_not_live(catalog: CatalogHarness) -> None:
    _two_files(catalog)
    catalog.write("DELETE FROM lake.main.t WHERE id = 3")
    data_file_id = _data_file_id_of(catalog, 3)
    snapshot = _scalar(
        catalog, "SELECT max(snapshot_id) AS v FROM lake_meta.ducklake_snapshot"
    )
    catalog.meta_exec(
        f"UPDATE ducklake_data_file SET end_snapshot = {snapshot} "
        f"WHERE data_file_id = {data_file_id}"
    )
    with open_catalog(catalog.settings()) as con, con.read_transaction():
        snap = current_snapshot(con)
        ident = resolve_table(con, TableRef("main", "t"), snap)
        layers = (
            catalog_data_path(con),
            ident.schema_path,
            ident.schema_path_is_relative,
            ident.table_path,
            ident.table_path_is_relative,
        )
        files = rd.list_delete_files(con, ident.table_id, snap.snapshot_id, *layers)
        assert len(files) == 1
        f = files[0]
        assert f.data_file_live is False
        assert f.data_file_path is None
        assert f.data_record_count is None
        positions, has_snapshot = rd.read_delete_file(con, f)
        assert positions.num_rows == 1
        assert has_snapshot is False
        # the data file vanished from the snapshot: the cross-check catches the gap
        with pytest.raises(rd.DeleteFileEnumerationMismatchError):
            _read_with(con)


# --- inlined deletions -------------------------------------------------------
def test_inlined_deletes_block_by_default(catalog: CatalogHarness) -> None:
    _two_files(catalog)
    catalog.write("CALL lake.set_option('data_inlining_row_limit', 10)")
    catalog.write("DELETE FROM lake.main.t WHERE id = 15")
    with pytest.raises(rd.InlinedDeletesPendingError) as exc_info:
        _read(catalog)
    assert exc_info.value.exit_code == ExitCode.BLOCKED
    assert "iceduck checkpoint" in (exc_info.value.hint or "")


def test_inlined_deletes_on_file_without_delete_file(catalog: CatalogHarness) -> None:
    """Regression: inlined deletes on a data file WITHOUT a Parquet delete file stay."""
    _two_files(catalog)
    catalog.write("DELETE FROM lake.main.t WHERE id = 3")  # delete file on the 1st
    catalog.write("CALL lake.set_option('data_inlining_row_limit', 10)")
    catalog.write("DELETE FROM lake.main.t WHERE id = 15")  # inlined on the 2nd
    ds = _read(catalog, include_inlined=True)

    assert ds.inlined is not None
    assert ds.inlined.table_name == f"ducklake_inlined_delete_{_table_id(catalog)}"
    assert ds.inlined.rows_total == ds.inlined.rows_visible == 1
    inlined = [r for r in ds.positions.to_pylist() if r["origin"] == "inlined"]
    second = _data_file_id_of(catalog, 15)
    assert len(inlined) == 1
    assert inlined[0]["pos"] == 5
    assert inlined[0]["source_id"] == second
    live = _live_data_files(catalog)
    assert inlined[0]["data_file_path"] in live
    assert any("inlined deletions included" in w for w in ds.warnings)

    visible, report = filter_visible(
        ds, snapshot_id=ds.snapshot_id, live_data_files=live
    )
    assert report.rows_applied == 2
    assert sorted(visible["pos"].to_pylist()) == [3, 5]


def test_inlined_table_before_snapshot_is_not_visible(catalog: CatalogHarness) -> None:
    _two_files(catalog)
    catalog.write("CALL lake.set_option('data_inlining_row_limit', 10)")
    before = _scalar(
        catalog, "SELECT max(snapshot_id) AS v FROM lake_meta.ducklake_snapshot"
    )
    assert isinstance(before, int)
    catalog.write("DELETE FROM lake.main.t WHERE id = 15")
    ds = _read(catalog, snapshot_id=before)
    assert ds.is_empty
    assert ds.inlined is not None
    assert ds.inlined.rows_total == 1
    assert ds.inlined.rows_visible == 0


# --- partial+inlined duplicate; nonexistent file_id (orphan) -----------------
def test_duplicate_and_orphan_inlined(catalog: CatalogHarness) -> None:
    _two_files(catalog)
    catalog.write("DELETE FROM lake.main.t WHERE id = 3")
    catalog.write("CALL lake.set_option('data_inlining_row_limit', 10)")
    catalog.write("DELETE FROM lake.main.t WHERE id = 15")
    table = f"ducklake_inlined_delete_{_table_id(catalog)}"
    first = _data_file_id_of(catalog, 3)
    catalog.meta_exec(f"INSERT INTO {table} VALUES ({first}, 3, 1)")
    catalog.meta_exec(f"INSERT INTO {table} VALUES (999, 0, 1)")

    ds = _read(catalog, include_inlined=True)
    assert any("1 duplicate positions" in w for w in ds.warnings)
    orphan = [r for r in ds.positions.to_pylist() if r["source_id"] == 999]
    assert orphan[0]["data_file_path"] == f"{rd.ORPHAN_DATA_FILE_PREFIX}999"

    live = _live_data_files(catalog)
    visible, report = filter_visible(
        ds, snapshot_id=ds.snapshot_id, live_data_files=live
    )
    assert report.rows_read == 4
    assert report.rows_deduplicated == 1
    assert report.rows_dropped_dangling == 1
    assert report.dangling_data_file_ids == (999,)
    assert report.rows_applied == 2
    assert visible.num_rows == 2
    with pytest.raises(IceduckError, match="999"):
        filter_visible(
            ds,
            snapshot_id=ds.snapshot_id,
            live_data_files=live,
            policy=DanglingPolicy.STRICT,
        )


# --- file validations: forged Parquet ----------------------------------------
def _write_delete_parquet(
    path: Path,
    rows: list[tuple[str | None, int | None]],
    *,
    file_path_id: int = _FILE_PATH_ID,
    snapshots: list[int] | None = None,
) -> None:
    fields = [
        pa.field(
            "file_path", pa.string(), metadata={"PARQUET:field_id": str(file_path_id)}
        ),
        pa.field("pos", pa.int64(), metadata={"PARQUET:field_id": str(_POS_ID)}),
    ]
    columns: list[list[object]] = [[r[0] for r in rows], [r[1] for r in rows]]
    if snapshots is not None:
        fields.append(
            pa.field(
                "_ducklake_internal_snapshot_id",
                pa.int64(),
                metadata={"PARQUET:field_id": str(_SNAPSHOT_ID)},
            )
        )
        columns.append(list(snapshots))
    schema = pa.schema(fields)
    pq.write_table(
        pa.Table.from_arrays([pa.array(c) for c in columns], schema=schema), path
    )


def _forged(
    path: Path, *, delete_count: int, record_count: int = 10
) -> rd.DuckLakeDeleteFile:
    return rd.DuckLakeDeleteFile(
        delete_file_id=7,
        data_file_id=1,
        path=str(path),
        file_format="parquet",
        delete_count=delete_count,
        file_size_bytes=1,
        footer_size=None,
        row_group_count=None,
        begin_snapshot=4,
        end_snapshot=None,
        partial_max=None,
        encryption_key=None,
        data_file_path="/d/a.parquet",
        data_record_count=record_count,
        data_file_live=True,
        has_snapshot_column=False,
    )


def test_read_delete_file_validations(catalog: CatalogHarness, tmp_path: Path) -> None:
    good = tmp_path / "good.parquet"
    _write_delete_parquet(
        good, [("/d/a.parquet", 1), ("/d/a.parquet", 2)], snapshots=[4, 6]
    )
    bad_id = tmp_path / "bad-id.parquet"
    _write_delete_parquet(bad_id, [("/d/a.parquet", 1)], file_path_id=1)
    null_path = tmp_path / "null-path.parquet"
    _write_delete_parquet(null_path, [(None, 1)])
    null_pos = tmp_path / "null-pos.parquet"
    _write_delete_parquet(null_pos, [("/d/a.parquet", None)])
    negative = tmp_path / "negative.parquet"
    _write_delete_parquet(negative, [("/d/a.parquet", -1)])
    wrong_path = tmp_path / "wrong-path.parquet"
    _write_delete_parquet(wrong_path, [("/d/other.parquet", 1)])

    with open_catalog(catalog.settings()) as con:
        positions, has_snapshot = rd.read_delete_file(
            con, _forged(good, delete_count=2)
        )
        assert has_snapshot is True
        assert positions["snapshot_id"].to_pylist() == [4, 6]
        assert positions["source_id"].to_pylist() == [7, 7]

        with pytest.raises(rd.InvalidDeletePositionError) as pos_err:
            rd.read_delete_file(con, _forged(good, delete_count=2, record_count=2))
        assert pos_err.value.exit_code == ExitCode.ERROR
        with pytest.raises(rd.InvalidDeletePositionError):
            rd.read_delete_file(con, _forged(negative, delete_count=1))

        with pytest.raises(rd.DeleteFileSchemaError) as schema_err:
            rd.read_delete_file(con, _forged(bad_id, delete_count=1))
        assert "file_path" in schema_err.value.message
        assert schema_err.value.exit_code == ExitCode.ERROR

        with pytest.raises(IceduckError, match="null file_path"):
            rd.read_delete_file(con, _forged(null_path, delete_count=1))
        with pytest.raises(IceduckError, match="null pos"):
            rd.read_delete_file(con, _forged(null_pos, delete_count=1))

        # file_path differs from the catalog: warning, the catalog wins
        f = _forged(wrong_path, delete_count=1)
        raw, _ = rd.read_delete_file(con, f)
        warnings: list[str] = []
        projected = rd._delete_file_positions(f, raw, {}, warnings)
        assert projected["data_file_path"].to_pylist() == ["/d/a.parquet"]
        assert len(warnings) == 1
        assert "differs from the catalog" in warnings[0]

        # data file outside the catalog: the Parquet file_path is the last resort
        quiet: list[str] = []
        gone = rd._delete_file_positions(_replace_not_live(f), raw, {}, quiet)
        assert gone["data_file_path"].to_pylist() == ["/d/other.parquet"]
        assert quiet == []


def _replace_not_live(f: rd.DuckLakeDeleteFile) -> rd.DuckLakeDeleteFile:
    import dataclasses

    return dataclasses.replace(
        f, data_file_live=False, data_file_path=None, data_record_count=None
    )


# --- composed path -----------------------------------------------------------
@pytest.mark.parametrize(
    ("layers", "path", "relative", "expected"),
    [
        (("/d/", "main/", True, "t/", True), "f.parquet", True, "/d/main/t/f.parquet"),
        (("/d/", "/abs/", False, "t/", True), "f.parquet", True, "/abs/t/f.parquet"),
        (("/d/", "main/", True, "/tab/", False), "f.parquet", True, "/tab/f.parquet"),
        (("/d/", None, True, None, True), "f.parquet", True, "/d/f.parquet"),
        (
            ("/d/", "main/", True, "t/", True),
            "s3://b/f.parquet",
            False,
            "s3://b/f.parquet",
        ),
    ],
)
def test_compose_delete_path(
    layers: tuple[str, str | None, bool, str | None, bool],
    path: str,
    relative: bool,
    expected: str,
) -> None:
    assert rd._compose_delete_path(*layers, path, relative) == expected


def test_assert_supported_accepts_parquet() -> None:
    rd.assert_supported([_forged(Path("/x"), delete_count=1)])
    rd.assert_supported([])
