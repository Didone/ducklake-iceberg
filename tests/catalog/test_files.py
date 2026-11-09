"""Pure tests for file enumeration (no catalog).

Covers the pure logic: path composition, canonicalization, cross-check and
divergences, reconciliation and blockers. Scenarios that need a SQLite + MinIO
catalog (empty_reason, current/historic snapshot, 1000 files) live in the
integration matrix.
"""

from __future__ import annotations

import dataclasses

import pytest

from ducklake_iceberg.catalog.files import (
    FileEnumerationError,
    FileSetDivergence,
    LiveDataFile,
    PathLayers,
    attach_delete_files,
    canonical,
    check_blockers,
    compose_path,
    reconcile_files,
)
from ducklake_iceberg.doctor.model import Severity


def _layers(
    *,
    data_path: str = "s3://bucket/ducklake/",
    schema_path: str | None = None,
    schema_rel: bool = True,
    table_path: str | None = "sales/orders",
    table_rel: bool = True,
) -> PathLayers:
    return PathLayers(
        data_path=data_path,
        schema_path=schema_path,
        schema_path_is_relative=schema_rel,
        table_path=table_path,
        table_path_is_relative=table_rel,
    )


def _lf(
    path: str, *, delete: str | None = None, enc: str | None = None
) -> dict[str, object]:
    return {
        "data_file": path,
        "data_file_size_bytes": 100,
        "data_file_footer_size": 10,
        "data_file_encryption_key": enc,
        "delete_file": delete,
        "delete_file_size_bytes": None,
        "delete_file_footer_size": None,
        "delete_file_encryption_key": None,
    }


def _df(
    data_file_id: int,
    path: str,
    *,
    is_relative: bool = True,
    fmt: str = "parquet",
    record_count: int | None = 3,
    partition_id: int | None = None,
    partial_max: int | None = None,
    mapping_id: int | None = None,
    encryption_key: str | None = None,
    begin_snapshot: int = 2,
) -> dict[str, object]:
    return {
        "data_file_id": data_file_id,
        "path": path,
        "path_is_relative": is_relative,
        "file_format": fmt,
        "record_count": record_count,
        "file_size_bytes": 100,
        "footer_size": 10,
        "row_id_start": None,
        "partition_id": partition_id,
        "encryption_key": encryption_key,
        "mapping_id": mapping_id,
        "partial_max": partial_max,
        "row_group_count": 1,
        "file_order": 0,
        "begin_snapshot": begin_snapshot,
    }


# -- compose_path ----------------------------------------------------


def test_compose_relative_file() -> None:
    layers = _layers()
    assert (
        compose_path(layers, "a.parquet", True)
        == "s3://bucket/ducklake/sales/orders/a.parquet"
    )


def test_compose_trailing_slash_equivalent() -> None:
    # data_path with and without a trailing slash produce the same path
    with_slash = compose_path(_layers(data_path="s3://b/d/"), "a.parquet", True)
    without = compose_path(_layers(data_path="s3://b/d"), "a.parquet", True)
    assert with_slash == without


def test_compose_absolute_file_no_prefix() -> None:
    # path_is_relative=false on the file -> published without a prefix
    layers = _layers()
    assert compose_path(layers, "s3://other/x.parquet", False) == "s3://other/x.parquet"


def test_compose_absolute_table_ends_composition() -> None:
    # path_is_relative=false on the table ends the composition
    layers = _layers(data_path="s3://b/d/", table_path="s3://abs/tbl", table_rel=False)
    assert compose_path(layers, "a.parquet", True) == "s3://abs/tbl/a.parquet"


def test_compose_schema_relative_from_global_data_path() -> None:
    # relative schema resolves from the global data_path
    layers = _layers(
        data_path="s3://b/d/", schema_path="sales", schema_rel=True, table_path=None
    )
    assert compose_path(layers, "a.parquet", True) == "s3://b/d/sales/a.parquet"


def test_compose_absolute_schema_path() -> None:
    layers = _layers(
        data_path="s3://b/d/",
        schema_path="s3://sch/abs",
        schema_rel=False,
        table_path="tbl",
    )
    assert compose_path(layers, "a.parquet", True) == "s3://sch/abs/tbl/a.parquet"


def test_compose_null_schema_path_skips_layer() -> None:
    layers = _layers(data_path="s3://b/d/", schema_path=None, table_path="tbl")
    assert compose_path(layers, "a.parquet", True) == "s3://b/d/tbl/a.parquet"


# -- canonical -------------------------------------------------------


def test_canonical_collapses_double_slash() -> None:
    # double slashes
    assert canonical("s3://b//ducklake///a.parquet") == "s3://b/ducklake/a.parquet"


def test_canonical_preserves_percent_encoding() -> None:
    # %2F != / - must not be unified
    assert canonical("s3://b/a%2Fx.parquet") == "s3://b/a%2Fx.parquet"


def test_canonical_preserves_case() -> None:
    assert canonical("s3://b/Data/A.parquet") == "s3://b/Data/A.parquet"


def test_canonical_does_not_resolve_dotdot() -> None:
    assert ".." in canonical("s3://b/x/../y.parquet")


def test_canonical_strips_leading_dot_segment() -> None:
    assert canonical("s3://b/./a.parquet") == "s3://b/a.parquet"


def test_canonical_lowercases_scheme() -> None:
    assert canonical("S3://b/a.parquet") == "s3://b/a.parquet"


# -- happy-path reconciliation --------------------------------------------


def test_reconcile_happy_path() -> None:
    # path from list_files; complementary fields from data_file
    layers = _layers()
    path = "s3://bucket/ducklake/sales/orders/a.parquet"
    lf = [_lf(path)]
    df = [_df(881, "a.parquet", record_count=3, partition_id=7, mapping_id=1)]
    files = reconcile_files(lf, df, layers, table="sales.orders", snapshot_id=5)
    assert len(files) == 1
    f = files[0]
    assert f.path == path  # path comes from list_files
    assert f.data_file_id == 881
    assert f.record_count == 3
    assert f.partition_id == 7
    assert f.mapping_id == 1


# -- divergences ----------------------------------------------------


def test_divergence_only_in_data_file() -> None:
    # loose SQL -> we would export too many
    layers = _layers()
    base = "s3://bucket/ducklake/sales/orders/a.parquet"
    extra = "s3://bucket/ducklake/sales/orders/b.parquet"
    lf = [_lf(base)]
    df = [_df(1, "a.parquet"), _df(2, "b.parquet")]
    with pytest.raises(FileSetDivergence) as exc:
        reconcile_files(lf, df, layers, table="sales.orders", snapshot_id=5)
    assert exc.value.exit_code == 3
    assert len(exc.value.only_in_data_file) == 1
    assert exc.value.only_in_data_file[0][0] == 2
    assert extra in exc.value.render()
    assert "too many" in exc.value.render()


def test_divergence_only_in_list_files() -> None:
    # overly restrictive interval -> we would export too few
    layers = _layers()
    a = "s3://bucket/ducklake/sales/orders/a.parquet"
    b = "s3://bucket/ducklake/sales/orders/b.parquet"
    lf = [_lf(a), _lf(b)]
    df = [_df(1, "a.parquet")]
    with pytest.raises(FileSetDivergence) as exc:
        reconcile_files(lf, df, layers, table="sales.orders", snapshot_id=5)
    assert len(exc.value.only_in_list_files) == 1
    assert b in exc.value.only_in_list_files
    assert "too few" in exc.value.render()


def test_divergence_wrong_data_path_hint() -> None:
    # tampered data_path -> equal sizes, empty intersection
    good = _layers(data_path="s3://bucket/ducklake/")
    bad = _layers(data_path="s3://other/prefix/")
    a = "s3://bucket/ducklake/sales/orders/a.parquet"
    lf = [_lf(a)]
    df = [_df(1, "a.parquet")]
    with pytest.raises(FileSetDivergence) as exc:
        reconcile_files(lf, df, bad, table="sales.orders", snapshot_id=5)
    render = exc.value.render()
    assert "path composition" in render
    # sanity: with the correct data_path there is no divergence
    assert reconcile_files(lf, df, good, table="t", snapshot_id=5)


def test_duplicate_in_list_files() -> None:
    # repeated path in list_files
    layers = _layers()
    a = "s3://bucket/ducklake/sales/orders/a.parquet"
    lf = [_lf(a), _lf(a)]
    df = [_df(1, "a.parquet")]
    with pytest.raises(FileSetDivergence) as exc:
        reconcile_files(lf, df, layers, table="t", snapshot_id=5)
    assert "repeated" in exc.value.render()


def test_duplicate_in_data_file() -> None:
    # repeated path in data_file
    layers = _layers()
    a = "s3://bucket/ducklake/sales/orders/a.parquet"
    lf = [_lf(a)]
    df = [_df(1, "a.parquet"), _df(2, "a.parquet")]
    with pytest.raises(FileSetDivergence) as exc:
        reconcile_files(lf, df, layers, table="t", snapshot_id=5)
    assert "repeated" in exc.value.render()


def test_percent_encoding_divergence_detected() -> None:
    # %2F on one side and / on the other -> divergence detected
    layers = _layers(table_path="sales/orders")
    lf = [_lf("s3://bucket/ducklake/sales/orders/a%2Fx.parquet")]
    df = [_df(1, "a/x.parquet")]
    with pytest.raises(FileSetDivergence):
        reconcile_files(lf, df, layers, table="t", snapshot_id=5)


def test_null_record_count_is_error() -> None:
    layers = _layers()
    a = "s3://bucket/ducklake/sales/orders/a.parquet"
    lf = [_lf(a)]
    df = [_df(1, "a.parquet", record_count=None)]
    with pytest.raises(FileEnumerationError):
        reconcile_files(lf, df, layers, table="t", snapshot_id=5)


def test_absolute_file_path_no_prefix_reconciles() -> None:
    # integrated: a file with an absolute path matches list_files
    layers = _layers()
    abs_path = "s3://outro/dir/a.parquet"
    lf = [_lf(abs_path)]
    df = [_df(1, abs_path, is_relative=False)]
    files = reconcile_files(lf, df, layers, table="t", snapshot_id=5)
    assert files[0].path == abs_path


# -- blockers ----------------------------------------------------


def _live(
    *,
    fmt: str = "parquet",
    mapping_id: int | None = None,
    partial_max: int | None = None,
    begin_snapshot: int = 2,
) -> LiveDataFile:
    return LiveDataFile(
        data_file_id=1,
        path="s3://b/a.parquet",
        canonical_path="s3://b/a.parquet",
        file_size_in_bytes=100,
        footer_size=10,
        record_count=3,
        file_format=fmt,
        partition_id=None,
        partial_max=partial_max,
        mapping_id=mapping_id,
        row_id_start=None,
        row_group_count=1,
        begin_snapshot=begin_snapshot,
        file_order=0,
    )


def test_blocker_encryption_key() -> None:
    # encrypted files
    findings = check_blockers([], [_lf("s3://b/a", enc="k")], [], 5)
    assert any("encrypted" in f.detail for f in findings)
    assert all(f.severity is Severity.BLOCKER for f in findings)


def test_parquet_delete_file_is_not_a_blocker() -> None:
    # parquet delete files are rewritten as Iceberg position deletes
    findings = check_blockers([], [_lf("s3://b/a", delete="s3://b/d")], [], 5)
    assert findings == []


def _with_delete(fmt: str) -> LiveDataFile:
    from ducklake_iceberg.catalog.files import LiveDeleteFile

    return dataclasses.replace(
        _live(),
        delete_file=LiveDeleteFile(
            delete_file_id=9,
            path="s3://b/d.puffin",
            canonical_path="s3://b/d.puffin",
            file_size_in_bytes=10,
            delete_count=1,
            file_format=fmt,
        ),
    )


def test_blocker_puffin_delete_file() -> None:
    findings = check_blockers([_with_delete("puffin")], [], [], 5)
    assert any("puffin" in f.detail for f in findings)
    assert any("ducklake_rewrite_data_files" in f.remediation for f in findings)
    assert check_blockers([_with_delete("parquet")], [], [], 5) == []


def test_blocker_non_parquet_format() -> None:
    findings = check_blockers([_live(fmt="orc")], [], [], 5)
    assert any("unsupported format" in f.detail for f in findings)


def test_blocker_divergent_mapping_id() -> None:
    # divergent mapping_id
    files = [_live(mapping_id=1), _live(mapping_id=2)]
    findings = check_blockers(files, [], [], 5)
    assert any("mapping_id" in f.detail for f in findings)


def test_single_mapping_id_ok() -> None:
    files = [_live(mapping_id=1), _live(mapping_id=1)]
    findings = check_blockers(files, [], [], 5)
    assert not any("mapping_id" in f.detail for f in findings)


def test_partial_max_equal_snapshot_ok() -> None:
    # partial_max equal to the snapshot is fine
    findings = check_blockers([_live(partial_max=5)], [], [], 5)
    assert not any("partial" in f.detail for f in findings)


def test_partial_max_beyond_snapshot_blocks() -> None:
    # partial_max beyond the snapshot blocks
    findings = check_blockers([_live(partial_max=6)], [], [], 5)
    assert any("partial" in f.detail for f in findings)


def test_no_blockers_clean() -> None:
    findings = check_blockers([_live()], [_lf("s3://b/a")], [], 5)
    assert findings == []


# -- delete file pairing ---------------------------------------


def _del(
    delete_file_id: int, data_file_id: int, path: str, *, ended: bool = False
) -> dict[str, object]:
    return {
        "delete_file_id": delete_file_id,
        "data_file_id": data_file_id,
        "path": path,
        "path_is_relative": True,
        "format": "parquet",
        "delete_count": 1,
        "file_size_bytes": 50,
        "encryption_key": None,
        "data_file_ended": ended,
    }


def _paired(delete_rows: list[dict[str, object]]) -> tuple[LiveDataFile, ...]:
    layers = _layers()
    path = "s3://bucket/ducklake/sales/orders/a.parquet"
    deleted = "s3://bucket/ducklake/sales/orders/a-delete.parquet"
    lf = [_lf(path, delete=deleted)]
    files = reconcile_files(
        lf, [_df(1, "a.parquet")], layers, table="sales.orders", snapshot_id=5
    )
    return attach_delete_files(
        files, lf, delete_rows, layers, table="sales.orders", snapshot_id=5
    )


def test_dangling_delete_file_of_ended_data_file_is_ignored() -> None:
    # data file 2 ended by a write that deleted all of its rows;
    # the previous delete file stays open and list_files omits it
    files = _paired(
        [_del(10, 1, "a-delete.parquet"), _del(11, 2, "b-delete.parquet", ended=True)]
    )
    assert files[0].delete_file is not None
    assert files[0].delete_file.delete_file_id == 10


def test_delete_file_of_unknown_data_file_is_divergence() -> None:
    with pytest.raises(FileSetDivergence) as exc:
        _paired([_del(10, 1, "a-delete.parquet"), _del(11, 2, "b-delete.parquet")])
    assert exc.value.only_in_data_file == ((11, "b-delete.parquet"),)
