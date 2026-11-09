"""Unit tests for the classification logic of the doctor rules.

The `classify` functions receive a hand-built `pa.Table`, with no catalog.
That is how cases whose DuckDB fixture syntax is uncertain stay covered
deterministically.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from ducklake_iceberg.doctor.model import (
    Capability,
    DoctorContext,
    DoctorTarget,
    Severity,
)
from ducklake_iceberg.doctor.rules import (
    calendar_transform,
    delete_parquet,
    delete_puffin,
    encryption_key,
    mapping_divergence,
    parquet_compression,
    partial_files,
    unsupported_type,
)


def _target(snapshot_id: int = 5) -> DoctorTarget:
    return DoctorTarget(
        catalog="lake",
        metadata_catalog="lake_meta",
        schema_name="sales",
        table_name="orders",
        table_id=7,
        snapshot_id=snapshot_id,
        schema_version=1,
    )


def _ctx(
    *, snapshot_id: int = 5, capabilities: frozenset[Capability] = frozenset()
) -> DoctorContext:
    return DoctorContext(
        con=None,  # type: ignore[arg-type]
        target=_target(snapshot_id),
        capabilities=capabilities,
    )


def _table(rows: list[dict[str, Any]], schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=schema)


# --------------------------------------------------------------------------
# partial-files
# --------------------------------------------------------------------------
_PARTIAL_SCHEMA = pa.schema(
    [
        ("kind", pa.string()),
        ("file_id", pa.int64()),
        ("path", pa.string()),
        ("path_is_relative", pa.bool_()),
        ("partial_max", pa.int64()),
        ("begin_snapshot", pa.int64()),
        ("row_count", pa.int64()),
        ("beyond_target", pa.bool_()),
    ]
)


def _partial_row(**kw: Any) -> dict[str, Any]:
    base = {
        "kind": "data",
        "file_id": 1,
        "path": "s3://b/x.parquet",
        "path_is_relative": False,
        "partial_max": 7,
        "begin_snapshot": 2,
        "row_count": 3,
        "beyond_target": True,
    }
    base.update(kw)
    return base


def test_partial_beyond_target_blocks() -> None:
    table = _table([_partial_row(partial_max=7, beyond_target=True)], _PARTIAL_SCHEMA)
    findings = partial_files.classify(table, _ctx(snapshot_id=5), "partial-files")
    assert len(findings) == 1
    assert findings[0].severity is Severity.BLOCKER
    assert findings[0].data["partial_max"] == 7


def test_partial_within_target_is_info() -> None:
    table = _table([_partial_row(partial_max=3, beyond_target=False)], _PARTIAL_SCHEMA)
    findings = partial_files.classify(table, _ctx(snapshot_id=5), "partial-files")
    assert len(findings) == 1
    assert findings[0].severity is Severity.INFO


def test_partial_delete_kind() -> None:
    table = _table([_partial_row(kind="delete", beyond_target=True)], _PARTIAL_SCHEMA)
    findings = partial_files.classify(table, _ctx(snapshot_id=5), "partial-files")
    assert findings[0].data["kind"] == "delete"


def test_partial_truncates_beyond_max_findings() -> None:
    rows = [_partial_row(file_id=i, beyond_target=True) for i in range(25)]
    table = _table(rows, _PARTIAL_SCHEMA)
    findings = partial_files.classify(table, _ctx(snapshot_id=5), "partial-files")
    blockers = [f for f in findings if f.severity is Severity.BLOCKER]
    assert len(blockers) == 21  # 20 individual + 1 aggregate
    assert "and 5 more" in blockers[-1].detail


# --------------------------------------------------------------------------
# delete-parquet
# --------------------------------------------------------------------------
_DP_SCHEMA = pa.schema(
    [
        ("delete_file_id", pa.int64()),
        ("data_file_id", pa.int64()),
        ("path", pa.string()),
        ("path_is_relative", pa.bool_()),
        ("format", pa.string()),
        ("delete_count", pa.int64()),
        ("file_size_bytes", pa.int64()),
        ("begin_snapshot", pa.int64()),
    ]
)


def _dp_row(fmt: str = "parquet", deletes: int = 37) -> dict[str, Any]:
    return {
        "delete_file_id": 1,
        "data_file_id": 2,
        "path": "s3://b/d.parquet",
        "path_is_relative": False,
        "format": fmt,
        "delete_count": deletes,
        "file_size_bytes": 100,
        "begin_snapshot": 2,
    }


def test_delete_parquet_blocks_without_capability() -> None:
    table = _table([_dp_row()], _DP_SCHEMA)
    findings = delete_parquet.classify(table, _ctx(), "delete-parquet")
    assert findings[0].severity is Severity.BLOCKER
    assert findings[0].data["delete_count"] == 37


def test_delete_parquet_degraded_with_capability() -> None:
    table = _table([_dp_row()], _DP_SCHEMA)
    ctx = _ctx(capabilities=frozenset({Capability.POSITION_DELETES}))
    findings = delete_parquet.classify(table, ctx, "delete-parquet")
    assert findings[0].severity is Severity.DEGRADED


def test_delete_unknown_format_always_blocks() -> None:
    table = _table([_dp_row(fmt="orc")], _DP_SCHEMA)
    ctx = _ctx(capabilities=frozenset({Capability.POSITION_DELETES}))
    findings = delete_parquet.classify(table, ctx, "delete-parquet")
    assert findings[0].severity is Severity.BLOCKER
    assert "unsupported" in findings[0].detail


def test_delete_parquet_empty_passes() -> None:
    table = _table([], _DP_SCHEMA)
    assert delete_parquet.classify(table, _ctx(), "delete-parquet") == []


# --------------------------------------------------------------------------
# delete-puffin
# --------------------------------------------------------------------------
_PUFFIN_SCHEMA = pa.schema(
    [
        ("delete_file_id", pa.int64()),
        ("data_file_id", pa.int64()),
        ("path", pa.string()),
        ("path_is_relative", pa.bool_()),
        ("delete_count", pa.int64()),
        ("file_size_bytes", pa.int64()),
        ("footer_size", pa.int64()),
        ("begin_snapshot", pa.int64()),
    ]
)
_OPTION_SCHEMA = pa.schema([("value", pa.string()), ("scope", pa.string())])


def test_puffin_file_blocks() -> None:
    puffin = _table(
        [
            {
                "delete_file_id": 1,
                "data_file_id": 2,
                "path": "s3://b/d.puffin",
                "path_is_relative": False,
                "delete_count": 37,
                "file_size_bytes": 10,
                "footer_size": 4,
                "begin_snapshot": 2,
            }
        ],
        _PUFFIN_SCHEMA,
    )
    option = _table([], _OPTION_SCHEMA)
    findings = delete_puffin.classify(puffin, option, _ctx(), "delete-puffin")
    assert findings[0].severity is Severity.BLOCKER
    assert findings[0].data["delete_count"] == 37


def test_puffin_option_only_is_info() -> None:
    puffin = _table([], _PUFFIN_SCHEMA)
    option = _table([{"value": "true", "scope": None}], _OPTION_SCHEMA)
    findings = delete_puffin.classify(puffin, option, _ctx(), "delete-puffin")
    assert len(findings) == 1
    assert findings[0].severity is Severity.INFO


def test_puffin_clean_passes() -> None:
    puffin = _table([], _PUFFIN_SCHEMA)
    option = _table([], _OPTION_SCHEMA)
    assert delete_puffin.classify(puffin, option, _ctx(), "delete-puffin") == []


# --------------------------------------------------------------------------
# unsupported-type
# --------------------------------------------------------------------------
_COL_SCHEMA = pa.schema(
    [
        ("column_id", pa.int64()),
        ("qualified_name", pa.string()),
        ("column_type", pa.string()),
        ("depth", pa.int64()),
    ]
)


def _col_row(cid: int, name: str, ctype: str, depth: int = 0) -> dict[str, Any]:
    return {
        "column_id": cid,
        "qualified_name": name,
        "column_type": ctype,
        "depth": depth,
    }


def test_unsupported_blocks_each_bad_type() -> None:
    table = _table(
        [
            _col_row(1, "id", "int32"),
            _col_row(2, "qty", "uint32"),
            _col_row(3, "created_at", "timestamp_ms"),
            _col_row(5, "measurement.duration", "interval", depth=1),
        ],
        _COL_SCHEMA,
    )
    findings = unsupported_type.classify(table, _ctx(), "unsupported-type")
    blockers = [f for f in findings if f.severity is Severity.BLOCKER]
    assert len(blockers) == 3
    assert any("measurement.duration" in f.subject for f in blockers)


def test_unsupported_decimal_param_blocks() -> None:
    table = _table([_col_row(1, "v", "decimal(40,2)")], _COL_SCHEMA)
    findings = unsupported_type.classify(table, _ctx(), "unsupported-type")
    assert len(findings) == 1
    assert findings[0].severity is Severity.BLOCKER


def test_unsupported_nested_no_finding() -> None:
    table = _table(
        [
            _col_row(1, "tags", "list"),
            _col_row(2, "attrs", "map"),
            _col_row(3, "measurement", "struct"),
        ],
        _COL_SCHEMA,
    )
    assert unsupported_type.classify(table, _ctx(), "unsupported-type") == []


def test_unsupported_timestamp_info() -> None:
    table = _table([_col_row(1, "created_at", "timestamp")], _COL_SCHEMA)
    findings = unsupported_type.classify(table, _ctx(), "unsupported-type")
    assert len(findings) == 1
    assert findings[0].severity is Severity.INFO
    assert "millisecond" in findings[0].detail


def test_unsupported_null_column_type_blocks() -> None:
    table = _table([_col_row(1, "x", None)], _COL_SCHEMA)  # type: ignore[arg-type]
    findings = unsupported_type.classify(table, _ctx(), "unsupported-type")
    assert findings[0].severity is Severity.BLOCKER


def test_unsupported_depth_guard() -> None:
    table = _table([_col_row(1, "deep", "int32", depth=65)], _COL_SCHEMA)
    with pytest.raises(unsupported_type.DepthExceededError):
        unsupported_type.classify(table, _ctx(), "unsupported-type")


# --------------------------------------------------------------------------
# calendar-transform
# --------------------------------------------------------------------------
_CT_SCHEMA = pa.schema(
    [
        ("partition_id", pa.int64()),
        ("is_current", pa.bool_()),
        ("partition_key_index", pa.int64()),
        ("column_id", pa.int64()),
        ("column_name", pa.string()),
        ("column_type", pa.string()),
        ("transform", pa.string()),
    ]
)


def _ct_row(transform: str, *, is_current: bool = True) -> dict[str, Any]:
    return {
        "partition_id": 1,
        "is_current": is_current,
        "partition_key_index": 0,
        "column_id": 3,
        "column_name": "dt",
        "column_type": "date",
        "transform": transform,
    }


def test_calendar_current_degraded() -> None:
    table = _table([_ct_row("year")], _CT_SCHEMA)
    findings = calendar_transform.classify(table, _ctx(), "calendar-transform")
    assert len(findings) == 1
    assert findings[0].severity is Severity.DEGRADED


def test_calendar_identity_no_finding() -> None:
    table = _table([_ct_row("identity")], _CT_SCHEMA)
    assert calendar_transform.classify(table, _ctx(), "calendar-transform") == []


def test_calendar_historical_info() -> None:
    table = _table([_ct_row("day", is_current=False)], _CT_SCHEMA)
    findings = calendar_transform.classify(table, _ctx(), "calendar-transform")
    assert len(findings) == 1
    assert findings[0].severity is Severity.INFO


def test_calendar_unknown_transform_degraded() -> None:
    table = _table([_ct_row("truncate(10)")], _CT_SCHEMA)
    findings = calendar_transform.classify(table, _ctx(), "calendar-transform")
    assert findings[0].severity is Severity.DEGRADED
    assert "cannot be mapped" in findings[0].detail


def test_calendar_bucket_info() -> None:
    table = _table([_ct_row("bucket(16)")], _CT_SCHEMA)
    findings = calendar_transform.classify(table, _ctx(), "calendar-transform")
    assert findings[0].severity is Severity.INFO
    table2 = _table([_ct_row("bucket[16]")], _CT_SCHEMA)
    assert (
        calendar_transform.classify(table2, _ctx(), "calendar-transform")[0].severity
        is Severity.INFO
    )


def test_calendar_epoch_maps_cleanly() -> None:
    table = _table([_ct_row("epoch_day")], _CT_SCHEMA)
    assert calendar_transform.classify(table, _ctx(), "calendar-transform") == []


# --------------------------------------------------------------------------
# parquet-compression
# --------------------------------------------------------------------------
_COMPRESSION_SCHEMA = pa.schema([("value", pa.string())])


@pytest.mark.parametrize("configured", [None, "snappy", "gzip"])
def test_non_zstd_compression_is_degraded(configured: str | None) -> None:
    rows = [] if configured is None else [{"value": configured}]
    table = _table(rows, _COMPRESSION_SCHEMA)
    findings = parquet_compression.classify(table, _ctx(), "parquet-compression")
    assert len(findings) == 1
    assert findings[0].severity is Severity.DEGRADED
    assert findings[0].data["configured_compression"] == configured


@pytest.mark.parametrize("configured", ["zstd", "ZSTD"])
def test_zstd_compression_passes(configured: str) -> None:
    table = _table([{"value": configured}], _COMPRESSION_SCHEMA)
    assert parquet_compression.classify(table, _ctx(), "parquet-compression") == []


# --------------------------------------------------------------------------
# mapping-divergence
# --------------------------------------------------------------------------
_DIST_SCHEMA = pa.schema(
    [
        ("live_files", pa.int64()),
        ("mapped_files", pa.int64()),
        ("distinct_mappings", pa.int64()),
        ("mapping_ids", pa.list_(pa.int64())),
    ]
)
_TYPES_SCHEMA = pa.schema([("mapping_id", pa.int64()), ("mapping_type", pa.string())])


def _dist(live: int, mapped: int, distinct: int, ids: list[int]) -> pa.Table:
    return _table(
        [
            {
                "live_files": live,
                "mapped_files": mapped,
                "distinct_mappings": distinct,
                "mapping_ids": ids,
            }
        ],
        _DIST_SCHEMA,
    )


def test_mapping_zero_passes() -> None:
    dist = _dist(2, 0, 0, [])
    types = _table([], _TYPES_SCHEMA)
    assert mapping_divergence.classify(dist, types, _ctx(), "mapping-divergence") == []


def test_mapping_multiple_blocks() -> None:
    dist = _dist(3, 3, 2, [4, 9])
    types = _table(
        [
            {"mapping_id": 4, "mapping_type": "map_by_name"},
            {"mapping_id": 9, "mapping_type": "map_by_name"},
        ],
        _TYPES_SCHEMA,
    )
    findings = mapping_divergence.classify(dist, types, _ctx(), "mapping-divergence")
    assert len(findings) == 1
    assert findings[0].severity is Severity.BLOCKER
    assert findings[0].data["mapping_ids"] == [4, 9]


def test_mapping_single_without_capability_blocks() -> None:
    dist = _dist(2, 2, 1, [4])
    types = _table([{"mapping_id": 4, "mapping_type": "map_by_name"}], _TYPES_SCHEMA)
    findings = mapping_divergence.classify(dist, types, _ctx(), "mapping-divergence")
    assert findings[0].severity is Severity.BLOCKER


def test_mapping_single_with_capability_degraded() -> None:
    dist = _dist(2, 2, 1, [4])
    types = _table([{"mapping_id": 4, "mapping_type": "map_by_name"}], _TYPES_SCHEMA)
    ctx = _ctx(capabilities=frozenset({Capability.NAME_MAPPING}))
    findings = mapping_divergence.classify(dist, types, ctx, "mapping-divergence")
    assert findings[0].severity is Severity.DEGRADED


def test_mapping_unknown_type_blocks() -> None:
    dist = _dist(2, 2, 1, [4])
    types = _table(
        [{"mapping_id": 4, "mapping_type": "map_by_position"}], _TYPES_SCHEMA
    )
    findings = mapping_divergence.classify(dist, types, _ctx(), "mapping-divergence")
    assert any("unknown" in f.detail for f in findings)
    assert all(f.severity is Severity.BLOCKER for f in findings)


# --------------------------------------------------------------------------
# encryption-key
# --------------------------------------------------------------------------
_ENC_SCHEMA = pa.schema(
    [
        ("encrypted_data_files", pa.int64()),
        ("encrypted_delete_files", pa.int64()),
        ("catalog_encrypted", pa.string()),
    ]
)


def _enc(data: int, delete: int, catalog: str | None) -> pa.Table:
    return _table(
        [
            {
                "encrypted_data_files": data,
                "encrypted_delete_files": delete,
                "catalog_encrypted": catalog,
            }
        ],
        _ENC_SCHEMA,
    )


def test_encryption_clean_passes() -> None:
    assert encryption_key.classify(_enc(0, 0, None), _ctx(), "encryption-key") == []


def test_encryption_files_block_without_key_leak() -> None:
    findings = encryption_key.classify(_enc(12, 1, None), _ctx(), "encryption-key")
    assert len(findings) == 1
    assert findings[0].severity is Severity.BLOCKER
    # never exposes key material
    assert "encryption_key" not in str(findings[0].data)
    assert findings[0].data == {
        "encrypted_data_files": 12,
        "encrypted_delete_files": 1,
    }


def test_encryption_catalog_flag_blocks() -> None:
    findings = encryption_key.classify(_enc(0, 0, "true"), _ctx(), "encryption-key")
    assert len(findings) == 1
    assert findings[0].severity is Severity.BLOCKER
    assert findings[0].data == {"catalog_encrypted": True}
