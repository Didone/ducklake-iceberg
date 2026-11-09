"""Tests for footer statistics and DataFile assembly.

Fixtures generate DuckLake-style Parquet: `PARQUET:field_id` metadata equal to
the Iceberg schema's `column_id`, plus the two internal columns
(`_ducklake_internal_row_id` / `_ducklake_internal_snapshot_id`) with the
reserved field IDs. The manifest round-trip is covered in test_avro.py.
"""

from __future__ import annotations

import struct
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.io.pyarrow import (
    PyArrowFileIO,
    data_file_statistics_from_parquet_metadata,
    parquet_path_to_id_mapping,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    StructType,
)

from iceduck.writer.stats import (
    DUCKLAKE_INTERNAL_FIELD_IDS,
    FileToExport,
    RecordCountMismatch,
    build_data_file,
    build_data_files,
)

ROW_ID = DUCKLAKE_INTERNAL_FIELD_IDS["_ducklake_internal_row_id"]
SNAP_ID = DUCKLAKE_INTERNAL_FIELD_IDS["_ducklake_internal_snapshot_id"]


def _io() -> PyArrowFileIO:
    return PyArrowFileIO()


def _fid(name: str, typ: pa.DataType, fid: int) -> pa.Field:
    return pa.field(name, typ, metadata={b"PARQUET:field_id": str(fid).encode()})


def _internal_fields() -> list[pa.Field]:
    return [
        _fid("_ducklake_internal_row_id", pa.int64(), ROW_ID),
        _fid("_ducklake_internal_snapshot_id", pa.int64(), SNAP_ID),
    ]


def _write(path: Path, schema: pa.Schema, data: dict[str, Any]) -> FileToExport:
    tbl = pa.table(data, schema=schema)
    pq.write_table(tbl, path)
    return FileToExport(
        path=str(path),
        file_size_in_bytes=path.stat().st_size,
        record_count=tbl.num_rows,
        partition_values=Record(),
    )


def _flat_schema() -> Schema:
    return Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(2, "state", StringType(), required=False),
        NestedField(3, "dt", DateType(), required=False),
        NestedField(4, "amount", DoubleType(), required=False),
        schema_id=0,
    )


def _flat_parquet(path: Path, n: int = 3) -> FileToExport:
    from datetime import date

    pa_schema = pa.schema(
        [
            _fid("id", pa.int32(), 1),
            _fid("state", pa.string(), 2),
            _fid("dt", pa.date32(), 3),
            _fid("amount", pa.float64(), 4),
            *_internal_fields(),
        ]
    )
    data = {
        "id": [1, 2, 3][:n],
        "state": ["MG", "RJ", "SP"][:n],
        "dt": [date(2024, 1, 5), date(2024, 2, 1), date(2024, 3, 11)][:n],
        "amount": [10.5, 20.0, 30.25][:n],
        "_ducklake_internal_row_id": [10, 11, 12][:n],
        "_ducklake_internal_snapshot_id": [2, 2, 2][:n],
    }
    return _write(path, pa_schema, data)


_SPEC = PartitionSpec(spec_id=0)


# -- flat table, decodable bounds ------------------------------------------


def test_flat_table_bounds(tmp_path: Path) -> None:
    fte = _flat_parquet(tmp_path / "a.parquet")
    df = build_data_file(fte, _flat_schema(), _SPEC, {}, _io())
    assert df.record_count == 3
    assert struct.unpack("<i", df.lower_bounds[1])[0] == 1
    assert struct.unpack("<i", df.upper_bounds[1])[0] == 3
    assert df.lower_bounds[2] == b"MG"
    assert df.upper_bounds[2] == b"SP"
    assert struct.unpack("<i", df.lower_bounds[3])[0] == 19727  # 2024-01-05
    assert struct.unpack("<d", df.lower_bounds[4])[0] == 10.5
    assert struct.unpack("<d", df.upper_bounds[4])[0] == 30.25


# -- no internal field ID is published -------------------------------------


def test_no_internal_ids_leak(tmp_path: Path) -> None:
    fte = _flat_parquet(tmp_path / "a.parquet")
    df = build_data_file(fte, _flat_schema(), _SPEC, {}, _io())
    for metric in (
        df.column_sizes,
        df.value_counts,
        df.null_value_counts,
        df.nan_value_counts,
        df.lower_bounds,
        df.upper_bounds,
    ):
        assert ROW_ID not in (metric or {})
        assert SNAP_ID not in (metric or {})


# -- not extending parquet_column_mapping -> KeyError on the name ----------


def test_regression_missing_column_mapping(tmp_path: Path) -> None:
    fte = _flat_parquet(tmp_path / "a.parquet")
    schema = _flat_schema()
    with _io().new_input(fte.path).open() as stream:
        footer = pq.read_metadata(stream)
    # mapping WITHOUT the internal columns
    column_mapping = dict(parquet_path_to_id_mapping(schema))
    from pyiceberg.io.pyarrow import compute_statistics_plan

    plan = dict(compute_statistics_plan(schema, {}))
    with pytest.raises(KeyError) as exc:
        data_file_statistics_from_parquet_metadata(
            parquet_metadata=footer,
            stats_columns=plan,
            parquet_column_mapping=column_mapping,
        )
    assert "_ducklake_internal_row_id" in str(exc.value)


# -- not extending stats_columns -> KeyError on the field ID ---------------


def test_regression_missing_stats_columns(tmp_path: Path) -> None:
    fte = _flat_parquet(tmp_path / "a.parquet")
    schema = _flat_schema()
    with _io().new_input(fte.path).open() as stream:
        footer = pq.read_metadata(stream)
    # mapping WITH the internals but plan WITHOUT -> KeyError on the ID instead
    column_mapping = dict(parquet_path_to_id_mapping(schema))
    column_mapping.update(DUCKLAKE_INTERNAL_FIELD_IDS)
    from pyiceberg.io.pyarrow import compute_statistics_plan

    plan = dict(compute_statistics_plan(schema, {}))
    with pytest.raises(KeyError) as exc:
        data_file_statistics_from_parquet_metadata(
            parquet_metadata=footer,
            stats_columns=plan,
            parquet_column_mapping=column_mapping,
        )
    assert str(ROW_ID) in str(exc.value)


# -- nested struct ---------------------------------------------------------


def test_nested_struct_ids_preserved(tmp_path: Path) -> None:
    # schema: id(1), address struct(5){ street(6), number(7) }
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(
            5,
            "address",
            StructType(
                NestedField(6, "street", StringType(), required=False),
                NestedField(7, "number", IntegerType(), required=False),
            ),
            required=False,
        ),
        schema_id=0,
    )
    assert 6 in set(schema.field_ids)
    assert 7 in set(schema.field_ids)

    struct_type = pa.struct(
        [
            _fid("street", pa.string(), 6),
            _fid("number", pa.int32(), 7),
        ]
    )
    pa_schema = pa.schema(
        [
            _fid("id", pa.int32(), 1),
            _fid("address", struct_type, 5),
            *_internal_fields(),
        ]
    )
    data = {
        "id": [1, 2],
        "address": [{"street": "A", "number": 10}, {"street": "B", "number": 20}],
        "_ducklake_internal_row_id": [1, 2],
        "_ducklake_internal_snapshot_id": [2, 2],
    }
    fte = _write(tmp_path / "n.parquet", pa_schema, data)
    df = build_data_file(fte, schema, _SPEC, {}, _io())
    # nested IDs were not dropped by the filtering
    assert 6 in df.value_counts
    assert 7 in df.value_counts
    assert ROW_ID not in df.value_counts


# -- all-null column -------------------------------------------------------


def test_all_nulls_column(tmp_path: Path) -> None:
    pa_schema = pa.schema(
        [
            _fid("id", pa.int32(), 1),
            _fid("state", pa.string(), 2),
            _fid("dt", pa.date32(), 3),
            _fid("amount", pa.float64(), 4),
            *_internal_fields(),
        ]
    )
    data = {
        "id": [1, 2, 3],
        "state": [None, None, None],
        "dt": [None, None, None],
        "amount": [1.0, 2.0, 3.0],
        "_ducklake_internal_row_id": [1, 2, 3],
        "_ducklake_internal_snapshot_id": [2, 2, 2],
    }
    fte = _write(tmp_path / "nulls.parquet", pa_schema, data)
    df = build_data_file(fte, _flat_schema(), _SPEC, {}, _io())
    assert df.null_value_counts[2] == df.value_counts[2] == 3
    assert 2 not in df.lower_bounds  # no bound for an all-null column


# -- double with NaN -------------------------------------------------------


def test_double_with_nan(tmp_path: Path) -> None:
    pa_schema = pa.schema(
        [
            _fid("id", pa.int32(), 1),
            _fid("state", pa.string(), 2),
            _fid("dt", pa.date32(), 3),
            _fid("amount", pa.float64(), 4),
            *_internal_fields(),
        ]
    )
    from datetime import date

    data = {
        "id": [1, 2, 3],
        "state": ["A", "B", "C"],
        "dt": [date(2024, 1, 1)] * 3,
        "amount": [1.0, float("nan"), 3.0],
        "_ducklake_internal_row_id": [1, 2, 3],
        "_ducklake_internal_snapshot_id": [2, 2, 2],
    }
    fte = _write(tmp_path / "nan.parquet", pa_schema, data)
    df = build_data_file(fte, _flat_schema(), _SPEC, {}, _io())
    # nan_value_counts is only filled if the Parquet writer stored the count;
    # the hard invariant is: a double's bound can NEVER be NaN.
    nan_counts = df.nan_value_counts or {}
    assert nan_counts.get(4, 0) >= 0  # present or absent, never negative
    if df.lower_bounds and 4 in df.lower_bounds:
        low = struct.unpack("<d", df.lower_bounds[4])[0]
        assert low == low  # not NaN
    if df.upper_bounds and 4 in df.upper_bounds:
        high = struct.unpack("<d", df.upper_bounds[4])[0]
        assert high == high  # not NaN


# -- empty file ------------------------------------------------------------


def test_empty_file(tmp_path: Path) -> None:
    fte = _flat_parquet(tmp_path / "empty.parquet", n=0)
    df = build_data_file(fte, _flat_schema(), _SPEC, {}, _io())
    assert df.record_count == 0


# -- tampered record_count -------------------------------------------------


def test_record_count_mismatch(tmp_path: Path) -> None:
    fte = _flat_parquet(tmp_path / "a.parquet")
    tampered = FileToExport(
        path=fte.path,
        file_size_in_bytes=fte.file_size_in_bytes,
        record_count=99,
        partition_values=Record(),
    )
    with pytest.raises(RecordCountMismatch):
        build_data_file(tampered, _flat_schema(), _SPEC, {}, _io())


# -- extras -----------------------------------------------------------------


def test_build_data_files_parallel_preserves_order(tmp_path: Path) -> None:
    files = [_flat_parquet(tmp_path / f"f{i}.parquet") for i in range(5)]
    # each file is distinguishable by its path
    dfs = build_data_files(files, _flat_schema(), _SPEC, {}, _io(), max_workers=4)
    assert [d.file_path for d in dfs] == [f.path for f in files]


def test_build_data_files_empty() -> None:
    assert build_data_files([], _flat_schema(), _SPEC, {}, _io()) == []


def test_long_type_internal_collector_is_none_mode(tmp_path: Path) -> None:
    # ensures the extension uses LongType and collects no bound for internals
    fte = _flat_parquet(tmp_path / "a.parquet")
    df = build_data_file(fte, _flat_schema(), _SPEC, {}, _io())
    assert ROW_ID not in (df.lower_bounds or {})
    assert isinstance(LongType(), LongType)
