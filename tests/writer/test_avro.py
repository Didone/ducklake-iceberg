"""Avro writer tests (round-trip against PyIceberg's reader).

The acceptance criterion is not "the file opens" but "PyIceberg's reader
rebuilds typed objects identical to the ones written". The regression tests
pin the three conversion pitfalls (int-key map, positional Record, enum).
"""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import IO, Any, cast

import fastavro
import pytest
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    construct_partition_summaries,
    data_file_with_partition,
    manifest_entry_schema_with_data_file,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, DayTransform
from pyiceberg.typedef import Record
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)
from pyiceberg.utils.schema_conversion import AvroSchemaConversion

from ducklake_iceberg.writer.avro import (
    UnsupportedFormatVersion,
    manifest_list_name,
    manifest_name,
    to_avro_value,
)
from ducklake_iceberg.writer.manifest import verify_manifest, write_manifest
from ducklake_iceberg.writer.manifest_list import (
    verify_manifest_list,
    write_manifest_list,
)


def _io() -> PyArrowFileIO:
    return PyArrowFileIO()


def _schema() -> Schema:
    return Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(2, "state", StringType(), required=False),
        NestedField(3, "dt", DateType(), required=False),
        NestedField(4, "amount", DoubleType(), required=False),
        schema_id=0,
    )


def _spec_partitioned() -> PartitionSpec:
    return PartitionSpec(
        PartitionField(
            source_id=3, field_id=1000, transform=DayTransform(), name="dt_day"
        ),
        PartitionField(
            source_id=2,
            field_id=1001,
            transform=BucketTransform(16),
            name="state_bucket",
        ),
        spec_id=0,
    )


def _spec_empty() -> PartitionSpec:
    return PartitionSpec(spec_id=0)


def _data_file(
    partition: Record, content: DataFileContent = DataFileContent.DATA
) -> DataFile:
    return DataFile.from_args(
        content=content,
        file_path="s3://b/t/data/a.parquet",
        file_format=FileFormat.PARQUET,
        partition=partition,
        file_size_in_bytes=100,
        record_count=3,
        spec_id=0,
        sort_order_id=None,
        equality_ids=None,
        key_metadata=None,
        value_counts={1: 3, 2: 3},
        lower_bounds={1: b"\x01\x00\x00\x00"},
        upper_bounds={1: b"\x03\x00\x00\x00"},
        split_offsets=[4],
    )


def _entry(
    partition: Record, content: DataFileContent = DataFileContent.DATA
) -> ManifestEntry:
    return ManifestEntry.from_args(
        status=ManifestEntryStatus.ADDED,
        snapshot_id=10,
        sequence_number=None,
        file_sequence_number=None,
        data_file=_data_file(partition, content),
    )


# -- round-trip, unpartitioned ---------------------------------------------


@pytest.mark.roundtrip
def test_roundtrip_unpartitioned(tmp_path: Path) -> None:
    io = _io()
    loc = "file://" + str(tmp_path)
    path = manifest_name(loc, 0)
    entry = _entry(Record())
    size = write_manifest(
        io, path, [entry], _schema(), _spec_empty(), ManifestContent.DATA
    )
    assert size > 0
    got = verify_manifest(io, path)
    assert len(got) == 1
    assert got[0].status is ManifestEntryStatus.ADDED
    assert got[0].data_file.file_format is FileFormat.PARQUET
    assert got[0].data_file.content is DataFileContent.DATA


# -- round-trip, partitioned -----------------------------------------------


@pytest.mark.roundtrip
def test_roundtrip_partitioned(tmp_path: Path) -> None:
    io = _io()
    path = manifest_name("file://" + str(tmp_path), 0)
    entry = _entry(Record(19723, 7))
    write_manifest(
        io, path, [entry], _schema(), _spec_partitioned(), ManifestContent.DATA
    )
    got = verify_manifest(io, path)
    part = got[0].data_file.partition
    assert (part[0], part[1]) == (19723, 7)


# -- int-key map passed as a plain dict -> ValueError ----------------------


def test_regression_int_key_map_as_dict() -> None:
    # Reproduces the pitfall: the Avro schema of column_sizes expects an array
    # of {key,value} records; passing a plain Python dict fails in fastavro.
    schema = _schema()
    spec = _spec_empty()
    entry_schema = manifest_entry_schema_with_data_file(
        2, data_file_with_partition(spec.partition_type(schema), 2)
    )
    avro_schema = fastavro.parse_schema(
        AvroSchemaConversion().iceberg_to_avro(
            Schema(*entry_schema.fields), schema_name="manifest_entry"
        )
    )
    struct = StructType(*entry_schema.fields)
    entry = _entry(Record())
    good = cast("dict[str, Any]", to_avro_value(entry, struct))
    # corrupt it: replace value_counts with a raw dict
    bad = dict(good)
    data_file = dict(bad["data_file"])
    data_file["value_counts"] = {1: 3}  # raw dict instead of array
    bad["data_file"] = data_file
    buffer = BytesIO()
    with pytest.raises(ValueError, match="do not match"):
        fastavro.writer(buffer, avro_schema, [bad])


# -- positional Record -> named getattr raises AttributeError --------------


def test_regression_record_positional_access() -> None:
    rec = Record(19723, 7)
    with pytest.raises(AttributeError):
        _ = rec.dt_day  # type: ignore[attr-defined]  # Record is positional
    # the correct conversion uses index access
    itype = StructType(
        NestedField(1000, "dt_day", DateType(), required=False),
        NestedField(1001, "state_bucket", IntegerType(), required=False),
    )
    out = to_avro_value(rec, itype)
    assert out == {"dt_day": 19723, "state_bucket": 7}


# -- enum needs .value -----------------------------------------------------


def test_regression_enum_value() -> None:
    assert str(FileFormat.PARQUET) == "FileFormat.PARQUET"
    assert to_avro_value(FileFormat.PARQUET, StringType()) == "PARQUET"


# -- sequence_number None is preserved -------------------------------------


def test_sequence_number_none_preserved(tmp_path: Path) -> None:
    io = _io()
    path = manifest_name("file://" + str(tmp_path), 0)
    entry = _entry(Record())
    write_manifest(io, path, [entry], _schema(), _spec_empty(), ManifestContent.DATA)
    got = verify_manifest(io, path)
    assert got[0].sequence_number is None
    assert got[0].file_sequence_number is None


# -- manifest list with 2 partition fields ---------------------------------


@pytest.mark.roundtrip
def test_manifest_list_two_partitions(tmp_path: Path) -> None:
    io = _io()
    schema = _schema()
    spec = _spec_partitioned()
    mpath = manifest_name("file://" + str(tmp_path), 0)
    entries = [_entry(Record(19723, 7)), _entry(Record(19800, 7))]
    size = write_manifest(io, mpath, entries, schema, spec, ManifestContent.DATA)
    summaries = construct_partition_summaries(
        spec, schema, [e.data_file.partition for e in entries]
    )
    mf = ManifestFile.from_args(
        manifest_path=mpath,
        manifest_length=size,
        partition_spec_id=0,
        content=ManifestContent.DATA,
        sequence_number=1,
        min_sequence_number=1,
        added_snapshot_id=10,
        added_files_count=2,
        existing_files_count=0,
        deleted_files_count=0,
        added_rows_count=6,
        existing_rows_count=0,
        deleted_rows_count=0,
        partitions=summaries,
        key_metadata=None,
    )
    lpath = manifest_list_name("file://" + str(tmp_path), 10)
    write_manifest_list(io, lpath, [mf], 10, None, 1)
    got = verify_manifest_list(io, lpath)
    assert got[0].content is ManifestContent.DATA
    assert got[0].added_files_count == 2
    # 2 partition fields -> 2 summaries
    assert got[0].partitions is not None
    assert len(got[0].partitions) == 2


# -- manifest header metadata ----------------------------------------------


def test_manifest_header_metadata(tmp_path: Path) -> None:
    io = _io()
    path = manifest_name("file://" + str(tmp_path), 0)
    write_manifest(
        io, path, [_entry(Record())], _schema(), _spec_empty(), ManifestContent.DATA
    )
    with io.new_input(path).open() as stream:
        reader = fastavro.reader(cast("IO[bytes]", stream))
        meta = reader.metadata
    for key in (
        "schema",
        "partition-spec",
        "partition-spec-id",
        "format-version",
        "content",
    ):
        assert key in meta
    assert meta["content"] == "data"
    assert meta["format-version"] == "2"


# -- content=DELETES -------------------------------------------------------


@pytest.mark.roundtrip
def test_delete_manifest_content(tmp_path: Path) -> None:
    io = _io()
    path = manifest_name("file://" + str(tmp_path), 0)
    entry = _entry(Record(), content=DataFileContent.POSITION_DELETES)
    write_manifest(io, path, [entry], _schema(), _spec_empty(), ManifestContent.DELETES)
    with io.new_input(path).open() as stream:
        meta = fastavro.reader(cast("IO[bytes]", stream)).metadata
    assert meta["content"] == "deletes"
    got = verify_manifest(io, path)
    assert got[0].data_file.content is DataFileContent.POSITION_DELETES


# -- writing to an existing path -> error, no overwrite --------------------


def test_no_overwrite(tmp_path: Path) -> None:
    io = _io()
    path = manifest_name("file://" + str(tmp_path), 0)
    entry = _entry(Record())
    write_manifest(io, path, [entry], _schema(), _spec_empty(), ManifestContent.DATA)
    # PyArrow raises FileExistsError (an OSError subclass) with overwrite=False
    with pytest.raises(OSError):  # noqa: PT011 - FileExistsError is the concrete class
        write_manifest(
            io, path, [entry], _schema(), _spec_empty(), ManifestContent.DATA
        )


# -- extras -----------------------------------------------------------------


def test_unsupported_format_version(tmp_path: Path) -> None:
    io = _io()
    path = manifest_name("file://" + str(tmp_path), 0)
    with pytest.raises(UnsupportedFormatVersion) as exc:
        write_manifest(
            io,
            path,
            [_entry(Record())],
            _schema(),
            _spec_empty(),
            ManifestContent.DATA,
            format_version=4,
        )
    assert exc.value.exit_code == 2


def test_to_avro_value_none() -> None:
    assert to_avro_value(None, StringType()) is None


def test_to_avro_value_string_key_map() -> None:
    mtype = MapType(
        key_id=1,
        key_type=StringType(),
        value_id=2,
        value_type=LongType(),
        value_required=True,
    )
    assert to_avro_value({"a": 1}, mtype) == {"a": 1}


def test_to_avro_value_int_key_map_becomes_array() -> None:
    mtype = MapType(
        key_id=1,
        key_type=IntegerType(),
        value_id=2,
        value_type=LongType(),
        value_required=True,
    )
    assert to_avro_value({5: 10}, mtype) == [{"key": 5, "value": 10}]


def test_manifest_names_unique() -> None:
    a = manifest_name("file:///x/t", 0)
    b = manifest_name("file:///x/t", 0)
    assert a != b  # uuid per call
    assert a.endswith("-m0.avro")
    la = manifest_list_name("file:///x/t", 10)
    assert "snap-10-1-" in la
