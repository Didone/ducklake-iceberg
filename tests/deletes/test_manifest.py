"""Delete manifest: round-trip against the PyIceberg reader.

The criterion is the reference reader: the manifest written by
``write_delete_manifest`` is read back by ``ManifestFile.fetch_manifest_entry``
(with sequence inheritance from the manifest list) and PyIceberg's
``DeleteFileIndex`` associates the delete with the data file through the
``file_path`` bounds.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DATA_FILE_TYPE,
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    read_manifest_list,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.delete_file_index import DeleteFileIndex
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField, StringType, StructType

from ducklake_iceberg.deletes import manifest as dm
from ducklake_iceberg.deletes.writer import (
    DeleteFileResult,
    PositionDelete,
    write_position_delete_file,
)
from ducklake_iceberg.errors import ExitCode
from ducklake_iceberg.writer.manifest_list import write_manifest_list

_SCHEMA = Schema(
    NestedField(1, "id", IntegerType(), required=False),
    NestedField(2, "uf", StringType(), required=False),
    schema_id=0,
)
_UNPARTITIONED = PartitionSpec(spec_id=0)
_BY_UF = PartitionSpec(
    PartitionField(
        source_id=2, field_id=1000, transform=IdentityTransform(), name="uf"
    ),
    spec_id=0,
)


def _location(tmp_path: Path) -> str:
    return "file://" + str(tmp_path)


def _delete_file(
    tmp_path: Path,
    data_path: str,
    positions: list[int],
    partition: Record,
    spec: PartitionSpec,
) -> DeleteFileResult:
    deletes = [PositionDelete(data_path, pos, partition) for pos in positions]
    return write_position_delete_file(
        PyArrowFileIO(), deletes, _location(tmp_path), partition, spec
    )


def _data_file(path: str, partition: Record) -> DataFile:
    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=path,
        file_format=FileFormat.PARQUET,
        partition=partition,
        record_count=10,
        file_size_in_bytes=100,
    )
    # ``from_args(spec_id=...)`` is ignored by PyIceberg 0.12: the spec_id is
    # not stored in the file and only exists once assigned (or inherited from
    # the manifest on re-read)
    data_file.spec_id = 0
    return data_file


# -- round-trip ---------------------------------------------------------------
@pytest.mark.roundtrip
@pytest.mark.parametrize(
    ("spec", "partition"),
    [(_UNPARTITIONED, Record()), (_BY_UF, Record("SP"))],
)
def test_roundtrip_with_pyiceberg_reader(
    tmp_path: Path, spec: PartitionSpec, partition: Record
) -> None:
    io = PyArrowFileIO()
    loc = _location(tmp_path)
    data_a = f"{loc}/data/a.parquet"
    data_b = f"{loc}/data/b.parquet"
    results = [
        _delete_file(tmp_path, data_a, [1, 3], partition, spec),
        _delete_file(tmp_path, data_b, [0], partition, spec),
    ]
    manifest = dm.write_delete_manifest(
        io,
        f"{loc}/metadata/deletes-m0.avro",
        results,
        _SCHEMA,
        spec,
        snapshot_id=99,
        sequence_number=3,
    )

    assert manifest.content == ManifestContent.DELETES
    assert manifest.sequence_number == 3
    assert manifest.min_sequence_number == 3
    assert manifest.added_snapshot_id == 99
    assert manifest.added_files_count == 2
    assert manifest.added_rows_count == 3
    assert manifest.existing_files_count == 0
    assert manifest.deleted_files_count == 0

    # Manifest list read back by PyIceberg: the entry comes back as DELETES
    list_path = f"{loc}/metadata/snap-99.avro"
    write_manifest_list(io, list_path, [manifest], 99, None, 3)
    (reread,) = list(read_manifest_list(io.new_input(list_path)))
    assert reread.content == ManifestContent.DELETES
    assert reread.sequence_number == 3

    entries = reread.fetch_manifest_entry(io)
    assert [e.status for e in entries] == [ManifestEntryStatus.ADDED] * 2
    # ADDED with a null sequence inherits the manifest's (v2)
    assert {e.sequence_number for e in entries} == {3}
    assert {e.file_sequence_number for e in entries} == {3}
    assert {e.snapshot_id for e in entries} == {99}
    assert all(e.data_file.content == DataFileContent.POSITION_DELETES for e in entries)
    assert [e.data_file.record_count for e in entries] == [2, 1]
    assert [e.data_file.file_path for e in entries] == [
        r.data_file.file_path for r in results
    ]

    # The reference reader matches each delete to its data file via the bounds
    index = DeleteFileIndex()
    for e in entries:
        index.add_delete_file(e, partition_key=partition)
    hit_a = index.for_data_file(1, _data_file(data_a, partition), partition)
    hit_b = index.for_data_file(1, _data_file(data_b, partition), partition)
    assert {f.file_path for f in hit_a} == {results[0].data_file.file_path}
    assert {f.file_path for f in hit_b} == {results[1].data_file.file_path}
    # a data file added later (higher seq) is not reached
    assert not index.for_data_file(4, _data_file(data_a, partition), partition)


# -- validations ---------------------------------------------------------------
def test_rejects_empty_and_other_versions(tmp_path: Path) -> None:
    io = PyArrowFileIO()
    path = f"{_location(tmp_path)}/m.avro"
    with pytest.raises(ValueError, match="empty"):
        dm.write_delete_manifest(
            io, path, [], _SCHEMA, _UNPARTITIONED, 1, sequence_number=1
        )
    result = _delete_file(tmp_path, "/d/a.parquet", [1], Record(), _UNPARTITIONED)
    with pytest.raises(ValueError, match="format_version=2"):
        dm.write_delete_manifest(
            io, path, [result], _SCHEMA, _UNPARTITIONED, 1, 3, sequence_number=1
        )


def test_rejects_data_content(tmp_path: Path) -> None:
    result = _delete_file(tmp_path, "/d/a.parquet", [1], Record(), _UNPARTITIONED)
    data = DeleteFileResult(
        data_file=_data_file("/d/a.parquet", Record()),
        referenced_data_file=None,
        record_count=1,
        file_size_in_bytes=1,
    )
    with pytest.raises(dm.MixedContentError):
        dm.write_delete_manifest(
            PyArrowFileIO(),
            f"{_location(tmp_path)}/m.avro",
            [result, data],
            _SCHEMA,
            _UNPARTITIONED,
            1,
            sequence_number=1,
        )


def test_referenced_data_file_must_match_bounds(tmp_path: Path) -> None:
    result = _delete_file(tmp_path, "/d/a.parquet", [1], Record(), _UNPARTITIONED)
    assert result.referenced_data_file == "/d/a.parquet"
    wrong = dataclasses.replace(result, referenced_data_file="/d/outro.parquet")
    with pytest.raises(dm.DeleteManifestError, match="do not match"):
        dm.write_delete_manifest(
            PyArrowFileIO(),
            f"{_location(tmp_path)}/m.avro",
            [wrong],
            _SCHEMA,
            _UNPARTITIONED,
            1,
            sequence_number=1,
        )


def test_multiple_referenced_files_rejected(tmp_path: Path) -> None:
    deletes = [
        PositionDelete("/d/a.parquet", 1, Record()),
        PositionDelete("/d/b.parquet", 2, Record()),
    ]
    result = write_position_delete_file(
        PyArrowFileIO(), deletes, _location(tmp_path), Record(), _UNPARTITIONED
    )
    assert result.referenced_data_file is None
    # without a declared reference, a multi-file delete is accepted
    manifest = dm.write_delete_manifest(
        PyArrowFileIO(),
        f"{_location(tmp_path)}/ok.avro",
        [result],
        _SCHEMA,
        _UNPARTITIONED,
        1,
        sequence_number=2,
    )
    assert manifest.added_rows_count == 2
    # declaring a single file with bounds for two is a grouping bug
    lying = dataclasses.replace(result, referenced_data_file="/d/a.parquet")
    with pytest.raises(dm.MultipleReferencedFilesError):
        dm.write_delete_manifest(
            PyArrowFileIO(),
            f"{_location(tmp_path)}/bad.avro",
            [lying],
            _SCHEMA,
            _UNPARTITIONED,
            1,
            sequence_number=2,
        )


def test_verification_detects_non_delete_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = _delete_file(tmp_path, "/d/a.parquet", [1], Record(), _UNPARTITIONED)
    data_entry = ManifestEntry.from_args(
        status=ManifestEntryStatus.ADDED,
        snapshot_id=1,
        data_file=_data_file("/d/a.parquet", Record()),
    )
    monkeypatch.setattr(dm, "verify_manifest", lambda *_a, **_k: [data_entry])
    with pytest.raises(dm.DeleteManifestVerificationError):
        dm.write_delete_manifest(
            PyArrowFileIO(),
            f"{_location(tmp_path)}/m.avro",
            [result],
            _SCHEMA,
            _UNPARTITIONED,
            1,
            sequence_number=1,
        )


def test_validate_sequence_numbers() -> None:
    dm.validate_sequence_numbers(3, 3)
    dm.validate_sequence_numbers(3, 1)
    with pytest.raises(dm.InvalidSequenceNumberError) as exc_info:
        dm.validate_sequence_numbers(1, 3)
    assert exc_info.value.exit_code == ExitCode.ERROR


def test_delete_file_type_v2_adds_field_143() -> None:
    partition_type = StructType(
        NestedField(1000, "uf", StringType(), required=False),
    )
    extended = dm.delete_file_type_v2(partition_type)
    ids = [f.field_id for f in extended.fields]
    assert ids[-1] == 143
    assert ids[:-1] == [f.field_id for f in DATA_FILE_TYPE[2].fields]
    partition_field = extended.field(102)
    assert partition_field is not None
    assert partition_field.field_type == partition_type
    assert dm.REFERENCED_DATA_FILE_FIELD.required is False
