"""Round-trip of the complete writer against PyIceberg's reader.

Version-change gate: using only iceduck's writer, it builds an Iceberg v2
table with **all** the artifacts the export writes (data parquet, position
delete file, data manifest, delete manifest, manifest list and
``metadata.json``) and re-reads each one with PyIceberg's reference reader,
up to a ``scan()`` with the deletes applied. Per-artifact round-trips live in
``test_avro.py``, ``test_metadata.py`` and ``tests/deletes/test_manifest.py``;
this one ties everything into a single scenario, the first one the
``compat-latest`` job runs (``pytest -m roundtrip``) against the new
PyIceberg/DuckDB version.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DataFileContent,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    construct_partition_summaries,
    read_manifest_list,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField, StringType

from iceduck.deletes.manifest import write_delete_manifest
from iceduck.deletes.writer import PositionDelete, write_position_delete_file
from iceduck.writer.avro import manifest_list_name, manifest_name
from iceduck.writer.manifest import write_manifest
from iceduck.writer.manifest_list import write_manifest_list
from iceduck.writer.metadata import (
    DuckLakeProvenance,
    SnapshotCounters,
    build_table_metadata,
    next_metadata_path,
    validate_counters,
    write_metadata_json,
)
from iceduck.writer.stats import FileToExport, build_data_file

pytestmark = pytest.mark.roundtrip

_SNAPSHOT_ID = 7_001
_SEQ = 1
_IDS = [1, 2, 3, 4, 5]
_DELETED_POS = [1, 3]  # ids 2 and 4

_SCHEMA = Schema(
    NestedField(1, "id", IntegerType(), required=False),
    NestedField(2, "state", StringType(), required=False),
    schema_id=0,
)
_UNPARTITIONED = PartitionSpec(spec_id=0)
_BY_UF = PartitionSpec(
    PartitionField(
        source_id=2, field_id=1000, transform=IdentityTransform(), name="state"
    ),
    spec_id=0,
)


def _pfield(name: str, typ: pa.DataType, fid: int) -> pa.Field:
    return pa.field(name, typ, metadata={b"PARQUET:field_id": str(fid).encode()})


def _manifest_file(
    path: str, size: int, spec: PartitionSpec, entries: list[ManifestEntry]
) -> ManifestFile:
    return ManifestFile.from_args(
        manifest_path=path,
        manifest_length=size,
        partition_spec_id=spec.spec_id,
        content=ManifestContent.DATA,
        sequence_number=_SEQ,
        min_sequence_number=_SEQ,
        added_snapshot_id=_SNAPSHOT_ID,
        added_files_count=len(entries),
        existing_files_count=0,
        deleted_files_count=0,
        added_rows_count=sum(e.data_file.record_count for e in entries),
        existing_rows_count=0,
        deleted_rows_count=0,
        partitions=construct_partition_summaries(
            spec, _SCHEMA, [e.data_file.partition for e in entries]
        ),
        key_metadata=None,
    )


@pytest.mark.parametrize(
    ("spec", "partition"),
    [(_UNPARTITIONED, Record()), (_BY_UF, Record("SP"))],
    ids=["unpartitioned", "identity"],
)
def test_full_table_roundtrip_with_position_deletes(
    tmp_path: Path, spec: PartitionSpec, partition: Record
) -> None:
    io = PyArrowFileIO()
    loc = "file://" + str(tmp_path)
    (tmp_path / "data").mkdir()
    (tmp_path / "metadata").mkdir()

    # 1. data parquet with the DuckLake field IDs (zero-copy: only read)
    data_local = tmp_path / "data" / "a.parquet"
    pq.write_table(
        pa.table(
            {"id": _IDS, "state": ["SP"] * len(_IDS)},
            schema=pa.schema(
                [_pfield("id", pa.int32(), 1), _pfield("state", pa.string(), 2)]
            ),
        ),
        data_local,
    )
    data_path = "file://" + str(data_local)
    data_file = build_data_file(
        FileToExport(
            path=data_path,
            file_size_in_bytes=data_local.stat().st_size,
            record_count=len(_IDS),
            partition_values=partition,
        ),
        _SCHEMA,
        spec,
        {},
        io,
    )

    # 2. data manifest
    entry = ManifestEntry.from_args(
        status=ManifestEntryStatus.ADDED,
        snapshot_id=_SNAPSHOT_ID,
        sequence_number=None,
        file_sequence_number=None,
        data_file=data_file,
    )
    data_manifest_path = manifest_name(loc, 0)
    data_manifest_size = write_manifest(
        io, data_manifest_path, [entry], _SCHEMA, spec, ManifestContent.DATA
    )
    data_manifest = _manifest_file(
        data_manifest_path, data_manifest_size, spec, [entry]
    )

    # 3. position delete file + delete manifest
    delete_result = write_position_delete_file(
        io,
        [PositionDelete(data_path, pos, partition) for pos in _DELETED_POS],
        loc,
        partition,
        spec,
    )
    delete_manifest = write_delete_manifest(
        io,
        manifest_name(loc, 1),
        [delete_result],
        _SCHEMA,
        spec,
        snapshot_id=_SNAPSHOT_ID,
        sequence_number=_SEQ,
    )

    # 4. manifest list and metadata.json
    list_path = manifest_list_name(loc, _SNAPSHOT_ID)
    write_manifest_list(
        io, list_path, [data_manifest, delete_manifest], _SNAPSHOT_ID, None, _SEQ
    )
    counters = SnapshotCounters(
        added_data_files=1,
        added_records=len(_IDS),
        added_files_size=data_file.file_size_in_bytes,
        total_data_files=1,
        total_records=len(_IDS),
        total_files_size=data_file.file_size_in_bytes,
        total_delete_files=1,
        total_position_deletes=len(_DELETED_POS),
    )
    validate_counters(counters, [entry])
    metadata = build_table_metadata(
        _SCHEMA,
        spec,
        loc,
        list_path,
        counters,
        DuckLakeProvenance(
            catalog="lake",
            schema_name="s",
            table_name="t",
            table_id=1,
            snapshot_id=3,
            schema_version=0,
            exported_at=datetime.now(UTC).isoformat(),
        ),
        table_uuid=uuid.uuid4(),
        snapshot_id=_SNAPSHOT_ID,
    )
    metadata_path = next_metadata_path(loc, 1)
    write_metadata_json(io, metadata, metadata_path)

    # -- re-read through PyIceberg ------------------------------------------
    doc = json.loads(Path(metadata_path.removeprefix("file://")).read_text())
    assert doc["format-version"] == 2

    table = StaticTable.from_metadata(metadata_path)
    assert [(f.field_id, f.name) for f in table.schema().fields] == [
        (1, "id"),
        (2, "state"),
    ]
    assert table.spec() == spec
    snapshot = table.current_snapshot()
    assert snapshot is not None
    assert snapshot.snapshot_id == _SNAPSHOT_ID
    assert snapshot.sequence_number == _SEQ
    assert snapshot.manifest_list == list_path

    manifests = list(read_manifest_list(io.new_input(list_path)))
    assert [m.content for m in manifests] == [
        ManifestContent.DATA,
        ManifestContent.DELETES,
    ]
    data_entries, delete_entries = (m.fetch_manifest_entry(io) for m in manifests)
    assert [e.data_file.file_path for e in data_entries] == [data_path]
    assert [e.data_file.record_count for e in data_entries] == [len(_IDS)]
    assert [e.sequence_number for e in data_entries] == [_SEQ]  # inherited (v2)
    (delete_entry,) = delete_entries
    assert delete_entry.data_file.content is DataFileContent.POSITION_DELETES
    assert delete_entry.data_file.record_count == len(_DELETED_POS)
    assert delete_entry.sequence_number == _SEQ
    if spec.fields:
        assert data_entries[0].data_file.partition[0] == "SP"
        assert delete_entry.data_file.partition[0] == "SP"

    # The scan applies the position delete file written by iceduck
    tasks = list(table.scan().plan_files())
    assert len(tasks) == 1
    assert [d.file_path for d in tasks[0].delete_files] == [
        delete_result.data_file.file_path
    ]
    rows = table.scan().to_arrow()
    assert sorted(rows.column("id").to_pylist()) == [1, 3, 5]
    assert table.scan().to_arrow().num_rows == len(_IDS) - len(_DELETED_POS)
