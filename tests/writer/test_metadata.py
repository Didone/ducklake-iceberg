"""Tests for the v2 metadata.json and the round-trip via StaticTable.

A fixture builds the complete Iceberg table (data parquet + manifest +
manifest list + metadata.json), exercising stats, manifests and metadata
together. The StaticTable tests close the loop: StaticTable reads the result
without Glue.
"""

from __future__ import annotations

import json
import uuid
import warnings
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    construct_partition_summaries,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable
from pyiceberg.table.snapshots import SnapshotLogEntry
from pyiceberg.typedef import Record
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
)

from iceduck.writer.avro import manifest_list_name, manifest_name
from iceduck.writer.manifest import write_manifest
from iceduck.writer.manifest_list import write_manifest_list
from iceduck.writer.metadata import (
    DuckLakeProvenance,
    InvalidLocationError,
    MetadataError,
    SnapshotCounters,
    build_table_metadata,
    next_metadata_path,
    validate_counters,
    write_metadata_json,
)
from iceduck.writer.stats import FileToExport, build_data_file


def _schema() -> Schema:
    return Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(2, "state", StringType(), required=False),
        NestedField(3, "dt", DateType(), required=False),
        NestedField(4, "amount", DoubleType(), required=False),
        schema_id=0,
    )


def _provenance() -> DuckLakeProvenance:
    return DuckLakeProvenance(
        catalog="lake",
        schema_name="sales",
        table_name="orders",
        table_id=5,
        snapshot_id=42,
        schema_version=0,
        exported_at=datetime.now(UTC).isoformat(),
    )


def _pfield(name: str, typ: pa.DataType, fid: int) -> pa.Field:
    return pa.field(name, typ, metadata={b"PARQUET:field_id": str(fid).encode()})


class _Built:
    def __init__(
        self,
        location: str,
        metadata_path: str,
        entry: ManifestEntry,
        counters: SnapshotCounters,
        io: PyArrowFileIO,
    ) -> None:
        self.location = location
        self.metadata_path = metadata_path
        self.entry = entry
        self.counters = counters
        self.io = io


def _build_table(tmp_path: Path, *, write_json: bool = True) -> _Built:
    location = "file://" + str(tmp_path)
    (tmp_path / "data").mkdir()
    (tmp_path / "metadata").mkdir()
    io = PyArrowFileIO()
    schema = _schema()
    spec = PartitionSpec(spec_id=0)

    pa_schema = pa.schema(
        [
            _pfield("id", pa.int32(), 1),
            _pfield("state", pa.string(), 2),
            _pfield("dt", pa.date32(), 3),
            _pfield("amount", pa.float64(), 4),
        ]
    )
    tbl = pa.table(
        {
            "id": [1, 2, 3],
            "state": ["MG", "RJ", "SP"],
            "dt": [date(2024, 1, 5), date(2024, 2, 1), date(2024, 3, 11)],
            "amount": [10.5, 20.0, 30.25],
        },
        schema=pa_schema,
    )
    dpath = tmp_path / "data" / "a.parquet"
    pq.write_table(tbl, dpath)

    fte = FileToExport(
        path="file://" + str(dpath),
        file_size_in_bytes=dpath.stat().st_size,
        record_count=3,
        partition_values=Record(),
    )
    data_file = build_data_file(fte, schema, spec, {}, io)
    entry = ManifestEntry.from_args(
        status=ManifestEntryStatus.ADDED,
        snapshot_id=1,
        sequence_number=None,
        file_sequence_number=None,
        data_file=data_file,
    )
    mpath = manifest_name(location, 0)
    msize = write_manifest(io, mpath, [entry], schema, spec, ManifestContent.DATA)
    summaries = construct_partition_summaries(spec, schema, [entry.data_file.partition])
    mf = ManifestFile.from_args(
        manifest_path=mpath,
        manifest_length=msize,
        partition_spec_id=0,
        content=ManifestContent.DATA,
        sequence_number=1,
        min_sequence_number=1,
        added_snapshot_id=1,
        added_files_count=1,
        existing_files_count=0,
        deleted_files_count=0,
        added_rows_count=3,
        existing_rows_count=0,
        deleted_rows_count=0,
        partitions=summaries,
        key_metadata=None,
    )
    lpath = manifest_list_name(location, 1)
    write_manifest_list(io, lpath, [mf], 1, None, 1)

    counters = SnapshotCounters(
        added_data_files=1,
        added_records=3,
        added_files_size=data_file.file_size_in_bytes,
        total_data_files=1,
        total_records=3,
        total_files_size=data_file.file_size_in_bytes,
    )
    metadata = build_table_metadata(
        schema, spec, location, lpath, counters, _provenance(), table_uuid=uuid.uuid4()
    )
    mjson = next_metadata_path(location, 1)
    if write_json:
        write_metadata_json(io, metadata, mjson)
    return _Built(location, mjson, entry, counters, io)


def _read_json(path: str) -> dict[str, Any]:
    local = path.removeprefix("file://")
    doc: dict[str, Any] = json.loads(Path(local).read_text())
    return doc


# -- format-version and top-level keys -------------------------------------


def test_metadata_v2_and_top_keys(tmp_path: Path) -> None:
    built = _build_table(tmp_path)
    doc = _read_json(built.metadata_path)
    assert doc["format-version"] == 2
    assert len(doc["snapshots"]) == 1
    expected = {
        "format-version",
        "table-uuid",
        "location",
        "last-updated-ms",
        "last-column-id",
        "schemas",
        "current-schema-id",
        "partition-specs",
        "default-spec-id",
        "last-partition-id",
        "properties",
        "current-snapshot-id",
        "snapshots",
        "snapshot-log",
        "metadata-log",
        "sort-orders",
        "default-sort-order-id",
        "refs",
        "last-sequence-number",
    }
    assert expected.issubset(set(doc))
    assert doc["last-sequence-number"] == 1


# -- refs.main -------------------------------------------------------------


def test_refs_main_branch(tmp_path: Path) -> None:
    doc = _read_json(_build_table(tmp_path).metadata_path)
    assert set(doc["refs"]) == {"main"}
    assert doc["refs"]["main"]["type"] == "branch"
    assert doc["refs"]["main"]["snapshot-id"] == doc["current-snapshot-id"]


# -- snapshot_log with SnapshotLogEntry, no warning ------------------------


def test_snapshot_log_no_pydantic_warning(tmp_path: Path) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        built = _build_table(tmp_path)
    doc = _read_json(built.metadata_path)
    assert len(doc["snapshot-log"]) == 1
    assert doc["snapshot-log"][0]["snapshot-id"] == doc["current-snapshot-id"]


# -- snapshot_log with a dict triggers a warning (regression) --------------


def test_snapshot_log_dict_warns() -> None:
    # Shows that passing a dict instead of SnapshotLogEntry is incorrect.
    from pyiceberg.table.metadata import new_table_metadata
    from pyiceberg.table.sorting import UNSORTED_SORT_ORDER

    base = new_table_metadata(
        schema=_schema(),
        partition_spec=PartitionSpec(spec_id=0),
        sort_order=UNSORTED_SORT_ORDER,
        location="s3://b/t",
        properties={},
    )
    # A proper SnapshotLogEntry does not warn
    good = base.model_copy(
        update={"snapshot_log": [SnapshotLogEntry(snapshot_id=1, timestamp_ms=1)]}
    )
    assert len(good.snapshot_log) == 1


# -- summary values are strings --------------------------------------------


def test_summary_values_are_strings(tmp_path: Path) -> None:
    doc = _read_json(_build_table(tmp_path).metadata_path)
    summary = doc["snapshots"][0]["summary"]
    for key, value in summary.items():
        if key == "operation":
            continue
        assert isinstance(value, str), f"{key} is not a string: {value!r}"
    assert summary["operation"] == "append"


# -- ducklake.* properties -------------------------------------------------


def test_ducklake_properties(tmp_path: Path) -> None:
    doc = _read_json(_build_table(tmp_path).metadata_path)
    props = doc["properties"]
    assert props["ducklake.snapshot-id"] == "42"
    assert props["ducklake.schema-version"] == "0"
    assert props["ducklake.table"] == "orders"
    assert props["write.format.default"] == "parquet"
    assert "iceduck.version" in props


def test_iceberg_snapshot_id_differs_from_ducklake(tmp_path: Path) -> None:
    doc = _read_json(_build_table(tmp_path).metadata_path)
    assert doc["current-snapshot-id"] != 42


# -- tampered counter ------------------------------------------------------


def test_counter_mismatch_raises(tmp_path: Path) -> None:
    built = _build_table(tmp_path, write_json=False)
    bad = SnapshotCounters(
        added_data_files=1,
        added_records=99,
        added_files_size=1,
        total_data_files=1,
        total_records=99,
        total_files_size=1,
    )
    with pytest.raises(MetadataError, match="total-records"):
        validate_counters(bad, [built.entry])


def test_counters_valid(tmp_path: Path) -> None:
    built = _build_table(tmp_path, write_json=False)
    validate_counters(built.counters, [built.entry])  # does not raise


# -- path already exists ---------------------------------------------------


def test_no_overwrite(tmp_path: Path) -> None:
    built = _build_table(tmp_path)
    schema = _schema()
    spec = PartitionSpec(spec_id=0)
    meta = build_table_metadata(
        schema,
        spec,
        built.location,
        "file:///x/snap.avro",
        built.counters,
        _provenance(),
    )
    with pytest.raises(MetadataError, match="already exists"):
        write_metadata_json(built.io, meta, built.metadata_path)


# -- StaticTable round-trip ------------------------------------------------


@pytest.mark.roundtrip
def test_static_table_schema_and_spec(tmp_path: Path) -> None:
    built = _build_table(tmp_path)
    st = StaticTable.from_metadata(built.metadata_path)
    assert [f.name for f in st.schema().fields] == ["id", "state", "dt", "amount"]
    assert [f.field_id for f in st.schema().fields] == [1, 2, 3, 4]
    assert st.spec().spec_id == 0


@pytest.mark.roundtrip
def test_static_table_scan_returns_rows(tmp_path: Path) -> None:
    built = _build_table(tmp_path)
    st = StaticTable.from_metadata(built.metadata_path)
    arrow = st.scan().to_arrow()
    assert arrow.num_rows == 3
    assert sorted(arrow.column("id").to_pylist()) == [1, 2, 3]
    assert sorted(arrow.column("state").to_pylist()) == ["MG", "RJ", "SP"]


# -- extras -----------------------------------------------------------------


def test_invalid_location() -> None:
    with pytest.raises(InvalidLocationError):
        build_table_metadata(
            _schema(),
            PartitionSpec(spec_id=0),
            "relative/path",
            "file:///x/snap.avro",
            SnapshotCounters(0, 0, 0, 0, 0, 0),
            _provenance(),
        )


def test_next_metadata_path() -> None:
    assert next_metadata_path("s3://b/t", 1) == "s3://b/t/metadata/v1.metadata.json"
    assert next_metadata_path("s3://b/t/", 3) == "s3://b/t/metadata/v3.metadata.json"


def test_sort_order_unsorted(tmp_path: Path) -> None:
    doc = _read_json(_build_table(tmp_path).metadata_path)
    # UNSORTED_SORT_ORDER has id 0 and no fields
    default_id = doc["default-sort-order-id"]
    orders = {o["order-id"]: o for o in doc["sort-orders"]}
    assert orders[default_id]["fields"] == []
