"""Edge cases of the incremental commit, delete conversion and schema evolution.

The happy path is covered end to end in tests/integration; this file holds the
safety nets the happy path never triggers.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.table.snapshots import Operation, Summary
from pyiceberg.typedef import Record
from pyiceberg.types import (
    DecimalType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
)

from iceduck.catalog.files import LiveDeleteFile
from iceduck.deletes.convert import (
    DeleteConversionError,
    convert_delete_file,
    delete_file_path,
    parse_source_key,
    read_visible_positions,
    source_key,
)
from iceduck.deletes.writer import InvalidDeletePositionError
from iceduck.errors import CompatibilityError
from iceduck.export import _evolve_schema
from iceduck.writer.commit import (
    BaseSnapshot,
    CommitError,
    ContentChange,
    DuplicatePathError,
    MissingPathError,
    ReconstructionError,
    classify_operation,
    delete_key,
    referenced_data_file,
    write_snapshot,
)
from iceduck.writer.metadata import (
    DuckLakeProvenance,
    SnapshotCounters,
    build_next_metadata,
    build_table_metadata,
)

SCHEMA = Schema(NestedField(1, "id", IntegerType(), required=False))
SPEC = PartitionSpec(spec_id=0)


def _data_file(path: str, records: int = 1) -> DataFile:
    return DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=path,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=records,
        file_size_in_bytes=10,
    )


def _entry(df: DataFile, status: ManifestEntryStatus) -> ManifestEntry:
    return ManifestEntry.from_args(
        status=status,
        snapshot_id=1,
        sequence_number=1,
        file_sequence_number=1,
        data_file=df,
    )


def _base(*entries: ManifestEntry, content: ManifestContent) -> BaseSnapshot:
    from pyiceberg.manifest import ManifestFile

    manifest = ManifestFile.from_args(
        manifest_path="/m.avro",
        manifest_length=1,
        partition_spec_id=0,
        content=content,
        sequence_number=1,
        min_sequence_number=1,
        added_snapshot_id=1,
    )
    return BaseSnapshot(manifests=(manifest,), entries={"/m.avro": entries})


# -- keys and bounds -----------------------------------------------------------


def test_source_key_round_trips_through_file_name() -> None:
    key = source_key("s3://b/t/ducklake-1-delete.parquet")
    assert len(key) == 24
    path = delete_file_path("s3://b/ice/t", key)
    assert parse_source_key(path) == key
    assert parse_source_key("s3://b/ice/t/deletes/other.parquet") is None


def test_delete_key_falls_back_to_path_for_foreign_files() -> None:
    df = _data_file("s3://b//x/deletes/foreign.parquet")
    assert delete_key(df) == "s3://b/x/deletes/foreign.parquet"


def test_referenced_data_file_requires_equal_bounds() -> None:
    assert referenced_data_file(_data_file("s3://b/d.parquet")) is None


# -- commit invariants -------------------------------------------------------


def test_removing_unknown_file_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(MissingPathError):
        write_snapshot(
            PyArrowFileIO(),
            str(tmp_path),
            BaseSnapshot.empty(),
            data=ContentChange(removed=frozenset({"s3://b/ghost.parquet"})),
            deletes=ContentChange(),
            schema=SCHEMA,
            specs={0: SPEC},
            snapshot_id=2,
            parent_snapshot_id=None,
            sequence_number=1,
            expected_data=frozenset(),
            expected_deletes=frozenset(),
        )


def test_adding_live_file_twice_is_rejected(tmp_path: Path) -> None:
    df = _data_file("s3://b/a.parquet")
    base = _base(_entry(df, ManifestEntryStatus.ADDED), content=ManifestContent.DATA)
    with pytest.raises(DuplicatePathError):
        write_snapshot(
            PyArrowFileIO(),
            str(tmp_path),
            base,
            data=ContentChange(added=((0, df),)),
            deletes=ContentChange(),
            schema=SCHEMA,
            specs={0: SPEC},
            snapshot_id=2,
            parent_snapshot_id=1,
            sequence_number=2,
            expected_data=frozenset({"s3://b/a.parquet"}),
            expected_deletes=frozenset(),
        )


def test_reconstruction_mismatch_aborts_before_manifest_list(tmp_path: Path) -> None:
    df = _data_file("s3://b/a.parquet")
    with pytest.raises(ReconstructionError, match="missing"):
        write_snapshot(
            PyArrowFileIO(),
            str(tmp_path),
            BaseSnapshot.empty(),
            data=ContentChange(added=((0, df),)),
            deletes=ContentChange(),
            schema=SCHEMA,
            specs={0: SPEC},
            snapshot_id=2,
            parent_snapshot_id=None,
            sequence_number=1,
            expected_data=frozenset({"s3://b/a.parquet", "s3://b/missing.parquet"}),
            expected_deletes=frozenset(),
        )
    assert not list(tmp_path.rglob("snap-*.avro"))


def test_unknown_spec_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(CommitError, match="spec 7"):
        write_snapshot(
            PyArrowFileIO(),
            str(tmp_path),
            BaseSnapshot.empty(),
            data=ContentChange(added=((7, _data_file("s3://b/a.parquet")),)),
            deletes=ContentChange(),
            schema=SCHEMA,
            specs={0: SPEC},
            snapshot_id=2,
            parent_snapshot_id=None,
            sequence_number=1,
            expected_data=frozenset({"s3://b/a.parquet"}),
            expected_deletes=frozenset(),
        )


def test_duplicate_live_entry_in_base_is_an_error() -> None:
    df = _data_file("s3://b/a.parquet")
    base = _base(
        _entry(df, ManifestEntryStatus.ADDED),
        _entry(df, ManifestEntryStatus.EXISTING),
        content=ManifestContent.DATA,
    )
    with pytest.raises(CommitError, match="duplicate"):
        base.live(ManifestContent.DATA)


# -- operation -----------------------------------------------------------------


def test_classify_operation() -> None:
    empty = BaseSnapshot.empty()
    added = ContentChange(added=((0, _data_file("s3://b/n.parquet")),))
    assert (
        classify_operation(added, ContentChange(), empty, compaction_only=False)
        is Operation.APPEND
    )
    assert (
        classify_operation(added, ContentChange(), empty, compaction_only=True)
        is Operation.REPLACE
    )
    delete_added = ContentChange(added=((0, _data_file("s3://b/d.parquet")),))
    assert (
        classify_operation(ContentChange(), delete_added, empty, compaction_only=False)
        is Operation.DELETE
    )
    # delete file removed without its data file: cumulative replacement
    df = _data_file("s3://b/deletes/x.parquet")
    base = _base(_entry(df, ManifestEntryStatus.ADDED), content=ManifestContent.DELETES)
    removed = ContentChange(removed=frozenset({delete_key(df)}))
    assert (
        classify_operation(ContentChange(), removed, base, compaction_only=False)
        is Operation.OVERWRITE
    )


# -- delete file conversion --------------------------------------------------


def _ducklake_delete(tmp_path: Path, positions: list[int], snaps: list[int]) -> Path:
    def field(name: str, typ: pa.DataType, fid: int) -> pa.Field:
        return pa.field(
            name, typ, nullable=False, metadata={b"PARQUET:field_id": str(fid).encode()}
        )

    schema = pa.schema(
        [
            field("file_path", pa.string(), 2147483646),
            field("pos", pa.int64(), 2147483645),
            field("_ducklake_internal_snapshot_id", pa.int64(), 2147483539),
        ]
    )
    path = tmp_path / "dl-delete.parquet"
    pq.write_table(
        pa.table(
            {
                "file_path": ["/d.parquet"] * len(positions),
                "pos": positions,
                "_ducklake_internal_snapshot_id": snaps,
            },
            schema=schema,
        ),
        path,
    )
    return path


def _live_delete(path: Path) -> LiveDeleteFile:
    return LiveDeleteFile(
        delete_file_id=1,
        path=str(path),
        canonical_path=str(path),
        file_size_in_bytes=path.stat().st_size,
        delete_count=3,
        file_format="parquet",
    )


def test_positions_filtered_by_snapshot(tmp_path: Path) -> None:
    path = _ducklake_delete(tmp_path, [5, 1, 3, 1], [2, 2, 4, 2])
    positions = read_visible_positions(
        PyArrowFileIO(), _live_delete(path), snapshot_id=3, record_count=10
    )
    assert positions == [1, 5]  # without the snapshot 4 row and without repetition


def test_position_out_of_range_is_rejected(tmp_path: Path) -> None:
    path = _ducklake_delete(tmp_path, [0, 10], [1, 1])
    with pytest.raises(InvalidDeletePositionError):
        read_visible_positions(
            PyArrowFileIO(), _live_delete(path), snapshot_id=1, record_count=10
        )


def test_delete_file_without_pos_column(tmp_path: Path) -> None:
    path = tmp_path / "bad.parquet"
    pq.write_table(pa.table({"x": [1]}), path)
    with pytest.raises(DeleteConversionError):
        read_visible_positions(
            PyArrowFileIO(), _live_delete(path), snapshot_id=1, record_count=10
        )


def test_no_visible_positions_writes_nothing(tmp_path: Path) -> None:
    path = _ducklake_delete(tmp_path, [1], [9])
    out = tmp_path / "ice"
    assert (
        convert_delete_file(
            PyArrowFileIO(),
            str(out),
            _live_delete(path),
            data_file_path="/d.parquet",
            record_count=10,
            partition=Record(),
            spec=SPEC,
            snapshot_id=1,
        )
        is None
    )
    assert not out.exists()


# -- schema evolution ---------------------------------------------------------


def _meta(schema: Schema) -> TableMetadataV2:
    meta = build_table_metadata(
        schema,
        SPEC,
        "s3://b/t",
        "s3://b/t/metadata/snap.avro",
        SnapshotCounters(0, 0, 0, 0, 0, 0),
        DuckLakeProvenance("lake", "main", "t", 1, 1, 0, "2026-01-01T00:00:00Z"),
    )
    assert isinstance(meta, TableMetadataV2)
    return meta


def test_evolve_schema_allows_promotions_and_keeps_optional() -> None:
    base = _meta(
        Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "p", DecimalType(10, 2), required=False),
        )
    )
    new = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "p", DecimalType(12, 2), required=False),
        NestedField(3, "n", StringType(), required=True),
    )
    evolved = _evolve_schema(new, base)
    assert evolved.find_field(1).field_type == LongType()
    assert not evolved.find_field(1).required  # was optional: stays optional
    assert evolved.find_field(3).required  # new column keeps NOT NULL


def test_evolve_schema_rejects_incompatible_type_change() -> None:
    base = _meta(Schema(NestedField(1, "id", IntegerType(), required=False)))
    with pytest.raises(CompatibilityError, match="promotion"):
        _evolve_schema(Schema(NestedField(1, "id", StringType(), required=False)), base)


def test_metadata_only_update_has_no_new_snapshot() -> None:
    base = _meta(SCHEMA)
    evolved = Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(2, "x", StringType(), required=False),
        schema_id=5,
    )
    nxt = build_next_metadata(
        base,
        "s3://b/t/metadata/v1.metadata.json",
        schema=evolved,
        specs=[SPEC],
        default_spec_id=0,
        manifest_list_path=None,
        summary=None,
        snapshot_id=99,
        properties={"ducklake.snapshot-id": "7"},
    )
    assert nxt.current_snapshot_id == base.current_snapshot_id
    assert len(nxt.snapshots) == 1
    assert nxt.current_schema_id == 5
    assert nxt.last_sequence_number == base.last_sequence_number
    assert nxt.properties["ducklake.snapshot-id"] == "7"
    assert [e.metadata_file for e in nxt.metadata_log] == [
        "s3://b/t/metadata/v1.metadata.json"
    ]


def _next(base: TableMetadataV2, snapshot_id: int, **kw: object) -> TableMetadataV2:
    return build_next_metadata(
        base,
        f"s3://b/t/metadata/v{snapshot_id}.metadata.json",
        schema=SCHEMA,
        specs=[SPEC],
        default_spec_id=0,
        manifest_list_path=f"s3://b/t/metadata/snap-{snapshot_id}.avro",
        summary=Summary(Operation.APPEND),
        snapshot_id=snapshot_id,
        properties={},
        **kw,  # type: ignore[arg-type]
    )


def _aged(meta: TableMetadataV2, hours: float) -> TableMetadataV2:
    """Shift all of ``meta``'s history back in time by ``hours`` hours."""
    shift = int(hours * 3_600_000)
    return meta.model_copy(
        update={
            "last_updated_ms": meta.last_updated_ms - shift,
            "snapshots": [
                s.model_copy(update={"timestamp_ms": s.timestamp_ms - shift})
                for s in meta.snapshots
            ],
            "snapshot_log": [
                e.model_copy(update={"timestamp_ms": e.timestamp_ms - shift})
                for e in meta.snapshot_log
            ],
            "metadata_log": [
                e.model_copy(update={"timestamp_ms": e.timestamp_ms - shift})
                for e in meta.metadata_log
            ],
        }
    )


def test_retention_expires_old_history_and_keeps_base_entry() -> None:
    old = _aged(_next(_meta(SCHEMA), 2), hours=48)  # snapshots 1 and 2, 48 h ago
    nxt = _next(old, 3, retention_ms=24 * 3_600_000)
    assert [s.snapshot_id for s in nxt.snapshots] == [3]
    assert [e.snapshot_id for e in nxt.snapshot_log] == [3]
    assert nxt.current_snapshot_id == 3
    assert (
        nxt.snapshots[0].parent_snapshot_id == 2
    )  # cites the expired one, as in Iceberg
    # the old log is dropped, but this commit's base stays (the next export checks it)
    assert [e.metadata_file for e in nxt.metadata_log] == [
        "s3://b/t/metadata/v3.metadata.json"
    ]


def test_retention_keeps_recent_and_current_snapshots() -> None:
    old = _aged(_next(_meta(SCHEMA), 2), hours=1)
    nxt = _next(old, 3, retention_ms=24 * 3_600_000)
    assert len(nxt.snapshots) == 3
    assert len(nxt.metadata_log) == 2

    # with no new snapshot (schema only), the current one stays despite retention
    stale = _aged(_meta(SCHEMA), hours=48)
    only_schema = build_next_metadata(
        stale,
        "s3://b/t/metadata/v1.metadata.json",
        schema=SCHEMA,
        specs=[SPEC],
        default_spec_id=0,
        manifest_list_path=None,
        summary=None,
        snapshot_id=99,
        properties={},
        retention_ms=3_600_000,
    )
    assert [s.snapshot_id for s in only_schema.snapshots] == [stale.current_snapshot_id]


def test_without_retention_history_is_kept() -> None:
    old = _aged(_next(_meta(SCHEMA), 2), hours=48)
    assert len(_next(old, 3).snapshots) == 3
