"""Incremental commit of an Iceberg v2 snapshot.

Turns "what is live in DuckLake at ``S``" and "what is live in the current
Iceberg snapshot" into a new snapshot, at a cost proportional to the delta.
The computation is a **set difference**: a DuckLake merge deletes the merged
files' rows from ``ducklake_data_file`` and expire may remove the previous
``S``, so ``begin``/``end_snapshot`` ranges are not enough.

For each manifest in the base snapshot:

- no removed path => **reused** by reference in the manifest list;
- some removed path => **rewritten**: live entries become ``EXISTING``
  (keeping their original ``snapshot_id`` and sequence numbers) and the
  removed ones become ``DELETED`` in the new snapshot;
- no live entry (only ``DELETED`` from old snapshots) => **dropped**.

New files go into new manifests, one per (content, spec), with ``ADDED``
entries and null sequence numbers (v2 inheritance). Data and deletes never
share a manifest.

Before writing the manifest list, the reconstructed state is checked against
the target -- the safety net against duplicating or losing files.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pyiceberg.conversions import from_bytes
from pyiceberg.manifest import (
    DataFileContent,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    construct_partition_summaries,
)
from pyiceberg.table.snapshots import Operation, Summary
from pyiceberg.types import StringType

from iceduck import timing
from iceduck.catalog.files import canonical
from iceduck.concurrency import ContextThreadPoolExecutor, io_workers
from iceduck.deletes.convert import parse_source_key
from iceduck.deletes.writer import POSITION_DELETE_SCHEMA
from iceduck.errors import IceduckError
from iceduck.obs.metrics import add_work
from iceduck.writer.avro import (
    StreamingReadFileIO,
    manifest_list_name,
    manifest_name,
    upload_all,
)
from iceduck.writer.manifest import encode_manifest
from iceduck.writer.manifest_list import encode_manifest_list

if TYPE_CHECKING:
    from pyiceberg.io import FileIO
    from pyiceberg.manifest import DataFile
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.table.metadata import TableMetadata

LOG = logging.getLogger("iceduck")

#: default for concurrent reads of base snapshot manifests; the effective
#: value comes from ``max_io_workers``
_READ_WORKERS = 8

_FILE_PATH_ID = POSITION_DELETE_SCHEMA.find_field("file_path").field_id


class CommitError(IceduckError):
    """Failed to build the incremental snapshot (exit 1)."""


class DuplicatePathError(CommitError):
    """File to add is already live in the base snapshot."""


class MissingPathError(CommitError):
    """File to remove is not live in the base snapshot."""


class ReconstructionError(CommitError):
    """The reconstructed state is not the target state; nothing is published."""


# -- file identity -----------------------------------------------------------


def data_key(data_file: DataFile) -> str:
    """Key of a data file: the canonicalized path."""
    return canonical(data_file.file_path)


def delete_key(data_file: DataFile) -> str:
    """Key of a delete file: the DuckLake source key in its name."""
    return parse_source_key(data_file.file_path) or canonical(data_file.file_path)


def referenced_data_file(data_file: DataFile) -> str | None:
    """Data file referenced by a position delete (``file_path`` bounds)."""
    lower = (data_file.lower_bounds or {}).get(_FILE_PATH_ID)
    upper = (data_file.upper_bounds or {}).get(_FILE_PATH_ID)
    if lower is None or lower != upper:
        return None
    return str(from_bytes(StringType(), lower))


def _key(entry: ManifestEntry) -> str:
    if entry.data_file.content == DataFileContent.DATA:
        return data_key(entry.data_file)
    return delete_key(entry.data_file)


# -- base state --------------------------------------------------------------


@dataclass(frozen=True)
class BaseSnapshot:
    """Manifests and entries of the current Iceberg snapshot."""

    manifests: tuple[ManifestFile, ...]
    entries: Mapping[str, tuple[ManifestEntry, ...]]  # manifest_path -> entries

    @classmethod
    def empty(cls) -> BaseSnapshot:
        return cls(manifests=(), entries={})

    def live(self, content: ManifestContent) -> dict[str, ManifestEntry]:
        """Live entries (``ADDED``/``EXISTING``) by key."""
        out: dict[str, ManifestEntry] = {}
        for manifest in self.manifests:
            if manifest.content != content:
                continue
            for entry in self.entries[manifest.manifest_path]:
                if entry.status == ManifestEntryStatus.DELETED:
                    continue
                key = _key(entry)
                if key in out:
                    raise CommitError(f"duplicate live file in base snapshot: {key}")
                out[key] = entry
        return out


def load_base_snapshot(io: FileIO, metadata: TableMetadata | None) -> BaseSnapshot:
    """Read the manifest list and manifests of the current snapshot."""
    if metadata is None:
        return BaseSnapshot.empty()
    snapshot = metadata.current_snapshot()
    if snapshot is None:
        return BaseSnapshot.empty()
    reader = StreamingReadFileIO(io)
    manifests = tuple(snapshot.manifests(reader))

    def fetch(m: ManifestFile) -> tuple[ManifestEntry, ...]:
        return tuple(m.fetch_manifest_entry(reader, discard_deleted=False))

    # one GET per manifest, in parallel: the number of manifests grows with
    # every sync that has new files
    workers = io_workers(len(manifests))
    with ContextThreadPoolExecutor(max_workers=workers) as pool:
        fetched = list(pool.map(fetch, manifests))
    entries = {m.manifest_path: e for m, e in zip(manifests, fetched, strict=True)}
    return BaseSnapshot(manifests=manifests, entries=entries)


# -- delta -------------------------------------------------------------------


@dataclass(frozen=True)
class ContentChange:
    """Files to add and keys to remove for one content kind (data or deletes)."""

    added: tuple[tuple[int, DataFile], ...] = ()  # (spec_id, DataFile)
    removed: frozenset[str] = frozenset()  # keys (data_key/delete_key)

    @property
    def is_empty(self) -> bool:
        return not self.added and not self.removed


@dataclass
class CommitCounters:
    """``summary`` counters (string values when published)."""

    added_data_files: int = 0
    deleted_data_files: int = 0
    added_records: int = 0
    deleted_records: int = 0
    added_files_size: int = 0
    removed_files_size: int = 0
    added_delete_files: int = 0
    removed_delete_files: int = 0
    added_position_deletes: int = 0
    removed_position_deletes: int = 0
    total_data_files: int = 0
    total_delete_files: int = 0
    total_records: int = 0
    total_files_size: int = 0
    total_position_deletes: int = 0

    def as_summary(self) -> dict[str, str]:
        return {
            "added-data-files": str(self.added_data_files),
            "deleted-data-files": str(self.deleted_data_files),
            "added-records": str(self.added_records),
            "deleted-records": str(self.deleted_records),
            "added-files-size": str(self.added_files_size),
            "removed-files-size": str(self.removed_files_size),
            "added-delete-files": str(self.added_delete_files),
            "removed-delete-files": str(self.removed_delete_files),
            "added-position-deletes": str(self.added_position_deletes),
            "removed-position-deletes": str(self.removed_position_deletes),
            "total-data-files": str(self.total_data_files),
            "total-delete-files": str(self.total_delete_files),
            "total-records": str(self.total_records),
            "total-files-size": str(self.total_files_size),
            "total-position-deletes": str(self.total_position_deletes),
            "total-equality-deletes": "0",
        }


def classify_operation(
    data: ContentChange,
    deletes: ContentChange,
    base: BaseSnapshot,
    *,
    compaction_only: bool,
) -> Operation:
    """``operation`` of the snapshot.

    - ``replace``: DuckLake only compacted (merge/rewrite) -- same rows,
      different files;
    - ``append``: only new data files;
    - ``delete``: no new data file; data files left and/or delete files
      came in, and every removed delete file goes with its removed data file;
    - ``overwrite``: everything else (e.g. ``UPDATE``, or a cumulative delete
      file replaced by another).
    """
    if compaction_only:
        return Operation.REPLACE
    if data.added and not data.removed and deletes.is_empty:
        return Operation.APPEND
    if not data.added:
        live_deletes = base.live(ManifestContent.DELETES)
        removed_data = data.removed
        orphan = [
            key
            for key in deletes.removed
            if (ref := referenced_data_file(live_deletes[key].data_file)) is None
            or canonical(ref) not in removed_data
        ]
        if not orphan:
            return Operation.DELETE
    return Operation.OVERWRITE


# -- writing -----------------------------------------------------------------


@dataclass(frozen=True)
class SnapshotFiles:
    """Write result: manifest list and snapshot bookkeeping."""

    manifest_list_path: str
    manifest_paths: tuple[str, ...]  # only those written now
    reused_manifests: int
    rewritten_manifests: int
    dropped_manifests: int
    counters: CommitCounters
    summary_properties: dict[str, str] = field(default_factory=dict)


def _schema_for_spec(
    spec: PartitionSpec, schema: Schema, fallbacks: Sequence[Schema]
) -> Schema:
    """Schema able to resolve the spec (a partition source may be gone)."""
    sources = {f.source_id for f in spec.fields}
    for candidate in (schema, *reversed(fallbacks)):
        if sources <= candidate.field_ids:
            return candidate
    raise CommitError(f"no schema resolves partition spec {spec.spec_id}")


def _manifest_file(
    path: str,
    size: int,
    spec: PartitionSpec,
    schema: Schema,
    content: ManifestContent,
    entries: Sequence[ManifestEntry],
    *,
    snapshot_id: int,
    sequence_number: int,
) -> ManifestFile:
    def count(status: ManifestEntryStatus) -> tuple[int, int]:
        chosen = [e for e in entries if e.status == status]
        return len(chosen), sum(e.data_file.record_count for e in chosen)

    added, added_rows = count(ManifestEntryStatus.ADDED)
    existing, existing_rows = count(ManifestEntryStatus.EXISTING)
    deleted, deleted_rows = count(ManifestEntryStatus.DELETED)
    live_seqs = [
        e.sequence_number
        for e in entries
        if e.status == ManifestEntryStatus.EXISTING and e.sequence_number is not None
    ]
    if added:
        live_seqs.append(sequence_number)
    return ManifestFile.from_args(
        manifest_path=path,
        manifest_length=size,
        partition_spec_id=spec.spec_id,
        content=content,
        sequence_number=sequence_number,
        min_sequence_number=min(live_seqs, default=sequence_number),
        added_snapshot_id=snapshot_id,
        added_files_count=added,
        existing_files_count=existing,
        deleted_files_count=deleted,
        added_rows_count=added_rows,
        existing_rows_count=existing_rows,
        deleted_rows_count=deleted_rows,
        partitions=construct_partition_summaries(
            spec, schema, [e.data_file.partition for e in entries]
        ),
        key_metadata=None,
    )


class _ManifestBuilder:
    def __init__(
        self,
        io: FileIO,
        location: str,
        schema: Schema,
        specs: Mapping[int, PartitionSpec],
        schemas: Sequence[Schema],
        snapshot_id: int,
        sequence_number: int,
    ) -> None:
        self.io = io
        self.location = location
        self.schema = schema
        self.specs = specs
        self.schemas = schemas
        self.snapshot_id = snapshot_id
        self.sequence_number = sequence_number
        self.ordinal = 0
        self.written: list[str] = []
        #: encoded and verified (path, bytes), uploaded together at the end
        self.pending: list[tuple[str, bytes]] = []

    def write(
        self, spec_id: int, content: ManifestContent, entries: list[ManifestEntry]
    ) -> ManifestFile:
        spec = self.specs.get(spec_id)
        if spec is None:
            raise CommitError(f"partition spec {spec_id} missing from metadata")
        schema = _schema_for_spec(spec, self.schema, self.schemas)
        path = manifest_name(self.location, self.ordinal)
        self.ordinal += 1
        with timing.phase("manifest"):
            payload = encode_manifest(path, entries, schema, spec, content)
            add_work("manifest", items=len(entries), byte_count=len(payload))
        self.pending.append((path, payload))
        self.written.append(path)
        return _manifest_file(
            path,
            len(payload),
            spec,
            schema,
            content,
            entries,
            snapshot_id=self.snapshot_id,
            sequence_number=self.sequence_number,
        )


def _added_entry(data_file: DataFile) -> ManifestEntry:
    return ManifestEntry.from_args(
        status=ManifestEntryStatus.ADDED,
        snapshot_id=None,
        sequence_number=None,
        file_sequence_number=None,
        data_file=data_file,
    )


def _carried_entry(
    entry: ManifestEntry, status: ManifestEntryStatus, snapshot_id: int
) -> ManifestEntry:
    """``EXISTING`` keeps the origin snapshot; ``DELETED`` is this snapshot's.

    Both keep the original sequence numbers, explicit: the data sequence
    decides how deletes apply, and it does not change when the manifest is
    rewritten.
    """
    return ManifestEntry.from_args(
        status=status,
        snapshot_id=(
            snapshot_id if status == ManifestEntryStatus.DELETED else entry.snapshot_id
        ),
        sequence_number=entry.sequence_number,
        file_sequence_number=entry.file_sequence_number,
        data_file=entry.data_file,
    )


def write_snapshot(
    io: FileIO,
    location: str,
    base: BaseSnapshot,
    *,
    data: ContentChange,
    deletes: ContentChange,
    schema: Schema,
    specs: Mapping[int, PartitionSpec],
    schemas: Sequence[Schema] = (),
    snapshot_id: int,
    parent_snapshot_id: int | None,
    sequence_number: int,
    expected_data: frozenset[str],
    expected_deletes: frozenset[str],
) -> SnapshotFiles:
    """Write new/rewritten manifests and the snapshot's manifest list."""
    changes = {ManifestContent.DATA: data, ManifestContent.DELETES: deletes}
    _check_change(base, ManifestContent.DATA, data)
    _check_change(base, ManifestContent.DELETES, deletes)

    builder = _ManifestBuilder(
        io, location, schema, specs, schemas, snapshot_id, sequence_number
    )
    new_manifests: list[ManifestFile] = []
    kept: list[ManifestFile] = []
    reused = rewritten = dropped = 0
    final_entries: list[ManifestEntry] = []  # all resulting live entries

    # new files: one manifest per (content, spec)
    for content, change in changes.items():
        by_spec: dict[int, list[ManifestEntry]] = {}
        for spec_id, data_file in change.added:
            by_spec.setdefault(spec_id, []).append(_added_entry(data_file))
        for spec_id in sorted(by_spec):
            new_manifests.append(builder.write(spec_id, content, by_spec[spec_id]))
            final_entries.extend(by_spec[spec_id])

    # base snapshot manifests: reuse, rewrite or drop
    for manifest in base.manifests:
        entries = base.entries[manifest.manifest_path]
        live = [e for e in entries if e.status != ManifestEntryStatus.DELETED]
        removed_keys = changes[manifest.content].removed
        hit = [e for e in live if _key(e) in removed_keys]
        if not live:
            dropped += 1
            continue
        if not hit:
            kept.append(manifest)
            final_entries.extend(live)
            reused += 1
            continue
        rewritten_entries = [
            _carried_entry(
                e,
                (
                    ManifestEntryStatus.DELETED
                    if _key(e) in removed_keys
                    else ManifestEntryStatus.EXISTING
                ),
                snapshot_id,
            )
            for e in live
        ]
        new_manifests.append(
            builder.write(
                manifest.partition_spec_id, manifest.content, rewritten_entries
            )
        )
        final_entries.extend(
            e for e in rewritten_entries if e.status != ManifestEntryStatus.DELETED
        )
        rewritten += 1

    _verify_reconstruction(final_entries, expected_data, expected_deletes)

    manifests = [*new_manifests, *kept]
    list_path = manifest_list_name(location, snapshot_id)
    with timing.phase("manifest_list"):
        list_payload = encode_manifest_list(
            list_path, manifests, snapshot_id, parent_snapshot_id, sequence_number
        )
        add_work("manifest_list", items=len(manifests), byte_count=len(list_payload))
    # Nothing references these files until metadata.json (written later): the
    # manifests and the manifest list are uploaded together, in parallel.
    # uuid4 names make the existence check unnecessary.
    upload_all(io, [*builder.pending, (list_path, list_payload)])
    LOG.info(
        "snapshot %d: %d new/rewritten manifest(s), %d reused, %d dropped",
        snapshot_id,
        len(new_manifests),
        reused,
        dropped,
    )
    return SnapshotFiles(
        manifest_list_path=list_path,
        manifest_paths=tuple(builder.written),
        reused_manifests=reused,
        rewritten_manifests=rewritten,
        dropped_manifests=dropped,
        counters=_counters(base, data, deletes, final_entries),
    )


def _check_change(
    base: BaseSnapshot, content: ManifestContent, change: ContentChange
) -> None:
    live = base.live(content)
    for key in change.removed:
        if key not in live:
            raise MissingPathError(f"removal of a file that is not live: {key}")
    for _spec_id, data_file in change.added:
        key = (
            data_key(data_file)
            if content == ManifestContent.DATA
            else delete_key(data_file)
        )
        if key in live and key not in change.removed:
            raise DuplicatePathError(f"file already published: {data_file.file_path}")


def _verify_reconstruction(
    entries: Sequence[ManifestEntry],
    expected_data: frozenset[str],
    expected_deletes: frozenset[str],
) -> None:
    got_data = [_key(e) for e in entries if e.data_file.content == DataFileContent.DATA]
    got_deletes = [
        _key(e) for e in entries if e.data_file.content != DataFileContent.DATA
    ]
    for label, got, expected in (
        ("data files", got_data, expected_data),
        ("delete files", got_deletes, expected_deletes),
    ):
        if len(got) != len(set(got)):
            raise ReconstructionError(f"duplicate {label} in the new snapshot")
        if set(got) != expected:
            missing = sorted(expected - set(got))[:5]
            extra = sorted(set(got) - expected)[:5]
            raise ReconstructionError(
                f"{label} in the new snapshot differ from DuckLake; "
                f"missing {missing}, extra {extra}"
            )


def _counters(
    base: BaseSnapshot,
    data: ContentChange,
    deletes: ContentChange,
    final_entries: Sequence[ManifestEntry],
) -> CommitCounters:
    live_data = base.live(ManifestContent.DATA)
    live_deletes = base.live(ManifestContent.DELETES)
    removed_data = [live_data[k].data_file for k in data.removed]
    removed_deletes = [live_deletes[k].data_file for k in deletes.removed]
    final_data = [
        e.data_file
        for e in final_entries
        if e.data_file.content == DataFileContent.DATA
    ]
    final_deletes = [
        e.data_file
        for e in final_entries
        if e.data_file.content != DataFileContent.DATA
    ]
    return CommitCounters(
        added_data_files=len(data.added),
        deleted_data_files=len(removed_data),
        added_records=sum(f.record_count for _s, f in data.added),
        deleted_records=sum(f.record_count for f in removed_data),
        added_files_size=sum(f.file_size_in_bytes for _s, f in data.added)
        + sum(f.file_size_in_bytes for _s, f in deletes.added),
        removed_files_size=sum(f.file_size_in_bytes for f in removed_data)
        + sum(f.file_size_in_bytes for f in removed_deletes),
        added_delete_files=len(deletes.added),
        removed_delete_files=len(removed_deletes),
        added_position_deletes=sum(f.record_count for _s, f in deletes.added),
        removed_position_deletes=sum(f.record_count for f in removed_deletes),
        total_data_files=len(final_data),
        total_delete_files=len(final_deletes),
        total_records=sum(f.record_count for f in final_data),
        total_files_size=sum(f.file_size_in_bytes for f in final_data)
        + sum(f.file_size_in_bytes for f in final_deletes),
        total_position_deletes=sum(f.record_count for f in final_deletes),
    )


def build_summary(
    operation: Operation, counters: CommitCounters, properties: Mapping[str, str]
) -> Summary:
    """Snapshot ``summary``: counters plus the state mirror."""
    return Summary(operation, **{**counters.as_summary(), **properties})
