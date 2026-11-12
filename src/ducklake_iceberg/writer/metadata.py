"""Assembly and writing of the v2 ``metadata.json``.

Builds the complete v2 ``TableMetadata`` (schema, partition spec, sort order,
snapshot, ``snapshot-log``, ``refs`` and ``ducklake.*`` properties) and writes
it as ``metadata.json`` on S3. It is the export's **commit point**: the
``metadata.json`` is the last file written, because it is what Glue points to
and what Athena reads first. The order is manifests -> manifest list ->
``metadata.json`` -> ``register_table``; any failure before registration
leaves only garbage on S3, never an invalid table. No write overwrites.

Design decisions:

- ``TableMetadataV2`` is built **directly**, never through
  ``new_table_metadata``: that calls ``assign_fresh_schema_ids`` and
  ``assign_fresh_partition_spec_ids``, which renumber field IDs and spec IDs.
  With a nested struct or a dropped column in DuckLake, the renumbered IDs no
  longer match the ``field_id`` values stored in the Parquet files -- columns
  silently swapped or null -- and the manifest ends up citing a
  ``partition_spec_id`` missing from the ``metadata.json``.
- ``UNSORTED_SORT_ORDER`` is mandatory: declaring a false ordering would
  allow the reader to skip data.
- The Iceberg ``snapshot_id`` is a random 64-bit integer, **never** the
  DuckLake one (independent namespaces; a retry needs a new ID). The DuckLake
  one is stored as the ``ducklake.snapshot-id`` property, which is what makes
  incremental sync possible.
- The snapshot is stitched in via ``model_copy`` updating five coherent
  fields; ``snapshot_log`` requires ``SnapshotLogEntry`` (a dict triggers
  ``PydanticSerializationUnexpectedValue``) and ``refs.main`` is mandatory.

Sync state:

- ``ducklake.*`` / ``ducklake_iceberg.*`` properties are written in full (never as a
  delta) and mirrored in the snapshot ``summary``.
- Old managed properties are **removed** (avoids orphans from a previous
  version).
- Unmanaged properties (``write.*``, user config) are **preserved intact**.
"""

from __future__ import annotations

import secrets
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyiceberg.partitioning import PARTITION_FIELD_ID_START
from pyiceberg.serializers import Compressor, FromByteStream
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.table.refs import MAIN_BRANCH, SnapshotRef, SnapshotRefType
from pyiceberg.table.snapshots import (
    MetadataLogEntry,
    Operation,
    Snapshot,
    SnapshotLogEntry,
    Summary,
)
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.utils.config import Config

from ducklake_iceberg import __version__, timing
from ducklake_iceberg.obs.metrics import add_call, add_work
from ducklake_iceberg.sync.state import (
    MANAGED_PREFIXES,
    PROP_SCHEMA_VERSION,
    PROP_SNAPSHOT_ID,
    PROP_TABLE_ID,
)

if TYPE_CHECKING:
    from pyiceberg.io import FileIO
    from pyiceberg.manifest import ManifestEntry
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.table.metadata import TableMetadata

#: sequence number of the first snapshot; 0 is reserved.
_FIRST_SEQUENCE_NUMBER = 1


@dataclass(frozen=True)
class SnapshotCounters:
    added_data_files: int
    added_records: int
    added_files_size: int
    total_data_files: int
    total_records: int
    total_files_size: int
    total_delete_files: int = 0
    total_position_deletes: int = 0
    total_equality_deletes: int = 0


@dataclass(frozen=True)
class DuckLakeProvenance:
    catalog: str
    schema_name: str
    table_name: str
    table_id: int
    snapshot_id: int  # snapshot_id of the exported DuckLake snapshot
    schema_version: int
    exported_at: str  # ISO-8601 UTC


class MetadataError(Exception):
    """Failed to build or write the metadata.json (exit 1)."""

    exit_code = 1


class InvalidLocationError(Exception):
    """Invalid ``location`` (exit 2)."""

    exit_code = 2


def new_snapshot_id() -> int:
    """Random signed 64-bit integer (does not reuse the DuckLake one)."""
    return secrets.randbits(63)


def _build_summary(
    counters: SnapshotCounters,
    operation: Operation = Operation.APPEND,
    extra_properties: dict[str, str] | None = None,
) -> Summary:
    """Build the snapshot summary with counters and state properties.

    Args:
        counters: File and record counters.
        operation: Snapshot operation (APPEND, REPLACE, OVERWRITE).
        extra_properties: Additional properties (ducklake.*, ducklake_iceberg.*).

    Returns:
        Snapshot summary with all counters and properties.
    """
    # Summary values are strings (map<string,string> in the Iceberg spec).
    summary_props = {
        "added-data-files": str(counters.added_data_files),
        "added-records": str(counters.added_records),
        "added-files-size": str(counters.added_files_size),
        "total-data-files": str(counters.total_data_files),
        "total-records": str(counters.total_records),
        "total-files-size": str(counters.total_files_size),
        "total-delete-files": str(counters.total_delete_files),
        "total-position-deletes": str(counters.total_position_deletes),
        "total-equality-deletes": str(counters.total_equality_deletes),
    }
    if extra_properties:
        summary_props.update(extra_properties)
    return Summary(operation, **summary_props)


def provenance_properties(
    provenance: DuckLakeProvenance,
    extra_properties: dict[str, str] | None,
) -> dict[str, str]:
    """Build provenance properties (first-export compatibility).

    Note: for incremental sync, use ``build_sync_properties`` from
    ``sync.state``, which is more complete and includes all sync state
    properties.
    """
    properties: dict[str, str] = {
        "ducklake.catalog": provenance.catalog,
        "ducklake.schema": provenance.schema_name,
        "ducklake.table": provenance.table_name,
        PROP_TABLE_ID: str(provenance.table_id),
        PROP_SNAPSHOT_ID: str(provenance.snapshot_id),
        PROP_SCHEMA_VERSION: str(provenance.schema_version),
        "ducklake_iceberg.exported_at": provenance.exported_at,
        "ducklake_iceberg.version": __version__,
        "write.format.default": "parquet",
    }
    if extra_properties:
        properties.update(extra_properties)
    return properties


def merge_properties(
    base_properties: dict[str, str],
    new_properties: dict[str, str],
) -> dict[str, str]:
    """Merge properties, keeping unmanaged ones and removing orphans.

    - Managed properties (``ducklake.*``, ``ducklake_iceberg.*``) in the base that are
      not in new are **removed** (orphans from a previous version).
    - Unmanaged properties (``write.*``, user config) are **preserved
      intact**.
    - new_properties overrides base on any conflict.

    Args:
        base_properties: Existing table properties.
        new_properties: New properties to apply.

    Returns:
        Merged properties with orphans removed and unmanaged ones preserved.
    """
    # Keep the base's unmanaged properties
    preserved = {
        k: v for k, v in base_properties.items() if not k.startswith(MANAGED_PREFIXES)
    }
    # Override with the new properties (managed + any other)
    return {**preserved, **new_properties}


def build_table_metadata(
    schema: Schema,
    spec: PartitionSpec,
    location: str,
    manifest_list_path: str,
    counters: SnapshotCounters,
    provenance: DuckLakeProvenance,
    table_uuid: uuid.UUID | None = None,
    extra_properties: dict[str, str] | None = None,
    operation: Operation = Operation.APPEND,
    summary_extra: dict[str, str] | None = None,
    *,
    specs: Sequence[PartitionSpec] = (),
    snapshot_id: int | None = None,
    last_column_id: int | None = None,
    summary: Summary | None = None,
) -> TableMetadataV2:
    """Build the v2 ``TableMetadata`` of the **first** export, with one snapshot.

    Args:
        schema: Table schema, with the DuckLake field IDs (preserved).
        spec: Default partition spec; its ``spec_id`` is preserved.
        location: Table base location (s3://...).
        manifest_list_path: Manifest list path.
        counters: File and record counters.
        provenance: DuckLake provenance (first-export compatibility).
        table_uuid: Table UUID (None generates a new one).
        extra_properties: Additional properties (use the sync properties).
        operation: Snapshot operation (APPEND, REPLACE, OVERWRITE).
        summary_extra: Extra summary properties (ducklake.*, ducklake_iceberg.*).
        specs: Additional specs referenced by manifests (besides ``spec``).
        snapshot_id: Iceberg snapshot ID; must be the same one written to the
            manifest list. ``None`` generates a new one.
        last_column_id: Highest column_id already assigned by DuckLake.
        summary: Ready-made summary; when given, ``counters``/``operation``/
            ``summary_extra`` are ignored.

    Returns:
        Complete v2 TableMetadata.
    """
    _check_location(location)
    properties = provenance_properties(provenance, extra_properties)
    all_specs = _unique_specs([spec, *specs])
    timestamp_ms = int(time.time() * 1000)
    snapshot = Snapshot(
        snapshot_id=snapshot_id if snapshot_id is not None else new_snapshot_id(),
        parent_snapshot_id=None,
        sequence_number=_FIRST_SEQUENCE_NUMBER,
        timestamp_ms=timestamp_ms,
        manifest_list=manifest_list_path,
        summary=summary or _build_summary(counters, operation, summary_extra),
        schema_id=schema.schema_id,
    )
    return TableMetadataV2(
        location=location,
        table_uuid=table_uuid or uuid.uuid4(),
        last_updated_ms=timestamp_ms,
        last_column_id=max(schema.highest_field_id, last_column_id or 0),
        schemas=[schema],
        current_schema_id=schema.schema_id,
        partition_specs=all_specs,
        default_spec_id=spec.spec_id,
        last_partition_id=_last_partition_id(all_specs),
        properties=properties,
        current_snapshot_id=snapshot.snapshot_id,
        snapshots=[snapshot],
        snapshot_log=[
            SnapshotLogEntry(
                snapshot_id=snapshot.snapshot_id, timestamp_ms=snapshot.timestamp_ms
            )
        ],
        metadata_log=[],
        sort_orders=[UNSORTED_SORT_ORDER],
        default_sort_order_id=UNSORTED_SORT_ORDER.order_id,
        refs={MAIN_BRANCH: _main_ref(snapshot.snapshot_id)},
        last_sequence_number=snapshot.sequence_number,
    )


def build_next_metadata(
    base: TableMetadataV2,
    base_location: str,
    *,
    schema: Schema,
    specs: Sequence[PartitionSpec],
    default_spec_id: int,
    manifest_list_path: str | None,
    summary: Summary | None,
    snapshot_id: int,
    properties: dict[str, str],
    last_column_id: int | None = None,
    retention_ms: int | None = None,
) -> TableMetadataV2:
    """Append a snapshot to ``base``: the ``metadata.json`` v{N+1}.

    - ``parent-snapshot-id`` points to the current snapshot of ``base`` and
      ``sequence-number`` is ``last-sequence-number + 1``;
    - the new schema enters with its own ``schema_id`` (DuckLake's
      ``schema_version``) when the structure changed; field IDs come from
      DuckLake and are therefore already the same as in earlier schemas;
    - specs not yet present are added, old ones stay (reused manifests still
      cite them);
    - ``metadata-log`` gets the entry for ``base_location``;
    - properties: see ``merge_properties``;
    - with ``retention_ms``, history is expired (``_expire``).

    With ``manifest_list_path=None`` there is no new snapshot: this is a
    schema/spec-only evolution (e.g. ``ADD COLUMN`` without a write), which in
    Iceberg does not create a snapshot.
    """
    timestamp_ms = max(int(time.time() * 1000), base.last_updated_ms + 1)

    schemas = list(base.schemas)
    current = next((s for s in schemas if s.as_struct() == schema.as_struct()), None)
    if current is None:
        taken = {s.schema_id for s in schemas}
        schema_id = (
            schema.schema_id if schema.schema_id not in taken else max(taken) + 1
        )
        current = schema.model_copy(update={"schema_id": schema_id})
        schemas.append(current)

    all_specs = _unique_specs([*base.partition_specs, *specs])
    update: dict[str, object] = {}
    current_snapshot_id = base.current_snapshot_id
    snapshots = list(base.snapshots)
    snapshot_log = list(base.snapshot_log)
    if manifest_list_path is not None:
        assert summary is not None
        sequence_number = base.last_sequence_number + 1
        snapshot = Snapshot(
            snapshot_id=snapshot_id,
            parent_snapshot_id=base.current_snapshot_id,
            sequence_number=sequence_number,
            timestamp_ms=timestamp_ms,
            manifest_list=manifest_list_path,
            summary=summary,
            schema_id=current.schema_id,
        )
        current_snapshot_id = snapshot.snapshot_id
        snapshots.append(snapshot)
        snapshot_log.append(
            SnapshotLogEntry(
                snapshot_id=snapshot.snapshot_id, timestamp_ms=timestamp_ms
            )
        )
        update = {
            "current_snapshot_id": current_snapshot_id,
            "refs": {**base.refs, MAIN_BRANCH: _main_ref(snapshot.snapshot_id)},
            "last_sequence_number": sequence_number,
        }
    metadata_log = [
        *base.metadata_log,
        MetadataLogEntry(
            metadata_file=base_location, timestamp_ms=base.last_updated_ms
        ),
    ]
    if retention_ms is not None:
        snapshots, snapshot_log, metadata_log = _expire(
            snapshots,
            snapshot_log,
            metadata_log,
            current_snapshot_id=current_snapshot_id,
            cutoff_ms=timestamp_ms - retention_ms,
        )
    return base.model_copy(
        update={
            "last_updated_ms": timestamp_ms,
            "last_column_id": max(
                base.last_column_id, current.highest_field_id, last_column_id or 0
            ),
            "schemas": schemas,
            "current_schema_id": current.schema_id,
            "partition_specs": all_specs,
            "default_spec_id": default_spec_id,
            "last_partition_id": _last_partition_id(all_specs),
            "properties": merge_properties(dict(base.properties), properties),
            "snapshots": snapshots,
            "snapshot_log": snapshot_log,
            "metadata_log": metadata_log,
            **update,
        }
    )


def _expire(
    snapshots: Sequence[Snapshot],
    snapshot_log: Sequence[SnapshotLogEntry],
    metadata_log: Sequence[MetadataLogEntry],
    *,
    current_snapshot_id: int | None,
    cutoff_ms: int,
) -> tuple[list[Snapshot], list[SnapshotLogEntry], list[MetadataLogEntry]]:
    """Expire history older than ``cutoff_ms`` (retention).

    Same semantics as Iceberg's ``expire_snapshots`` with
    ``min-snapshots-to-keep = 1``: the current snapshot is always kept, and
    the ``parent-snapshot-id`` of the oldest retained one may cite a snapshot
    that no longer exists. The metadata log is retained independently: Glue
    may still point at an older version after a failed publication, and its
    ancestry must remain provable on a later retry.
    Files that become unreachable are left to ``clean``; nothing is deleted
    here.
    """
    kept = [
        s
        for s in snapshots
        if s.timestamp_ms >= cutoff_ms or s.snapshot_id == current_snapshot_id
    ]
    kept_ids = {s.snapshot_id for s in kept}
    return (
        kept,
        [e for e in snapshot_log if e.snapshot_id in kept_ids],
        list(metadata_log),
    )


def _check_location(location: str) -> None:
    # Production uses `s3://`; `file://` and local absolute paths are accepted
    # for dev/test, consistent with the location validator.
    if not location.startswith(("s3://", "file://", "/")):
        raise InvalidLocationError(f"invalid location: {location!r}")


def _unique_specs(specs: Sequence[PartitionSpec]) -> list[PartitionSpec]:
    """One spec per ``spec_id``, in id order; the first occurrence wins."""
    by_id: dict[int, PartitionSpec] = {}
    for spec in specs:
        by_id.setdefault(spec.spec_id, spec)
    return [by_id[k] for k in sorted(by_id)]


def _last_partition_id(specs: Sequence[PartitionSpec]) -> int:
    field_ids = [f.field_id for spec in specs for f in spec.fields]
    return max(field_ids, default=PARTITION_FIELD_ID_START - 1)


def _main_ref(snapshot_id: int) -> SnapshotRef:
    return SnapshotRef(
        snapshot_id=snapshot_id,
        snapshot_ref_type=SnapshotRefType.BRANCH,
        min_snapshots_to_keep=None,
        max_snapshot_age_ms=None,
        max_ref_age_ms=None,
    )


def validate_counters(
    counters: SnapshotCounters, entries: Sequence[ManifestEntry]
) -> None:
    """Check the counters against the written manifest entries.

    Two sources (DuckLake catalog vs entries) can diverge silently; this check
    runs **before** serializing and aborts the export on a mismatch.
    """
    total_records = sum(e.data_file.record_count for e in entries)
    total_files = len(entries)
    total_size = sum(e.data_file.file_size_in_bytes for e in entries)

    if counters.total_records != total_records:
        raise MetadataError(
            f"inconsistent summary: total-records "
            f"{counters.total_records} != {total_records}"
        )
    if counters.total_data_files != total_files:
        raise MetadataError(
            f"inconsistent summary: total-data-files "
            f"{counters.total_data_files} != {total_files}"
        )
    if counters.total_files_size != total_size:
        raise MetadataError(
            f"inconsistent summary: total-files-size "
            f"{counters.total_files_size} != {total_size}"
        )


def next_metadata_path(location: str, version: int) -> str:
    """``{location}/metadata/v{version}.metadata.json``."""
    return f"{location.rstrip('/')}/metadata/v{version}.metadata.json"


def write_metadata_json(io: FileIO, metadata: TableMetadata, path: str) -> None:
    """Write the ``metadata.json``. Never overwrites.

    Serializes like PyIceberg's ``ToOutputFile.table_metadata``, but in two
    timed phases: ``metadata_json`` (serialization) and ``s3_put`` (upload).
    """
    with timing.phase("metadata_json"):
        # None must be serialized only in legacy mode (current-snapshot-id -1)
        exclude_none = not Config().get_bool("legacy-current-snapshot-id")
        payload = metadata.model_dump_json(exclude_none=exclude_none).encode("utf-8")
        payload = Compressor.get_compressor(path).bytes_compressor()(payload)
        add_work(
            "metadata_json", items=len(metadata.snapshots), byte_count=len(payload)
        )
    with timing.phase("s3.metadata_put"), timing.phase("s3_put"):
        try:
            with io.new_output(path).create(overwrite=False) as out:
                out.write(payload)
        except FileExistsError as exc:
            raise MetadataError(f"{path} already exists") from exc
        add_call("s3_put")
        add_work("s3_put", items=1, byte_count=len(payload))


def read_metadata_json(io: FileIO, path: str) -> TableMetadataV2:
    """Read a published v2 ``metadata.json`` (base of the incremental commit)."""
    metadata = read_metadata_or_none(io, path)
    if metadata is None:
        raise MetadataError(f"{path} does not exist")
    return metadata


def read_metadata_or_none(io: FileIO, path: str) -> TableMetadataV2 | None:
    """Like ``read_metadata_json``, but ``None`` if the file does not exist.

    A single streaming GET (no HEAD or ``exists()``): absence costs one
    request, not four.
    """
    try:
        with (
            timing.phase("s3.metadata_get"),
            io.new_input(path).open(seekable=False) as stream,
        ):
            metadata = FromByteStream.table_metadata(stream)
    except FileNotFoundError:
        return None
    if not isinstance(metadata, TableMetadataV2):
        raise MetadataError(
            f"{path} has format-version {metadata.format_version}; the mirror is v2"
        )
    return metadata
