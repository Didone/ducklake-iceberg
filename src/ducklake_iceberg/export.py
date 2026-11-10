"""The export stage of ``sync``.

Produces, under the Iceberg metadata prefix, the Iceberg snapshot that mirrors
the current snapshot of a DuckLake table: data and delete manifests, the
manifest list, ``v{N}.metadata.json`` and the handoff pointer consumed by the
register stage. It **only reads the catalog (READ_ONLY) and writes to S3** - it
never copies data Parquet (zero-copy) and never touches Glue.

First export or incremental, the path is the same:

1. base state: the pointer references the last ``metadata.json`` written;
   without a pointer, this is the first export (``v1``);
2. read DuckLake in one transaction: identity, inlined-data guard, enumeration
   with cross-checking of data and delete files, schema and partition specs
   using the DuckLake IDs;
3. **set difference** against the live manifests of the base snapshot: data
   files by path and delete files by consolidated group;
4. no file or schema difference => exit 4, nothing is written;
5. footers of the new data files and conversion of the new delete files;
6. manifests + manifest list, ``metadata.json`` v{N+1} and the pointer, in
   this order.

The **write boundary** is step 5: everything before it only reads, so an
``export`` that aborts during planning leaves no object in S3, and
``--dry-run`` stops exactly there, with no parallel code path.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Final

from pyiceberg.manifest import ManifestContent
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.snapshots import Operation
from pyiceberg.typedef import Record

from ducklake_iceberg import __version__, timing
from ducklake_iceberg.catalog.connection import metadata_catalog
from ducklake_iceberg.catalog.files import (
    FileSet,
    LiveDataFile,
    LiveDeleteFile,
)
from ducklake_iceberg.catalog.view import (
    CatalogView,
    check_initial_defaults,
    read_catalog,
)
from ducklake_iceberg.concurrency import (
    ContextThreadPoolExecutor,
    io_workers,
)
from ducklake_iceberg.deletes.convert import (
    delete_file_path,
    read_visible_positions,
)
from ducklake_iceberg.deletes.writer import PositionDelete, write_position_delete_file
from ducklake_iceberg.doctor.model import Finding
from ducklake_iceberg.errors import (
    DivergenceError,
    DuckLakeIcebergError,
    ExitCode,
)
from ducklake_iceberg.handoff import (
    DELETE_FILE_LAYOUT_PARTITION_V1,
    ExportPointer,
    PointerNotFoundError,
    normalize_table_name,
    pointer_path,
    read_pointer,
    table_location,
    utc_now_iso,
    write_pointer,
)
from ducklake_iceberg.mapping.evolution import evolve_schema
from ducklake_iceberg.mapping.partition import (
    SpecMapping,
    build_partition_record,
    with_published_names,
)
from ducklake_iceberg.obs.metrics import add_call, add_work
from ducklake_iceberg.storage import build_file_io
from ducklake_iceberg.sync.state import (
    PROP_SNAPSHOT_ID,
    PROP_TABLE_ID,
    PROP_TABLE_UUID,
    DuckLakeIdentity,
    DuckLakeSnapshot,
    SyncMode,
    build_sync_properties,
    parse_sync_state,
    validate_identity,
)
from ducklake_iceberg.writer.catalog_stats import (
    ColumnStats,
    build_data_file_from_catalog,
    read_file_column_stats,
)
from ducklake_iceberg.writer.commit import (
    BaseSnapshot,
    ContentChange,
    SnapshotFiles,
    build_summary,
    classify_operation,
    load_base_snapshot,
    write_snapshot,
)
from ducklake_iceberg.writer.metadata import (
    DuckLakeProvenance,
    MetadataError,
    SnapshotCounters,
    build_next_metadata,
    build_table_metadata,
    new_snapshot_id,
    next_metadata_path,
    provenance_properties,
    read_metadata_json,
    read_metadata_or_none,
    write_metadata_json,
)
from ducklake_iceberg.writer.stats import FileToExport, build_data_files

if TYPE_CHECKING:
    from pyiceberg.io import FileIO
    from pyiceberg.manifest import DataFile
    from pyiceberg.table.metadata import TableMetadata, TableMetadataV2

    from ducklake_iceberg.catalog.connection import DuckLakeConnection
    from ducklake_iceberg.config import DuckLakeIcebergSettings

LOG = logging.getLogger("ducklake_iceberg")

_FIRST_METADATA_VERSION: Final[int] = 1

#: ``ducklake_snapshot_changes`` tokens that only compact.
_COMPACTION_TOKENS: Final[frozenset[str]] = frozenset(
    {"merge_adjacent", "rewrite_delete"}
)
#: default for concurrent delete file conversions; the effective value comes
#: from ``max_io_workers``
_DELETE_WORKERS: Final[int] = 8


@dataclass(frozen=True, slots=True)
class ExportPlan:
    """Everything resolved before writing a byte (steps 1-4)."""

    schema_name: str
    table_name: str
    table_id: int
    ducklake_snapshot_id: int
    schema_version: int
    location: str  # {iceberg_metadata_prefix}/{db}/{table}
    metadata_version: int  # N of v{N}.metadata.json
    metadata_location: str
    files: FileSet
    total_records: int
    total_files_size: int
    previous_metadata_location: str | None = None
    added_data_files: int = 0
    removed_data_files: int = 0
    added_delete_files: int = 0
    removed_delete_files: int = 0
    schema_changed: bool = False
    ducklake_snapshot_time: datetime | None = None

    @property
    def is_incremental(self) -> bool:
        return self.previous_metadata_location is not None

    @property
    def has_changes(self) -> bool:
        return bool(
            self.added_data_files
            or self.removed_data_files
            or self.added_delete_files
            or self.removed_delete_files
            or self.schema_changed
            or not self.is_incremental
        )


@dataclass(frozen=True, slots=True)
class ExportResult:
    plan: ExportPlan
    dry_run: bool
    manifest_paths: tuple[str, ...]
    manifest_list_path: str
    pointer_path: str | None  # None on dry-run or when nothing changed
    exit_code: ExitCode
    operation: str | None = None
    snapshot_id: int | None = None
    #: degradations with no effect on the exit code (partition without equivalent)
    warnings: tuple[Finding, ...] = ()
    #: current handoff pointer (the one just written or the one read) and the
    #: metadata.json just written: ``sync`` hands them to register without
    #: re-reading them from S3
    handoff: ExportPointer | None = None
    metadata: TableMetadata | None = None

    @property
    def metadata_location(self) -> str:
        if self.exit_code is ExitCode.NOTHING_TO_DO and self.handoff is not None:
            return self.handoff.metadata_location
        return self.plan.metadata_location

    def to_json(self) -> dict[str, object]:
        snapshot_time = self.plan.ducklake_snapshot_time
        return {
            "schema": self.plan.schema_name,
            "table": self.plan.table_name,
            "metadata_location": self.metadata_location,
            "metadata_version": (
                self.handoff.metadata_version
                if self.exit_code is ExitCode.NOTHING_TO_DO and self.handoff is not None
                else self.plan.metadata_version
            ),
            "previous_metadata_location": self.plan.previous_metadata_location,
            "ducklake_snapshot_id": self.plan.ducklake_snapshot_id,
            # commit time of the published snapshot: the newest data visible in Iceberg
            "ducklake_snapshot_time": snapshot_time.isoformat()
            if snapshot_time
            else None,
            "iceberg_snapshot_id": self.snapshot_id,
            "operation": self.operation,
            "data_files": len(self.plan.files.files),
            "delete_files": sum(
                1 for f in self.plan.files.files if f.delete_file is not None
            ),
            "added_data_files": self.plan.added_data_files,
            "removed_data_files": self.plan.removed_data_files,
            "added_delete_files": self.plan.added_delete_files,
            "removed_delete_files": self.plan.removed_delete_files,
            "schema_changed": self.plan.schema_changed,
            "total_records": self.plan.total_records,
            "total_files_size": self.plan.total_files_size,
            "manifest_list": self.manifest_list_path or None,
            "pointer": self.pointer_path,
            "dry_run": self.dry_run,
            "exit_code": int(self.exit_code),
            "warnings": [{"rule_id": w.rule_id, **w.to_json()} for w in self.warnings],
        }


# -- base state (step 1) -----------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Base:
    pointer: ExportPointer | None
    metadata: TableMetadataV2 | None


def _glue_target(settings: DuckLakeIcebergSettings, glue_table: str) -> str:
    return f"{settings.glue_database}.{normalize_table_name(glue_table)}"


def _read_base_pointer(io: FileIO, location: str) -> ExportPointer | None:
    """Handoff pointer of the last export, or ``None`` (first export)."""
    try:
        return read_pointer(io, location)
    except PointerNotFoundError:
        return None


def _load_base(io: FileIO, location: str, pointer: ExportPointer | None) -> _Base:
    if pointer is None:
        return _Base(pointer=None, metadata=None)
    # the pointer's metadata.json and the successor probe go out together (2
    # parallel GETs; a missing successor is the common case)
    with ContextThreadPoolExecutor(max_workers=2) as pool:
        current = pool.submit(read_metadata_json, io, pointer.metadata_location)
        probe = pool.submit(
            read_metadata_or_none,
            io,
            next_metadata_path(location, pointer.metadata_version + 1),
        )
        metadata = current.result()
        candidate = probe.result()
    # A previous export wrote v{N+1} and crashed before the pointer: the
    # successor is adopted only if it provably descends from the pointer (last
    # metadata-log entry); otherwise v{N+1} is from another chain and the
    # export stops.
    while candidate is not None:
        successor = next_metadata_path(location, pointer.metadata_version + 1)
        log = candidate.metadata_log
        if not log or log[-1].metadata_file != pointer.metadata_location:
            raise MetadataError(
                f"{successor} exists and does not descend from "
                f"{pointer.metadata_location}"
            )
        pointer = dataclasses.replace(
            pointer,
            metadata_location=successor,
            metadata_version=pointer.metadata_version + 1,
            ducklake_snapshot_id=int(
                candidate.properties.get(
                    PROP_SNAPSHOT_ID, str(pointer.ducklake_snapshot_id)
                )
            ),
        )
        metadata = candidate
        candidate = read_metadata_or_none(
            io, next_metadata_path(location, pointer.metadata_version + 1)
        )
    return _Base(pointer=pointer, metadata=metadata)


# -- desired vs. published (step 3) ------------------------------------------


@dataclass(frozen=True, slots=True)
class _DeleteGroup:
    spec_id: int
    partition: Record
    files: tuple[tuple[LiveDataFile, LiveDeleteFile], ...]


@dataclass(frozen=True, slots=True)
class _Desired:
    data: Mapping[str, tuple[LiveDataFile, int, Record]]  # canon -> (f, spec, part)
    deletes: Mapping[str, _DeleteGroup]  # fingerprint -> file per partition


def _partition_record(
    df: LiveDataFile,
    mapping: SpecMapping,
    spec: PartitionSpec,
    schema: Schema,
    values_by_file: dict[int, dict[int, str | None]],
) -> Record:
    """Translate the per-file values (key_index) into the positional Record."""
    if df.partition_id is None or spec.spec_id == 0:
        return Record()
    raw_by_index = values_by_file.get(df.data_file_id, {})
    raw_by_field_id: dict[int, str | None] = {}
    for (partition_id, key_index), field_id in mapping.field_ids.items():
        if partition_id == df.partition_id and key_index in raw_by_index:
            raw_by_field_id[field_id] = raw_by_index[key_index]
    return build_partition_record(spec, schema, raw_by_field_id)


def _desired(view: CatalogView, schema: Schema) -> _Desired:
    mapping = view.spec_mapping
    data: dict[str, tuple[LiveDataFile, int, Record]] = {}
    files_by_group: dict[
        tuple[int, Record], list[tuple[LiveDataFile, LiveDeleteFile]]
    ] = {}
    for f in view.files.files:
        spec_id = (
            0
            if f.partition_id is None
            else mapping.spec_id_by_partition_id.get(f.partition_id, 0)
        )
        spec = mapping.specs[spec_id]
        data[f.canonical_path] = (
            f,
            spec_id,
            _partition_record(f, mapping, spec, schema, view.values_by_file),
        )
        if f.delete_file is not None:
            partition = data[f.canonical_path][2]
            files_by_group.setdefault((spec_id, partition), []).append(
                (f, f.delete_file)
            )
    deletes: dict[str, _DeleteGroup] = {}
    for (spec_id, partition), group_files in files_by_group.items():
        members = tuple(
            sorted(
                group_files,
                key=lambda pair: (
                    pair[0].canonical_path,
                    pair[1].canonical_path,
                ),
            )
        )
        fingerprint = json.dumps(
            [
                spec_id,
                repr(partition),
                [(f.canonical_path, delete.canonical_path) for f, delete in members],
            ],
            separators=(",", ":"),
        )
        key = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:24]
        deletes[key] = _DeleteGroup(spec_id, partition, members)
    return _Desired(data=data, deletes=deletes)


def _source_identity(view: CatalogView) -> DuckLakeIdentity:
    return DuckLakeIdentity(
        table_id=view.identity.table_id,
        table_uuid=str(view.identity.table_uuid),
        schema_name=view.identity.ref.schema_name,
        table_name=view.identity.ref.table_name,
        data_path=view.files.layers.data_path,
        partition_id=view.spec_mapping.default_spec_id or -1,
        last_column_id=view.schema_mapping.last_column_id,
    )


def _check_identity(base: _Base, view: CatalogView, glue_target: str) -> None:
    """The published state must belong to the same DuckLake table.

    Two causes, with distinct messages: another DuckLake table with the same
    Glue name - the Glue name (and location) drops the schema and normalizes
    the name - or the same ``schema.table`` recreated (new table-id/uuid). In
    both cases nothing was written.
    """
    if base.metadata is None:
        return
    identity = view.identity
    props = base.metadata.properties
    published_uuid = props.get(PROP_TABLE_UUID)
    published_id = props.get(PROP_TABLE_ID)
    if (published_uuid is None or published_uuid == str(identity.table_uuid)) and (
        published_id is None or published_id == str(identity.table_id)
    ):
        mismatches = validate_identity(parse_sync_state(props), _source_identity(view))
        if mismatches:
            raise DivergenceError(
                f"invalid published sync state: {mismatches[0].detail}",
                hint="inspect source identity and metadata; do not force the update",
            )
        return
    where = base.pointer.metadata_location if base.pointer else "?"
    current = f"{identity.ref.schema_name}.{identity.ref.table_name}"
    published_schema = props.get("ducklake.schema")
    published_table = props.get("ducklake.table")
    if published_schema is not None and published_table is not None:
        published = f"{published_schema}.{published_table}"
        if published != current:
            raise DivergenceError(
                f"{published} and {current} map to the same Glue table "
                f"{glue_target}; the Glue name does not include the DuckLake "
                f"schema (and normalizes the table name). The Iceberg table at "
                f"{where} mirrors {published}",
                hint=(
                    "use a distinct glue_database per schema (another config) or "
                    "rename one of the tables in DuckLake; Glue and S3 were not "
                    "changed"
                ),
            )
    raise DivergenceError(
        f"the Iceberg table at {where} mirrors another DuckLake table (table-id "
        f"{published_id}, uuid {published_uuid}); table {current} was recreated",
        hint="publish to another prefix/Glue table or remove the old mirror",
    )


# -- pipeline ----------------------------------------------------------------


def plan_export(
    settings: DuckLakeIcebergSettings, schema_name: str, table_name: str
) -> ExportPlan:
    """Resolve everything without writing (steps 1-4)."""
    return _prepare(settings, schema_name, table_name).plan


@dataclass(frozen=True, slots=True)
class _Prepared:
    plan: ExportPlan
    base: _Base
    base_snapshot: BaseSnapshot
    view: CatalogView
    schema: Schema
    desired: _Desired
    data_added: tuple[str, ...]
    data_removed: frozenset[str]
    deletes_added: tuple[str, ...]
    deletes_removed: frozenset[str]
    io: FileIO


@dataclass(frozen=True, slots=True)
class _Unchanged:
    """Nothing changed in the table since the last export (short-circuit)."""

    plan: ExportPlan
    view: CatalogView
    pointer: ExportPointer


def _unchanged_since(pointer: ExportPointer, view: CatalogView) -> bool:
    """Does the pointer already mirror the table's current state?

    Every data or schema write to a DuckLake table records a token with the
    ``table_id`` in ``ducklake_snapshot_changes`` (``inserted_into_table``,
    ``inlined_insert``/``inlined_delete``, ``deleted_from_table``,
    ``altered_table``, ``inline_flush``, ``merge_adjacent``,
    ``rewrite_delete``); with no token in the ``(S_pointer, S]`` window - and no
    expired snapshot in it - the table did not change. Renaming or recreating
    the table changes its name/``table_id``, and another ducklake-iceberg
    version may map differently: in those cases the full path decides. An
    orphan ``v{N+1}`` implies a change in the window (export only writes when
    something changed), so it also falls through to the full path.
    """
    identity = view.identity
    return (
        pointer.table_id == identity.table_id
        and pointer.ducklake_schema == identity.ref.schema_name
        and pointer.ducklake_table == identity.ref.table_name
        and pointer.schema_version == identity.schema_version
        and pointer.ducklake_iceberg_version == __version__
        and (
            pointer.delete_file_layout == DELETE_FILE_LAYOUT_PARTITION_V1
            or not any(f.delete_file is not None for f in view.files.files)
        )
        and pointer.ducklake_snapshot_id <= view.snapshot_id
        and view.table_changes_complete
        and not view.table_changes
    )


def _unchanged_plan(
    location: str, pointer: ExportPointer, view: CatalogView
) -> ExportPlan:
    version = pointer.metadata_version + 1
    return ExportPlan(
        schema_name=view.identity.ref.schema_name,
        table_name=view.identity.ref.table_name,
        table_id=view.identity.table_id,
        ducklake_snapshot_id=view.snapshot_id,
        ducklake_snapshot_time=view.snapshot_time,
        schema_version=view.identity.schema_version,
        location=location,
        metadata_version=version,
        metadata_location=next_metadata_path(location, version),
        files=view.files,
        total_records=view.files.total_record_count,
        total_files_size=view.files.total_size_in_bytes,
        previous_metadata_location=pointer.metadata_location,
    )


def _prepare(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    con: DuckLakeConnection | None = None,
) -> _Prepared | _Unchanged:
    io = build_file_io(settings)
    location = table_location(
        settings.iceberg_metadata_prefix, settings.glue_database, table_name
    )
    ref = f"{schema_name}.{table_name}"
    with timing.phase("export.base"):
        pointer = _read_base_pointer(io, location)

    def read_view(previous: int | None) -> CatalogView:
        with (
            timing.phase("export.catalog"),
            metadata_catalog(settings, con) as ro,
            ro.read_transaction(),
        ):
            return read_catalog(ro, ref, previous)

    view = read_view(pointer.ducklake_snapshot_id if pointer is not None else None)
    # work counters once per export, even if the view is re-read
    add_work("resolve", items=1)
    add_work(
        "enumerate",
        items=len(view.files.files),
        byte_count=view.files.total_size_in_bytes,
    )
    if pointer is not None and _unchanged_since(pointer, view):
        return _Unchanged(_unchanged_plan(location, pointer, view), view, pointer)

    with timing.phase("export.base"):
        base = _load_base(io, location, pointer)
    _check_identity(base, view, _glue_target(settings, table_name))
    previous_snapshot = (
        int(base.metadata.properties[PROP_SNAPSHOT_ID])
        if base.metadata is not None and PROP_SNAPSHOT_ID in base.metadata.properties
        else None
    )
    if previous_snapshot != view.table_changes_since:
        # successor adopted or pointer without the property: window recomputed
        view = read_view(previous_snapshot)
        _check_identity(base, view, _glue_target(settings, table_name))
    if base.metadata is not None:
        # specs already published keep their partition field names
        view = dataclasses.replace(
            view,
            spec_mapping=with_published_names(view.spec_mapping, base.metadata.specs()),
        )
    check_initial_defaults(view, io)

    schema = evolve_schema(view.schema_mapping.schema, base.metadata)
    desired = _desired(view, schema)
    with timing.phase("export.manifests_read"):
        base_snapshot = load_base_snapshot(io, base.metadata)
    live_data = base_snapshot.live(ManifestContent.DATA)
    live_deletes = base_snapshot.live(ManifestContent.DELETES)

    with timing.phase("delta"):
        data_added = tuple(sorted(set(desired.data) - set(live_data)))
        data_removed = frozenset(set(live_data) - set(desired.data))
        deletes_added = tuple(sorted(set(desired.deletes) - set(live_deletes)))
        deletes_removed = frozenset(set(live_deletes) - set(desired.deletes))
        add_work(
            "delta",
            items=len(data_added)
            + len(data_removed)
            + len(deletes_added)
            + len(deletes_removed),
            byte_count=sum(
                desired.data[key][0].file_size_in_bytes for key in data_added
            ),
        )
    schema_changed = base.metadata is not None and (
        base.metadata.schema().as_struct() != schema.as_struct()
        or base.metadata.default_spec_id != view.spec_mapping.default_spec_id
    )

    version = (
        base.pointer.metadata_version + 1
        if base.pointer is not None
        else _FIRST_METADATA_VERSION
    )
    metadata_location = next_metadata_path(location, version)
    plan = ExportPlan(
        schema_name=view.identity.ref.schema_name,
        table_name=view.identity.ref.table_name,
        table_id=view.identity.table_id,
        ducklake_snapshot_id=view.snapshot_id,
        ducklake_snapshot_time=view.snapshot_time,
        schema_version=view.identity.schema_version,
        location=location,
        metadata_version=version,
        metadata_location=metadata_location,
        files=view.files,
        total_records=view.files.total_record_count,
        total_files_size=view.files.total_size_in_bytes,
        previous_metadata_location=(
            base.pointer.metadata_location if base.pointer is not None else None
        ),
        added_data_files=len(data_added),
        removed_data_files=len(data_removed),
        added_delete_files=len(deletes_added),
        removed_delete_files=len(deletes_removed),
        schema_changed=schema_changed,
    )
    return _Prepared(
        plan=plan,
        base=base,
        base_snapshot=base_snapshot,
        view=view,
        schema=schema,
        desired=desired,
        data_added=data_added,
        data_removed=data_removed,
        deletes_added=deletes_added,
        deletes_removed=deletes_removed,
        io=io,
    )


def run_export(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    *,
    dry_run: bool = False,
    con: DuckLakeConnection | None = None,
    max_footer_workers: int | None = None,
) -> ExportResult:
    """Full pipeline; ``dry_run`` stops before the first write.

    ``con``: READ_ONLY connection borrowed from ``sync`` (not closed here);
    without it, the export uses a connection of its own.
    """
    with metadata_catalog(settings, con) as ro:
        return _run_export(
            settings,
            schema_name,
            table_name,
            ro,
            dry_run=dry_run,
            max_footer_workers=max_footer_workers or settings.max_footer_workers,
        )


def _run_export(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    con: DuckLakeConnection,
    *,
    dry_run: bool,
    max_footer_workers: int = 8,
) -> ExportResult:
    prep = _prepare(settings, schema_name, table_name, con)
    plan = prep.plan
    warnings = _partition_warnings(prep.view)

    if isinstance(prep, _Unchanged) or not plan.has_changes:
        return ExportResult(
            plan=plan,
            dry_run=dry_run,
            manifest_paths=(),
            manifest_list_path="",
            pointer_path=None,
            exit_code=ExitCode.NOTHING_TO_DO,
            warnings=warnings,
            handoff=(
                prep.pointer if isinstance(prep, _Unchanged) else prep.base.pointer
            ),
        )
    # `_load_base` just proved that v{N+1} does not exist; the write with
    # `overwrite=False` (write_metadata_json) remains the final guard.

    # step 5a: footers of the new data files - still no writes
    with timing.phase("export.stats"):
        added_data = _build_added_data_files(
            prep, settings, con, max_footer_workers=max_footer_workers
        )
    if dry_run:
        return ExportResult(
            plan=plan,
            dry_run=True,
            manifest_paths=(),
            manifest_list_path="",
            pointer_path=None,
            exit_code=ExitCode.SUCCESS,
            warnings=warnings,
            handoff=prep.base.pointer,
        )

    # step 5b onward: writes
    snapshot_id = new_snapshot_id()
    with timing.phase("export.deletes"):
        added_deletes, expected_deletes = _convert_added_deletes(prep)
    base_meta = prep.base.metadata
    data_change = ContentChange(added=tuple(added_data), removed=prep.data_removed)
    delete_change = ContentChange(
        added=tuple(added_deletes), removed=prep.deletes_removed
    )
    files_changed = not (data_change.is_empty and delete_change.is_empty)

    specs: dict[int, PartitionSpec] = dict(prep.view.spec_mapping.specs)
    if base_meta is not None:
        specs = {**base_meta.specs(), **specs}

    snapshot_files: SnapshotFiles | None = None
    operation: Operation | None = None
    if files_changed or base_meta is None:
        sequence_number = base_meta.last_sequence_number + 1 if base_meta else 1
        with timing.phase("export.write"):
            snapshot_files = write_snapshot(
                prep.io,
                plan.location,
                prep.base_snapshot,
                data=data_change,
                deletes=delete_change,
                schema=prep.schema,
                specs=specs,
                schemas=base_meta.schemas if base_meta else (),
                snapshot_id=snapshot_id,
                parent_snapshot_id=base_meta.current_snapshot_id if base_meta else None,
                sequence_number=sequence_number,
                expected_data=frozenset(prep.desired.data),
                expected_deletes=expected_deletes,
            )
        operation = classify_operation(
            data_change,
            delete_change,
            prep.base_snapshot,
            compaction_only=_compaction_only(prep, snapshot_files),
        )

    retention = settings.snapshot_retention_hours
    metadata = _build_metadata(
        prep,
        settings.catalog_alias,
        specs,
        snapshot_files,
        operation,
        snapshot_id,
        retention_ms=retention * 3_600_000 if retention is not None else None,
    )
    with timing.phase("export.write"):
        write_metadata_json(prep.io, metadata, plan.metadata_location)
        handoff = _write_handoff(plan, settings, prep.io)

    return ExportResult(
        plan=plan,
        dry_run=False,
        manifest_paths=snapshot_files.manifest_paths if snapshot_files else (),
        manifest_list_path=snapshot_files.manifest_list_path if snapshot_files else "",
        pointer_path=pointer_path(plan.location),
        exit_code=ExitCode.SUCCESS,
        operation=str(operation.value) if operation else None,
        snapshot_id=metadata.current_snapshot_id,
        warnings=warnings,
        handoff=handoff,
        metadata=metadata,
    )


def _partition_warnings(view: CatalogView) -> tuple[Finding, ...]:
    """Degraded specs that affect live files, also logged.

    The degradation does not block (the table comes out without the partition
    key and only loses pruning), but it must show up in ``export``/``sync`` and
    not only in ``doctor``. Historical specs with no live file do not warn.
    """
    live = {f.partition_id for f in view.files.files if f.partition_id is not None}
    warnings = tuple(
        f for f in view.spec_mapping.degraded if f.data.get("partition_id") in live
    )
    table = view.identity.ref
    for w in warnings:
        LOG.warning(
            "%s.%s: %s; remediation: %s",
            table.schema_name,
            table.table_name,
            w.detail,
            w.remediation,
        )
    return warnings


def _build_added_data_files(
    prep: _Prepared,
    settings: DuckLakeIcebergSettings,
    con: DuckLakeConnection | None,
    *,
    max_footer_workers: int = 8,
) -> list[tuple[int, DataFile]]:
    """``DataFile`` for the new data files, grouped by spec.

    With ``stats_source: catalog`` the metrics come from
    ``ducklake_file_column_stats`` (one query, no S3); a file without catalog
    stats, or ``stats_source: footer``, reads the Parquet footer.
    """
    by_spec: dict[int, list[tuple[LiveDataFile, FileToExport]]] = {}
    for key in prep.data_added:
        f, spec_id, partition = prep.desired.data[key]
        by_spec.setdefault(spec_id, []).append(
            (
                f,
                FileToExport(
                    path=f.path,
                    file_size_in_bytes=f.file_size_in_bytes,
                    record_count=f.record_count,
                    partition_values=partition,
                ),
            )
        )
    stats: dict[int, dict[int, ColumnStats]] = {}
    if settings.stats_source == "catalog" and by_spec:
        with metadata_catalog(settings, con) as ro, ro.read_transaction():
            stats = read_file_column_stats(
                ro,
                prep.view.identity.table_id,
                (f.data_file_id for items in by_spec.values() for f, _ in items),
            )
    out: list[tuple[int, DataFile]] = []
    for spec_id, items in sorted(by_spec.items()):
        spec = prep.view.spec_mapping.specs[spec_id]
        footer = [fx for f, fx in items if f.data_file_id not in stats]
        with timing.phase("footers"):
            footer_bytes = sum(file.file_size_in_bytes for file in footer)
            timing.count("footers.files", len(footer))
            timing.count("footers.bytes", footer_bytes)
            add_work("footers", items=len(footer), byte_count=footer_bytes)
            # one footer read (ranged S3 GET) per file; counted here because the
            # read is done by PyArrow, outside the botocore hook
            for _ in footer:
                add_call("footers")
            from_footer = {
                df.file_path: df
                for df in build_data_files(
                    footer,
                    prep.schema,
                    spec,
                    {},
                    prep.io,
                    max_workers=max_footer_workers,
                )
            }
        for f, fx in items:
            df = (
                build_data_file_from_catalog(
                    fx, prep.schema, spec, stats[f.data_file_id]
                )
                if f.data_file_id in stats
                else from_footer[fx.path]
            )
            out.append((spec_id, df))
    # ``stats`` phase: columns with bounds in the new data files
    add_work(
        "stats",
        items=len({fid for _, df in out for fid in (df.lower_bounds or {})}),
    )
    return out


def _convert_added_deletes(
    prep: _Prepared,
) -> tuple[list[tuple[int, DataFile]], frozenset[str]]:
    """Write one position delete file per partition."""

    def convert(key: str) -> tuple[int, DataFile | None]:
        group = prep.desired.deletes[key]
        positions = [
            PositionDelete(f.path, pos, group.partition)
            for f, delete in group.files
            for pos in read_visible_positions(
                prep.io,
                delete,
                snapshot_id=prep.view.snapshot_id,
                record_count=f.record_count,
            )
        ]
        if not positions:
            return group.spec_id, None
        result = write_position_delete_file(
            prep.io,
            positions,
            prep.plan.location,
            group.partition,
            prep.view.spec_mapping.specs[group.spec_id],
            path=delete_file_path(prep.plan.location, key),
            check_exists=False,
        )
        return group.spec_id, result.data_file

    # each file is an independent S3 GET + PUT: run in parallel, in order
    workers = io_workers(len(prep.deletes_added))
    with ContextThreadPoolExecutor(max_workers=workers) as pool:
        converted = list(pool.map(convert, prep.deletes_added))
    out: list[tuple[int, DataFile]] = []
    expected = set(prep.desired.deletes)
    for key, (spec_id, data_file) in zip(prep.deletes_added, converted, strict=True):
        if data_file is None:
            expected.discard(key)
        else:
            out.append((spec_id, data_file))
    # one PutObject per delete file written
    for _ in out:
        add_call("deletes")
    add_work(
        "deletes",
        items=len(out),
        byte_count=sum(data_file.file_size_in_bytes for _, data_file in out),
    )
    return out, frozenset(expected)


def _compaction_only(prep: _Prepared, files: SnapshotFiles) -> bool:
    """``replace`` only when DuckLake merely compacted and the row counts match."""
    kinds = set(prep.view.table_changes)
    if not kinds or not kinds <= _COMPACTION_TOKENS or prep.base.metadata is None:
        return False
    snapshot = prep.base.metadata.current_snapshot()
    if snapshot is None or snapshot.summary is None:
        return False
    before = snapshot.summary.additional_properties
    try:
        logical_before = int(before["total-records"]) - int(
            before.get("total-position-deletes", "0")
        )
    except (KeyError, ValueError):
        return False
    c = files.counters
    return logical_before == c.total_records - c.total_position_deletes


def _build_metadata(
    prep: _Prepared,
    catalog_alias: str,
    specs: dict[int, PartitionSpec],
    files: SnapshotFiles | None,
    operation: Operation | None,
    snapshot_id: int,
    *,
    retention_ms: int | None = None,
) -> TableMetadata:
    view = prep.view
    plan = prep.plan
    base_meta = prep.base.metadata
    default_spec_id = view.spec_mapping.default_spec_id
    current_snapshot_id = (
        snapshot_id
        if files is not None
        else (base_meta.current_snapshot_id if base_meta else None)
    )
    state = build_sync_properties(
        _source_identity(view),
        DuckLakeSnapshot(
            snapshot_id=view.snapshot_id,
            snapshot_time=view.snapshot_time.astimezone(UTC),
            schema_version=view.identity.schema_version,
        ),
        mode=SyncMode.INCREMENTAL if base_meta is not None else SyncMode.FULL,
        iceberg_snapshot_id=current_snapshot_id or snapshot_id,
        now=datetime.now(UTC),
        tool_version=__version__,
    )
    provenance = DuckLakeProvenance(
        catalog=catalog_alias,
        schema_name=plan.schema_name,
        table_name=plan.table_name,
        table_id=plan.table_id,
        snapshot_id=plan.ducklake_snapshot_id,
        schema_version=plan.schema_version,
        exported_at=utc_now_iso(),
    )
    summary = (
        build_summary(operation or Operation.APPEND, files.counters, state)
        if files is not None
        else None
    )

    if base_meta is None:
        assert files is not None
        assert summary is not None
        return build_table_metadata(
            prep.schema,
            specs[default_spec_id],
            plan.location,
            files.manifest_list_path,
            SnapshotCounters(0, 0, 0, 0, 0, 0),
            provenance,
            extra_properties=state,
            specs=list(specs.values()),
            snapshot_id=snapshot_id,
            last_column_id=view.schema_mapping.last_column_id,
            summary=summary,
        )

    assert prep.base.pointer is not None
    return build_next_metadata(
        base_meta,
        prep.base.pointer.metadata_location,
        schema=prep.schema,
        specs=list(specs.values()),
        default_spec_id=default_spec_id,
        manifest_list_path=files.manifest_list_path if files else None,
        summary=summary,
        snapshot_id=snapshot_id,
        properties=provenance_properties(provenance, state),
        last_column_id=view.schema_mapping.last_column_id,
        retention_ms=retention_ms,
    )


def _write_handoff(
    plan: ExportPlan, settings: DuckLakeIcebergSettings, io: FileIO
) -> ExportPointer:
    """Step 6: the handoff pointer, written last."""
    pointer = ExportPointer(
        version=1,
        metadata_location=plan.metadata_location,
        metadata_version=plan.metadata_version,
        location=plan.location,
        glue_database=settings.glue_database,
        glue_table=normalize_table_name(plan.table_name),
        ducklake_catalog=settings.catalog_alias,
        ducklake_schema=plan.schema_name,
        ducklake_table=plan.table_name,
        table_id=plan.table_id,
        ducklake_snapshot_id=plan.ducklake_snapshot_id,
        schema_version=plan.schema_version,
        exported_at=utc_now_iso(),
        ducklake_iceberg_version=__version__,
        delete_file_layout=DELETE_FILE_LAYOUT_PARTITION_V1,
    )
    write_pointer(io, pointer)
    return pointer


__all__ = [
    "DuckLakeIcebergError",
    "ExportPlan",
    "ExportResult",
    "plan_export",
    "run_export",
]
