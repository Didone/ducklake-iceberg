"""The ``ducklake-iceberg register`` command and the handoff contract.

This module **only talks to Glue and S3** (to read the ``metadata.json`` and the
pointer). It opens no DuckDB connection and reads no Parquet - an architecture
test guarantees it imports neither ``duckdb`` nor ``ducklake_iceberg.catalog``. This
separation allows running ``register`` in a catalog account, without Postgres
credentials or access to the ``data_path``.

The handoff between ``export`` and ``register`` is a JSON pointer at
``{location}/_ducklake_iceberg_last_export.json``. Format decisions:

- It is **not** ``version-hint.text`` - that name would make an engine treat the
  prefix as a Hadoop catalog parallel to Glue.
- It lives **outside** ``metadata/`` (which only holds Iceberg format artifacts).
- It is the **only** object the utility overwrites; Iceberg artifacts are
  immutable, while the pointer is operational state.
- The pointer is a convenience, **not** the source of truth: the truth is the
  ``metadata_location`` in Glue. So ``register`` resolves by
  ``--metadata-location`` > pointer > error, and **never** scans ``metadata/``
  looking for the highest ``N`` (a scan would find metadata.json files from
  exports that failed after writing).
"""

from __future__ import annotations

import functools
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

from pyiceberg.io import AWS_PROFILE_NAME, AWS_REGION, load_file_io

from ducklake_iceberg import timing
from ducklake_iceberg.aws import s3_credential_properties, s3_endpoint_properties
from ducklake_iceberg.errors import ConfigError, DivergenceError
from ducklake_iceberg.glue import (
    GlueTarget,
    RegisterResult,
    TableAlreadyRegistered,
    describe_table,
    normalize_table_name,
)
from ducklake_iceberg.glue import (
    register as glue_register,
)
from ducklake_iceberg.obs.metrics import add_call, add_work
from ducklake_iceberg.sync.state import PROP_SNAPSHOT_ID

if TYPE_CHECKING:
    from pyiceberg.io import FileIO
    from pyiceberg.table.metadata import TableMetadata

    from ducklake_iceberg.config import DuckLakeIcebergSettings

#: handoff pointer name. NEVER `version-hint.text`.
POINTER_NAME: str = "_ducklake_iceberg_last_export.json"

#: pointer format version; `register` rejects unknown versions.
POINTER_VERSION: Literal[1] = 1

# Layout recorded in the handoff so older per-data-file exports are rebuilt.
DELETE_FILE_LAYOUT_PARTITION_V1: str = "partition-v1"
DELETE_FILE_LAYOUT_LEGACY: str = "per-data-file-v1"


@dataclass(frozen=True, slots=True)
class ExportPointer:
    """Contents of ``_ducklake_iceberg_last_export.json``."""

    version: Literal[1]
    metadata_location: str
    metadata_version: int
    location: str
    glue_database: str
    glue_table: str
    ducklake_catalog: str
    ducklake_schema: str
    ducklake_table: str
    table_id: int
    ducklake_snapshot_id: int
    schema_version: int
    exported_at: str  # ISO-8601 UTC
    ducklake_iceberg_version: str
    delete_file_layout: str = DELETE_FILE_LAYOUT_LEGACY

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


class PointerError(ConfigError):
    """Pointer missing, unreadable, or of an unsupported version (exit 1)."""


class PointerNotFoundError(PointerError):
    """The table was never exported: there is no pointer yet."""


def utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_file_io(settings: DuckLakeIcebergSettings) -> FileIO:
    """PyIceberg ``FileIO`` configured with the AWS region/profile from settings.

    The profile credentials are passed explicitly (``s3.*``): ``PyArrowFileIO``
    does not read ``client.profile-name``.
    """
    props: dict[str, str] = {AWS_REGION: settings.aws_region}
    if settings.aws_profile is not None:
        props[AWS_PROFILE_NAME] = settings.aws_profile
    props.update(s3_credential_properties(settings.aws_region, settings.aws_profile))
    props.update(s3_endpoint_properties(settings.s3_endpoint))
    return _file_io(tuple(sorted(props.items())))


@functools.lru_cache(maxsize=8)
def _file_io(props: tuple[tuple[str, str], ...]) -> FileIO:
    """One FileIO per set of properties in the process.

    Reusing the FileIO reuses Arrow's ``S3FileSystem`` and its HTTP connections
    (keep-alive); the credentials are part of the key, so a renewal yields a new
    FileIO.
    """
    timing.count("fileio")
    return load_file_io(properties=dict(props))


def clear_file_io_cache() -> None:
    """Drop the process-wide FileIO cache (tests)."""
    _file_io.cache_clear()


def pointer_path(location: str) -> str:
    """``{location}/_ducklake_iceberg_last_export.json``."""
    return f"{location.rstrip('/')}/{POINTER_NAME}"


def write_pointer(io: FileIO, pointer: ExportPointer) -> str:
    """Write the pointer, **overwriting** - the only object that is overwritten."""
    path = pointer_path(pointer.location)
    payload = json.dumps(pointer.to_json(), indent=2).encode("utf-8")
    output = io.new_output(path)
    with timing.phase("s3_put"):
        try:
            with output.create(overwrite=True) as stream:
                stream.write(payload)
        except TypeError:
            # Some OutputFile implementations do not accept overwrite=; fall back
            # to the default create().
            with output.create() as stream:
                stream.write(payload)
        add_call("s3_put")
        add_work("s3_put", items=1, byte_count=len(payload))
    return path


def read_pointer(io: FileIO, location: str) -> ExportPointer:
    """Read and validate the pointer. ``PointerError`` if missing or incompatible."""
    path = pointer_path(location)
    # a single GET: ``exists()`` + ``open()`` cost ~4 S3 requests
    try:
        with io.new_input(path).open(seekable=False) as stream:
            raw = stream.read()
    except FileNotFoundError:
        raise PointerNotFoundError(
            f"no export found at {location}",
            hint="run ducklake-iceberg sync",
        ) from None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PointerError(f"unreadable pointer at {path}: {exc}") from exc
    version = data.get("version")
    if version != POINTER_VERSION:
        raise PointerError(f"pointer version {version} is not supported")
    return _pointer_from_dict(data)


def _pointer_from_dict(data: dict[str, Any]) -> ExportPointer:
    return ExportPointer(
        version=POINTER_VERSION,
        metadata_location=str(data["metadata_location"]),
        metadata_version=int(data["metadata_version"]),
        location=str(data["location"]),
        glue_database=str(data["glue_database"]),
        glue_table=str(data["glue_table"]),
        ducklake_catalog=str(data["ducklake_catalog"]),
        ducklake_schema=str(data["ducklake_schema"]),
        ducklake_table=str(data["ducklake_table"]),
        table_id=int(data["table_id"]),
        ducklake_snapshot_id=int(data["ducklake_snapshot_id"]),
        schema_version=int(data["schema_version"]),
        exported_at=str(data["exported_at"]),
        ducklake_iceberg_version=str(data["ducklake_iceberg_version"]),
        delete_file_layout=str(
            data.get("delete_file_layout", DELETE_FILE_LAYOUT_LEGACY)
        ),
    )


def _table_location(settings: DuckLakeIcebergSettings, table_name: str) -> str:
    glue_table = normalize_table_name(table_name)
    prefix = settings.iceberg_metadata_prefix.rstrip("/")
    return f"{prefix}/{settings.glue_database}/{glue_table}"


def read_current_pointer(
    settings: DuckLakeIcebergSettings, table_name: str
) -> ExportPointer | None:
    """The table's last export pointer, or ``None`` before the first export."""
    location = _table_location(settings, table_name)
    try:
        return read_pointer(build_file_io(settings), location)
    except PointerNotFoundError:
        return None


def resolve_metadata_location(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    *,
    explicit: str | None = None,
) -> tuple[str, ExportPointer | None]:
    """``--metadata-location`` > pointer > error. Never scans the prefix."""
    if explicit is not None:
        return explicit, None
    io = build_file_io(settings)
    location = _table_location(settings, table_name)
    pointer = read_pointer(io, location)
    return pointer.metadata_location, pointer


def _read_metadata_properties(io: FileIO, metadata_location: str) -> dict[str, str]:
    """Read the ``ducklake.*`` properties from ``metadata.json``."""
    from pyiceberg.serializers import FromInputFile

    input_file = io.new_input(metadata_location)
    if not input_file.exists():
        raise PointerError(
            f"pointer points to {metadata_location}, which does not exist",
            hint="re-export with ducklake-iceberg sync",
        )
    metadata = FromInputFile.table_metadata(input_file)
    return dict(metadata.properties)


def _check_coherence(
    properties: dict[str, str],
    *,
    pointer: ExportPointer | None,
    schema_name: str,
    table_name: str,
    metadata_location: str,
) -> None:
    """Cross-check pointer and metadata. A divergence is exit 3."""
    meta_snapshot = properties.get(PROP_SNAPSHOT_ID)
    meta_table = properties.get("ducklake.table")

    if pointer is not None:
        if meta_snapshot is not None and meta_snapshot != str(
            pointer.ducklake_snapshot_id
        ):
            raise DivergenceError(
                f"pointer (snapshot {pointer.ducklake_snapshot_id}) does not "
                f"match {metadata_location} (snapshot {meta_snapshot})"
            )
    else:
        # explicit --metadata-location: check that it is the right table.
        if meta_table is not None and meta_table != table_name:
            raise DivergenceError(
                f"{metadata_location} belongs to table {meta_table!r}, "
                f"not {table_name!r}"
            )


def run_register(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    *,
    metadata_location: str | None = None,
    create_database: bool = True,
    dry_run: bool = False,
    handoff: ExportPointer | None = None,
    metadata: TableMetadata | None = None,
) -> RegisterResult:
    """Resolve the ``metadata_location``, check coherence, and publish.

    ``handoff``/``metadata``: the current pointer and the ``metadata.json`` that
    the export of the same ``sync`` just read/wrote, to avoid re-reading them
    from S3.
    """
    target = GlueTarget(
        database=settings.glue_database,
        table=normalize_table_name(table_name),
        region=settings.aws_region,
        profile=settings.aws_profile,
        catalog_id=None,
        s3_endpoint=settings.s3_endpoint,
    )
    if handoff is not None and metadata is None:
        # export without changes: the common case is Glue already pointing at the
        # pointer, decided by a single GetTable without reading metadata.json
        _raise_if_published(target, handoff.metadata_location)

    pointer: ExportPointer | None
    if handoff is not None:
        resolved, pointer = handoff.metadata_location, handoff
    else:
        resolved, pointer = resolve_metadata_location(
            settings, schema_name, table_name, explicit=metadata_location
        )

    properties = (
        dict(metadata.properties)
        if metadata is not None
        else _read_metadata_properties(build_file_io(settings), resolved)
    )
    _check_coherence(
        properties,
        pointer=pointer,
        schema_name=schema_name,
        table_name=table_name,
        metadata_location=resolved,
    )
    return glue_register(
        target,
        resolved,
        create_database=create_database,
        dry_run=dry_run,
        data_path=settings.data_path,
        metadata=metadata,
    )


def _raise_if_published(target: GlueTarget, metadata_location: str) -> None:
    """``TableAlreadyRegistered`` if Glue already points at ``metadata_location``.

    Exact match only: a non-Iceberg table, a missing table, or a different
    pointer follows the full ``glue.register`` flow, with its own messages.
    """
    table = describe_table(target)
    if table is None:
        return
    params = table.get("Parameters")
    if not isinstance(params, dict):
        return
    if (
        str(params.get("table_type", "")).upper() == "ICEBERG"
        and params.get("metadata_location") == metadata_location
    ):
        raise TableAlreadyRegistered(
            f"{target.database}.{target.table} already published at "
            f"{metadata_location}",
            metadata_location=metadata_location,
        )


# Public API re-exported for explicit use by the orchestrator (cli.py).
__all__ = [
    "POINTER_NAME",
    "POINTER_VERSION",
    "ExportPointer",
    "PointerError",
    "build_file_io",
    "clear_file_io_cache",
    "pointer_path",
    "read_current_pointer",
    "read_pointer",
    "resolve_metadata_location",
    "run_register",
    "utc_now_iso",
    "write_pointer",
]
