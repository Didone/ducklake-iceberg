"""Shared destination layout and completed-export handoff; no Glue access."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

from ducklake_iceberg import timing
from ducklake_iceberg.errors import ConfigError
from ducklake_iceberg.obs.metrics import add_call, add_work
from ducklake_iceberg.storage import build_file_io

if TYPE_CHECKING:
    from pyiceberg.io import FileIO

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


def normalize_table_name(name: str) -> str:
    """Normalize the destination table component without adding the schema."""
    return "".join(
        c if (c.isalnum() and c.isascii()) or c == "_" else "_"
        for c in name.strip().lower()
    )


def table_location(prefix: str, database: str, table_name: str) -> str:
    return f"{prefix.rstrip('/')}/{database}/{normalize_table_name(table_name)}"


def pointer_path(location: str) -> str:
    """``{location}/_ducklake_iceberg_last_export.json``."""
    return f"{location.rstrip('/')}/{POINTER_NAME}"


def write_pointer(io: FileIO, pointer: ExportPointer) -> str:
    """Write the pointer, **overwriting** - the only object that is overwritten."""
    path = pointer_path(pointer.location)
    payload = json.dumps(pointer.to_json(), indent=2).encode("utf-8")
    output = io.new_output(path)
    with timing.phase("s3.pointer_put"), timing.phase("s3_put"):
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
        with (
            timing.phase("s3.pointer_get"),
            io.new_input(path).open(seekable=False) as stream,
        ):
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


def read_current_pointer(
    settings: DuckLakeIcebergSettings, table_name: str
) -> ExportPointer | None:
    """The table's last export pointer, or ``None`` before the first export."""
    location = table_location(
        settings.iceberg_metadata_prefix, settings.glue_database, table_name
    )
    try:
        return read_pointer(build_file_io(settings), location)
    except PointerNotFoundError:
        return None
