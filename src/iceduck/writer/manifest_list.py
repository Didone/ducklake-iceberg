"""Manifest list writer and verifier (``manifest_file`` Avro).

Unlike the manifest, the manifest list schema **does not depend on the spec**
-- it is fixed per version (``MANIFEST_LIST_FILE_SCHEMAS[v]``). The
``partitions`` field is a list of ``PartitionFieldSummary``, one per spec
field, aggregating the values of **all** files in the manifest; it is produced
by ``construct_partition_summaries``. The post-write verification re-reads
with PyIceberg's ``AvroFile``, as with the manifest.
"""

from __future__ import annotations

from io import BytesIO
from typing import TYPE_CHECKING, Any, Final, Literal, cast

import fastavro
from pyiceberg.avro.file import AvroFile
from pyiceberg.manifest import (
    MANIFEST_LIST_FILE_SCHEMAS,
    ManifestContent,
    ManifestFile,
    PartitionFieldSummary,
)
from pyiceberg.schema import Schema
from pyiceberg.types import StructType
from pyiceberg.utils.schema_conversion import AvroSchemaConversion

from iceduck.writer.avro import (
    ManifestSerializationError,
    PayloadFileIO,
    require_format_version,
    to_avro_value,
    upload,
)

if TYPE_CHECKING:
    from pyiceberg.io import FileIO

#: Schema type that ``fastavro.parse_schema`` returns and ``writer`` accepts.
_AvroSchema = dict[str, Any] | list[Any] | str

#: read_types/read_enums to re-read the manifest list. `Any` as the value for
#: the same reason as in manifest.py (invariant dict vs expected Callable).
_LIST_READ_TYPES: Final[dict[int, Any]] = {
    -1: ManifestFile,
    508: PartitionFieldSummary,
}
_LIST_READ_ENUMS: Final[dict[int, Any]] = {517: ManifestContent}


def _manifest_file_struct(format_version: int) -> tuple[_AvroSchema, StructType]:
    version = cast("Literal[1, 2, 3]", format_version)
    list_type = MANIFEST_LIST_FILE_SCHEMAS[version]
    avro_schema = fastavro.parse_schema(
        AvroSchemaConversion().iceberg_to_avro(
            Schema(*list_type.fields), schema_name="manifest_file"
        )
    )
    return avro_schema, StructType(*list_type.fields)


def write_manifest_list(
    io: FileIO,
    path: str,
    manifests: list[ManifestFile],
    snapshot_id: int,
    parent_snapshot_id: int | None,
    sequence_number: int,
    format_version: int = 2,
) -> int:
    """Write the Avro manifest list and return its size in bytes."""
    payload = encode_manifest_list(
        path,
        manifests,
        snapshot_id,
        parent_snapshot_id,
        sequence_number,
        format_version,
    )
    upload(io, path, payload)
    return len(payload)


def encode_manifest_list(
    path: str,
    manifests: list[ManifestFile],
    snapshot_id: int,
    parent_snapshot_id: int | None,
    sequence_number: int,
    format_version: int = 2,
) -> bytes:
    """Serialize and verify the manifest list in memory, without writing it."""
    require_format_version(format_version)
    avro_schema, struct = _manifest_file_struct(format_version)
    records = [to_avro_value(manifest, struct) for manifest in manifests]

    metadata = {
        "snapshot-id": str(snapshot_id),
        "parent-snapshot-id": (
            "null" if parent_snapshot_id is None else str(parent_snapshot_id)
        ),
        "sequence-number": str(sequence_number),
        "format-version": str(format_version),
    }

    buffer = BytesIO()
    try:
        fastavro.writer(
            buffer, avro_schema, records, metadata=metadata, codec="deflate"
        )
    except (ValueError, AttributeError) as exc:
        raise ManifestSerializationError(
            f"failed to serialize manifest list {path}: {exc}"
        ) from exc

    payload = buffer.getvalue()
    reread = verify_manifest_list(PayloadFileIO(path, payload), path, format_version)
    if len(reread) != len(manifests):
        raise ManifestSerializationError(
            f"manifest list {path} did not re-read identically: "
            f"wrote {len(manifests)}, read {len(reread)}"
        )
    return payload


def verify_manifest_list(
    io: FileIO, path: str, format_version: int = 2
) -> list[ManifestFile]:
    """Re-read the manifest list with PyIceberg's reader."""
    require_format_version(format_version)
    version = cast("Literal[1, 2, 3]", format_version)
    with AvroFile[ManifestFile](
        io.new_input(path),
        MANIFEST_LIST_FILE_SCHEMAS[version],
        read_types=_LIST_READ_TYPES,
        read_enums=_LIST_READ_ENUMS,
    ) as reader:
        return list(reader)
