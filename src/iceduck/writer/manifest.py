"""Manifest writer and verifier (``manifest_entry`` Avro).

A manifest's Avro schema is **not fixed**: the ``partition`` field of
``data_file`` is a struct whose fields come from the table's partition spec,
with synthetic field IDs. The chain that resolves this (verified) is
``spec.partition_type(schema)`` -> ``data_file_with_partition`` ->
``manifest_entry_schema_with_data_file`` -> ``AvroSchemaConversion`` ->
``fastavro.parse_schema``.

The header metadata is mandatory (``schema``, ``partition-spec``,
``partition-spec-id``, ``format-version``, ``content``): without ``schema``/
``partition-spec`` the reader cannot interpret the partition struct. The
post-write verification re-reads with PyIceberg's own reader and **aborts
before** any ``metadata.json`` is written -- the success criterion is the
round-trip against the reference implementation, not "the file opens".
"""

from __future__ import annotations

from io import BytesIO
from typing import TYPE_CHECKING, Any, Final, Literal, cast

import fastavro
from pyiceberg.avro.file import AvroFile
from pyiceberg.manifest import (
    MANIFEST_ENTRY_SCHEMAS,
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    data_file_with_partition,
    manifest_entry_schema_with_data_file,
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
    from pyiceberg.partitioning import PartitionSpec

#: Schema type that ``fastavro.parse_schema`` returns and ``writer`` accepts.
_AvroSchema = dict[str, Any] | list[Any] | str

#: read_types/read_enums to re-read the manifest. `Any` as the value because
#: `AvroFile` expects `Callable[..., StructProtocol]`/`Callable[..., Enum]`,
#: which the classes satisfy but mypy's invariant dict does not accept.
_ENTRY_READ_TYPES: Final[dict[int, Any]] = {-1: ManifestEntry, 2: DataFile}
_ENTRY_READ_ENUMS: Final[dict[int, Any]] = {
    0: ManifestEntryStatus,
    101: FileFormat,
    134: DataFileContent,
}


def _manifest_entry_struct(
    schema: Schema, spec: PartitionSpec, format_version: int
) -> tuple[_AvroSchema, StructType]:
    version = cast("Literal[1, 2, 3]", format_version)
    ptype = spec.partition_type(schema)
    entry_schema = manifest_entry_schema_with_data_file(
        version, data_file_with_partition(ptype, version)
    )
    avro_schema = fastavro.parse_schema(
        AvroSchemaConversion().iceberg_to_avro(
            Schema(*entry_schema.fields), schema_name="manifest_entry"
        )
    )
    return avro_schema, StructType(*entry_schema.fields)


def write_manifest(
    io: FileIO,
    path: str,
    entries: list[ManifestEntry],
    schema: Schema,
    spec: PartitionSpec,
    content: ManifestContent,
    format_version: int = 2,
) -> int:
    """Write an Avro manifest and return its size in bytes.

    Post-write verification is mandatory: it re-reads with PyIceberg's reader
    and aborts (``ManifestSerializationError``) if the entry count differs.
    """
    payload = encode_manifest(path, entries, schema, spec, content, format_version)
    upload(io, path, payload)
    return len(payload)


def encode_manifest(
    path: str,
    entries: list[ManifestEntry],
    schema: Schema,
    spec: PartitionSpec,
    content: ManifestContent,
    format_version: int = 2,
) -> bytes:
    """Serialize and verify a manifest in memory, without writing it.

    The re-read uses PyIceberg's reader over the bytes that will be uploaded.
    """
    require_format_version(format_version)
    avro_schema, struct = _manifest_entry_struct(schema, spec, format_version)
    records = [to_avro_value(entry, struct) for entry in entries]

    metadata = {
        "schema": schema.model_dump_json(),
        "schema-id": str(schema.schema_id),
        "partition-spec": _spec_json(spec),
        "partition-spec-id": str(spec.spec_id),
        "format-version": str(format_version),
        "content": "data" if content == ManifestContent.DATA else "deletes",
    }

    buffer = BytesIO()
    try:
        fastavro.writer(
            buffer, avro_schema, records, metadata=metadata, codec="deflate"
        )
    except (ValueError, AttributeError) as exc:
        raise ManifestSerializationError(
            f"failed to serialize manifest {path}: {exc}"
        ) from exc

    payload = buffer.getvalue()
    reread = verify_manifest(PayloadFileIO(path, payload), path, format_version)
    if len(reread) != len(entries):
        raise ManifestSerializationError(
            f"manifest {path} did not re-read identically: "
            f"wrote {len(entries)} entries, read {len(reread)}"
        )
    return payload


def verify_manifest(
    io: FileIO, path: str, format_version: int = 2
) -> list[ManifestEntry]:
    """Re-read the manifest with PyIceberg's reader."""
    require_format_version(format_version)
    version = cast("Literal[1, 2, 3]", format_version)
    with AvroFile[ManifestEntry](
        io.new_input(path),
        MANIFEST_ENTRY_SCHEMAS[version],
        read_types=_ENTRY_READ_TYPES,
        read_enums=_ENTRY_READ_ENUMS,
    ) as reader:
        return list(reader)


def _spec_json(spec: PartitionSpec) -> str:
    """Serialize the spec for the header; ``model_dump_json`` when available."""
    dump = getattr(spec, "model_dump_json", None)
    if callable(dump):
        return str(dump())
    import json

    return json.dumps(
        {
            "spec-id": spec.spec_id,
            "fields": [
                {
                    "source-id": f.source_id,
                    "field-id": f.field_id,
                    "name": f.name,
                    "transform": str(f.transform),
                }
                for f in spec.fields
            ],
        }
    )
