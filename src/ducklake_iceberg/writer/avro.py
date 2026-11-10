"""Iceberg-to-Avro converter and file naming conventions.

The core of the writer is ``to_avro_value``, which translates PyIceberg data
model objects (``Record``, enums, typed maps) into the form ``fastavro``
accepts. Three verified pitfalls shape the function:

1. ``Record`` is **positional** -- ``getattr(record, "name")`` raises
   ``AttributeError``; fields are accessed by index (``record[i]``) in the
   order of the ``StructType`` fields.
2. An Iceberg map with **non-string keys** does not become an Avro map but an
   **array of records** ``{key, value}`` (``logicalType: map``). Passing a
   ``dict`` straight to fastavro fails. This affects every metric map
   (``column_sizes``, ``value_counts``, ``*_bounds`` etc.), all keyed by int.
3. Enums need ``.value`` -- ``str(FileFormat.PARQUET)`` gives
   ``'FileFormat.PARQUET'``, which Avro rejects; ``.value`` gives ``'PARQUET'``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Sequence
from enum import Enum
from io import BytesIO
from typing import Final, cast

from pyiceberg.io import FileIO, InputFile, InputStream, OutputFile
from pyiceberg.types import IcebergType, ListType, MapType, StringType, StructType

from ducklake_iceberg import timing
from ducklake_iceberg.concurrency import ContextThreadPoolExecutor, io_workers
from ducklake_iceberg.obs.metrics import add_call, add_work

_FORMAT_VERSIONS: Final[frozenset[int]] = frozenset({1, 2, 3})


class UnsupportedFormatVersion(Exception):
    """``format_version`` outside 1-3 (exit 2)."""

    exit_code = 2


class ManifestSerializationError(Exception):
    """Failed to serialize or re-read a manifest (exit 1)."""

    exit_code = 1


def require_format_version(format_version: int) -> None:
    if format_version not in _FORMAT_VERSIONS:
        raise UnsupportedFormatVersion(f"unsupported format version: {format_version}")


def to_avro_value(value: object, itype: IcebergType) -> object:
    """Convert an Iceberg ``Record``/enum/map to the form fastavro accepts.

    Recursive and driven by the **Iceberg type**, not by the shape of the
    Python value: the type decides whether a map becomes a ``dict`` or an
    ``array`` of records.
    """
    if value is None:
        return None
    if isinstance(itype, StructType):
        # Positional Record: access by index, never by name (pitfall 1).
        record = cast("Sequence[object]", value)
        return {
            field.name: to_avro_value(record[i], field.field_type)
            for i, field in enumerate(itype.fields)
        }
    if isinstance(itype, MapType):
        mapping = _as_mapping(value)
        if isinstance(itype.key_type, StringType):
            return {
                key: to_avro_value(val, itype.value_type)
                for key, val in mapping.items()
            }
        # non-string key: array of {key, value} records (pitfall 2).
        return [
            {"key": key, "value": to_avro_value(val, itype.value_type)}
            for key, val in mapping.items()
        ]
    if isinstance(itype, ListType):
        items = cast("Iterable[object]", value)
        return [to_avro_value(item, itype.element_type) for item in items]
    if isinstance(value, Enum):
        return value.value  # pitfall 3
    return value


def _as_mapping(value: object) -> dict[object, object]:
    if isinstance(value, dict):
        return value
    # Some maps arrive as iterable items; normalize defensively.
    items = getattr(value, "items", None)
    if callable(items):
        return dict(items())
    raise ManifestSerializationError(
        f"MapType value is not a mapping: {type(value).__name__}"
    )


# -- file naming conventions -----------------------------------------------


def _join(prefix: str, suffix: str) -> str:
    return f"{prefix.rstrip('/')}/{suffix.lstrip('/')}"


def manifest_name(location: str, ordinal: int) -> str:
    """``{location}/metadata/{uuid4}-m{n}.avro``."""
    return _join(location, f"metadata/{uuid.uuid4()}-m{ordinal}.avro")


def manifest_list_name(location: str, snapshot_id: int, attempt: int = 1) -> str:
    """``{location}/metadata/snap-{snapshot_id}-{attempt}-{uuid4}.avro``."""
    return _join(location, f"metadata/snap-{snapshot_id}-{attempt}-{uuid.uuid4()}.avro")


# -- upload and in-memory verification -------------------------------------

#: default for concurrent metadata uploads (small PUTs, latency-dominated);
#: the effective value comes from ``max_io_workers``
UPLOAD_WORKERS: Final[int] = 8


class _PayloadInput(InputFile):
    def __init__(self, location: str, payload: bytes) -> None:
        super().__init__(location)
        self._payload = payload

    def __len__(self) -> int:
        return len(self._payload)

    def exists(self) -> bool:
        return True

    def open(self, seekable: bool = True) -> InputStream:
        return BytesIO(self._payload)


class PayloadFileIO(FileIO):
    """Read-only FileIO over already-serialized bytes.

    Post-write verification re-reads, with the PyIceberg reader, exactly the
    bytes that were uploaded, without a GET back to S3 per file.
    """

    def __init__(self, location: str, payload: bytes) -> None:
        super().__init__()
        self._location = location
        self._payload = payload

    def new_input(self, location: str) -> InputFile:
        if location != self._location:
            raise FileNotFoundError(location)
        return _PayloadInput(location, self._payload)

    def new_output(self, location: str) -> OutputFile:
        raise PermissionError(f"PayloadFileIO is read-only: {location}")

    def delete(self, location: str | InputFile | OutputFile) -> None:
        raise PermissionError("PayloadFileIO is read-only")


def upload(io: FileIO, path: str, payload: bytes, *, check_exists: bool = True) -> None:
    """Write ``payload`` to ``path``.

    ``check_exists`` preserves "never overwrite", at the cost of an existence
    check (about 3 requests on PyArrow's S3). Names with a uuid4 (manifests,
    manifest list) do not need it.
    """
    with io.new_output(path).create(overwrite=not check_exists) as out:
        out.write(payload)


def upload_all(io: FileIO, payloads: Sequence[tuple[str, bytes]]) -> None:
    """Upload uniquely named (uuid4) files in parallel, without existence checks.

    Phase ``s3_put``: one ``PutObject`` per file.
    """
    with timing.phase("s3.manifests_put"), timing.phase("s3_put"):
        if len(payloads) <= 1:
            for path, payload in payloads:
                upload(io, path, payload, check_exists=False)
        else:
            with ContextThreadPoolExecutor(
                max_workers=io_workers(len(payloads))
            ) as pool:
                for future in [
                    pool.submit(upload, io, path, payload, check_exists=False)
                    for path, payload in payloads
                ]:
                    future.result()
        for _ in payloads:
            add_call("s3_put")
        add_work(
            "s3_put",
            items=len(payloads),
            byte_count=sum(len(payload) for _, payload in payloads),
        )


class _StreamingInput(InputFile):
    def __init__(self, inner: InputFile) -> None:
        super().__init__(inner.location)
        self._inner = inner

    def __len__(self) -> int:
        return len(self._inner)

    def exists(self) -> bool:
        return self._inner.exists()

    def open(self, seekable: bool = True) -> InputStream:
        return self._inner.open(seekable=False)


class StreamingReadFileIO(FileIO):
    """Reads without HEAD on top of another FileIO.

    PyIceberg's Avro reader reads the whole file at once; PyArrow's seekable
    ``open`` does a HEAD (size) before the GET. Streaming makes each manifest
    cost one request, not two.
    """

    def __init__(self, inner: FileIO) -> None:
        super().__init__(inner.properties)
        self._inner = inner

    def new_input(self, location: str) -> InputFile:
        return _StreamingInput(self._inner.new_input(location))

    def new_output(self, location: str) -> OutputFile:
        return self._inner.new_output(location)

    def delete(self, location: str | InputFile | OutputFile) -> None:
        self._inner.delete(location)
