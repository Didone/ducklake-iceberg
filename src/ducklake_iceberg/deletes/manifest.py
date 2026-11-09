"""Delete manifest.

Records position delete files in a separate manifest with content 'deletes'
and references it in the manifest list with ManifestContent.DELETES. It fully
reuses the Avro manifest writer; what changes is the content, the header, the
data_file struct, and the accounting.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Final

from pyiceberg.manifest import (
    DATA_FILE_TYPE,
    DataFile,
    DataFileContent,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    construct_partition_summaries,
)
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, StructType

from ducklake_iceberg.deletes.writer import DeleteFileResult
from ducklake_iceberg.errors import DuckLakeIcebergError
from ducklake_iceberg.writer.manifest import verify_manifest, write_manifest
from ducklake_iceberg.writer.sequence import INHERITED

if TYPE_CHECKING:
    from pyiceberg.io import FileIO
    from pyiceberg.partitioning import PartitionSpec

LOG = logging.getLogger(__name__)

# Field 143: referenced_data_file, absent from DATA_FILE_TYPE[2] in PyIceberg 0.12.0.
# Not written by export (see write_delete_manifest); kept for the v3 struct.
REFERENCED_DATA_FILE_FIELD: NestedField = NestedField(
    143, "referenced_data_file", StringType(), required=False
)

# Field ID of ``file_path`` in the position delete file schema (Iceberg spec)
_FILE_PATH_FIELD_ID: Final[int] = 2147483546


# -- exceptions ---------------------------------------------------------------


class DeleteManifestError(DuckLakeIcebergError):
    """Error while writing a delete manifest."""


class MixedContentError(DeleteManifestError):
    """POSITION_DELETES entry in a manifest with content: data."""

    def __init__(self, path: str) -> None:
        super().__init__(
            f"Mixed content in manifest {path}",
            hint="Bug: keep the manifests separate",
        )


class InvalidSequenceNumberError(DeleteManifestError):
    """min_sequence_number > sequence_number."""

    def __init__(self, min_seq: int, seq: int) -> None:
        super().__init__(
            f"min_sequence_number ({min_seq}) > sequence_number ({seq})",
            hint="Accounting bug",
        )


class MultipleReferencedFilesError(DeleteManifestError):
    """referenced_data_file with more than one data file."""

    def __init__(self, count: int) -> None:
        super().__init__(
            f"referenced_data_file with {count} referenced files",
            hint="Grouping bug",
        )


class DeleteManifestVerificationError(DeleteManifestError):
    """Delete manifest did not read back correctly."""

    def __init__(self, path: str) -> None:
        super().__init__(
            f"Delete manifest {path} did not read back correctly",
            hint="Abort before writing metadata.json",
        )


# -- helpers -----------------------------------------------------


def delete_file_type_v2(partition_type: StructType) -> StructType:
    """Extend DATA_FILE_TYPE[2] with referenced_data_file.

    PyIceberg 0.12.0 only defines field 143 in v3. The spec declares it
    as optional in both v2 and v3.
    """
    # Copy the DATA_FILE_TYPE[2] fields, replacing the partition type
    fields = [
        (
            f.model_copy(update={"field_type": partition_type})
            if f.field_id == 102
            else f
        )
        for f in DATA_FILE_TYPE[2].fields
    ]
    # Add referenced_data_file
    fields.append(REFERENCED_DATA_FILE_FIELD)
    return StructType(*fields)


def _check_referenced_bounds(result: DeleteFileResult) -> None:
    """Check ``referenced_data_file`` against the ``file_path`` bounds.

    In v2 the reader derives the referenced data file from the ``file_path``
    bounds (``lower == upper``); that is what export publishes. When the
    writer declares a single referenced file, the bounds must say the same
    thing, otherwise the reader would apply the delete to a different set of
    files.
    """
    referenced = result.referenced_data_file
    if referenced is None:
        return
    lower = (result.data_file.lower_bounds or {}).get(_FILE_PATH_FIELD_ID)
    upper = (result.data_file.upper_bounds or {}).get(_FILE_PATH_FIELD_ID)
    if lower is not None and upper is not None and lower != upper:
        raise MultipleReferencedFilesError(2)
    expected = referenced.encode()
    if lower is not None and lower != expected:
        raise DeleteManifestError(
            f"file_path bounds of {result.data_file.file_path} do not match "
            f"referenced_data_file {referenced}",
            hint="Grouping bug",
        )


def _create_delete_manifest_entry(
    data_file: DataFile,
    snapshot_id: int,
) -> ManifestEntry:
    """Create an ADDED ManifestEntry for a delete file.

    ``sequence_number`` and ``file_sequence_number`` are left unset (inherited):
    the entry inherits the sequence number of the manifest in the manifest
    list (v2).
    """
    if data_file.content != DataFileContent.POSITION_DELETES:
        raise MixedContentError(data_file.file_path)

    return ManifestEntry.from_args(
        status=ManifestEntryStatus.ADDED,
        snapshot_id=snapshot_id,
        sequence_number=INHERITED,
        file_sequence_number=INHERITED,
        data_file=data_file,
    )


# -- main function -------------------------------------------------------


def write_delete_manifest(
    io: FileIO,
    path: str,
    delete_files: list[DeleteFileResult],
    schema: Schema,
    spec: PartitionSpec,
    snapshot_id: int,
    format_version: int = 2,
    *,
    sequence_number: int,
) -> ManifestFile:
    """Write a delete manifest and return its manifest list entry.

    All entries are ADDED with an inherited sequence number, so the manifest
    has ``sequence_number == min_sequence_number ==`` the commit sequence. The
    ``data_file`` struct is the one from ``DATA_FILE_TYPE[2]``, **without**
    field 143 (``referenced_data_file``): export uses the ``file_path``
    bounds, the only mechanism that Athena's Iceberg 1.4 and PyIceberg 0.12
    understand in v2. The result's ``referenced_data_file`` is only checked
    against those bounds; ``delete_file_type_v2`` remains available for
    anyone who needs the v3 struct.

    Args:
        io: PyIceberg FileIO
        path: Path where the manifest is written
        delete_files: Results from write_position_deletes
        schema: Table schema
        spec: Table PartitionSpec
        snapshot_id: Current snapshot ID
        format_version: Iceberg format version (2)
        sequence_number: Sequence number of the commit writing the manifest

    Returns:
        ManifestFile to include in the manifest list

    Raises:
        DeleteManifestError: on failure
    """
    if format_version != 2:
        raise ValueError(f"Only format_version=2 is supported, got {format_version}")

    if not delete_files:
        raise ValueError("delete_files must not be empty")

    # ADDED entries: min_sequence_number is the commit sequence itself
    min_sequence_number = sequence_number
    validate_sequence_numbers(sequence_number, min_sequence_number)

    entries: list[ManifestEntry] = []
    for result in delete_files:
        _check_referenced_bounds(result)
        entries.append(_create_delete_manifest_entry(result.data_file, snapshot_id))

    # Write the manifest with content = DELETES (re-reads and checks the count)
    manifest_size = write_manifest(
        io,
        path,
        entries,
        schema,
        spec,
        ManifestContent.DELETES,
        format_version,
    )

    # Post-write check: only position deletes may come back
    reread = verify_manifest(io, path, format_version)
    if not all(e.data_file.content == DataFileContent.POSITION_DELETES for e in reread):
        raise DeleteManifestVerificationError(path)

    LOG.info(
        "Delete manifest written: %s (%d bytes, %d entries)",
        path,
        manifest_size,
        len(entries),
    )

    return ManifestFile.from_args(
        manifest_path=path,
        manifest_length=manifest_size,
        partition_spec_id=spec.spec_id,
        content=ManifestContent.DELETES,
        sequence_number=sequence_number,
        min_sequence_number=min_sequence_number,
        added_snapshot_id=snapshot_id,
        added_files_count=len(entries),
        existing_files_count=0,
        deleted_files_count=0,
        added_rows_count=sum(e.data_file.record_count for e in entries),
        existing_rows_count=0,
        deleted_rows_count=0,
        partitions=construct_partition_summaries(
            spec, schema, [e.data_file.partition for e in entries]
        ),
        key_metadata=None,
    )


def validate_sequence_numbers(
    sequence_number: int,
    min_sequence_number: int,
) -> None:
    """Validate that min_sequence_number <= sequence_number."""
    if min_sequence_number > sequence_number:
        raise InvalidSequenceNumberError(min_sequence_number, sequence_number)
