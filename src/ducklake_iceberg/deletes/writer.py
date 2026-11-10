"""Position delete file.

Writes Iceberg v2 position delete files (Parquet files with the file_path and
pos columns) from the deleted positions read from DuckLake.
"""

from __future__ import annotations

import dataclasses
import logging
import uuid
from collections.abc import Mapping, Sequence
from io import BytesIO
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.conversions import to_bytes
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from ducklake_iceberg import timing
from ducklake_iceberg.errors import DuckLakeIcebergError
from ducklake_iceberg.writer.avro import upload

if TYPE_CHECKING:
    from pyiceberg.io import FileIO
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.typedef import Record

LOG = logging.getLogger(__name__)

# Correct Iceberg schema for position deletes
# Do NOT use PyIceberg's POSITIONAL_DELETE_SCHEMA: it diverges from the spec
POSITION_DELETE_SCHEMA: Schema = Schema(
    NestedField(2147483546, "file_path", StringType(), required=True),
    NestedField(2147483545, "pos", LongType(), required=True),
)


# -- exceptions ---------------------------------------------------------------


class InvalidDeletePositionError(DuckLakeIcebergError):
    """Invalid delete position."""

    def __init__(self, pos: int, record_count: int, path: str) -> None:
        super().__init__(
            f"Position {pos} >= record_count {record_count} in {path}",
            hint="Bug in delete reading or visibility filtering",
        )


class MissingDataFileError(DuckLakeIcebergError):
    """Delete references a data file missing from the snapshot."""

    def __init__(self, path: str) -> None:
        super().__init__(
            f"Delete references {path}, which is missing from the snapshot",
            hint="Check the temporal filtering",
        )


class PartitionMismatchError(DuckLakeIcebergError):
    """Delete partition differs from the data file partition."""

    def __init__(self, delete_partition: str, data_partition: str) -> None:
        super().__init__(
            f"Delete of partition {delete_partition} points to a data file "
            f"of partition {data_partition}",
            hint="Grouping bug",
        )


class DeleteFileExistsError(DuckLakeIcebergError):
    """Delete file already exists."""

    def __init__(self, path: str) -> None:
        super().__init__(
            f"{path} already exists",
            hint="A UUID-based name must be unique",
        )


# -- data contracts -----------------------------------------------------


@dataclasses.dataclass(frozen=True, slots=True)
class PositionDelete:
    """A deleted position in a data file."""

    data_file_path: str  # absolute path of the Iceberg data file
    pos: int  # 0-based position within the data file
    partition: Record  # partition of the target data file


@dataclasses.dataclass(frozen=True, slots=True)
class DeleteFileResult:
    """Result of writing a delete file."""

    data_file: DataFile
    referenced_data_file: str | None  # path of the single data file, if applicable
    record_count: int
    file_size_in_bytes: int


# -- write functions -----------------------------------------------------


def _deduplicate_positions(
    deletes: Sequence[PositionDelete],
) -> list[PositionDelete]:
    """Remove duplicate (data_file_path, pos) entries.

    Assumes the list is already sorted by (data_file_path, pos).
    """
    if not deletes:
        return []

    result = [deletes[0]]
    for d in deletes[1:]:
        prev = result[-1]
        if d.data_file_path != prev.data_file_path or d.pos != prev.pos:
            result.append(d)

    if len(result) < len(deletes):
        LOG.info(
            "Deduplicated %d duplicate positions in delete file",
            len(deletes) - len(result),
        )

    return result


def _group_by_partition(
    deletes: Sequence[PositionDelete],
) -> Mapping[Record, list[PositionDelete]]:
    """Group deletes by partition."""
    by_partition: dict[Record, list[PositionDelete]] = {}
    for d in deletes:
        by_partition.setdefault(d.partition, []).append(d)
    return by_partition


def _sort_and_deduplicate(
    deletes: list[PositionDelete],
) -> list[PositionDelete]:
    """Sort by (file_path, pos) and remove duplicates."""
    deletes.sort(key=lambda d: (d.data_file_path, d.pos))
    return _deduplicate_positions(deletes)


def _determine_referenced_data_file(
    deletes: Sequence[PositionDelete],
) -> str | None:
    """Return referenced_data_file if all positions point to a single file."""
    if not deletes:
        return None

    data_files = {d.data_file_path for d in deletes}
    if len(data_files) == 1:
        return data_files.pop()
    return None


def write_position_delete_file(
    io: FileIO,
    deletes: Sequence[PositionDelete],
    location: str,
    partition: Record,
    spec: PartitionSpec,
    *,
    path: str | None = None,
    check_exists: bool = True,
) -> DeleteFileResult:
    """Write a position delete file for one partition.

    Args:
        io: PyIceberg FileIO
        deletes: Deleted positions (already filtered for this partition)
        location: Table base location
        partition: Partition record
        spec: Table PartitionSpec
        path: Explicit path; ``None`` generates ``{location}/deletes/{uuid}``.
        check_exists: refuse to overwrite (existence check on S3);
            paths containing a uuid4 may skip it.

    Returns:
        DeleteFileResult with the DataFile and metadata
    """
    if not deletes:
        raise ValueError("deletes must not be empty")

    # Sort and deduplicate
    deletes_sorted = _sort_and_deduplicate(list(deletes))

    # Convert to PyArrow
    pa_schema = schema_to_pyarrow(POSITION_DELETE_SCHEMA)
    table = pa.table(
        {
            "file_path": [d.data_file_path for d in deletes_sorted],
            "pos": [d.pos for d in deletes_sorted],
        },
        schema=pa_schema,
    )

    # Generate a unique path
    if path is None:
        path = f"{location}/deletes/{uuid.uuid4()}-deletes.parquet"

    # Serialize in memory: the size comes from the buffer, no S3 HEAD needed
    buffer = BytesIO()
    with timing.phase("deletes.encode"):
        pq.write_table(table, buffer, compression="zstd")
    payload = buffer.getvalue()
    file_size = len(payload)
    try:
        with timing.phase("deletes.upload"):
            upload(io, path, payload, check_exists=check_exists)
    except Exception as e:
        if "already exists" in str(e).lower():
            raise DeleteFileExistsError(path) from e
        raise DuckLakeIcebergError(f"Failed to write delete file: {e}") from e

    # Determine referenced_data_file
    referenced = _determine_referenced_data_file(deletes_sorted)

    # Create the DataFile. In v2 the reader derives the referenced file from
    # the `file_path` bounds (lower == upper => a single data file): this is
    # what Athena's Iceberg 1.4 and PyIceberg use to match delete and data
    # files. The bounds are written in full, untruncated, so the equality holds.
    lower, upper, counts = _delete_metrics(deletes_sorted)
    data_file = DataFile.from_args(
        content=DataFileContent.POSITION_DELETES,
        file_path=path,
        file_format=FileFormat.PARQUET,
        partition=partition,
        record_count=len(deletes_sorted),
        file_size_in_bytes=file_size,
        column_sizes=None,
        value_counts=counts,
        null_value_counts=dict.fromkeys(counts, 0),
        nan_value_counts=None,
        lower_bounds=lower,
        upper_bounds=upper,
        key_metadata=None,
        split_offsets=None,
        equality_ids=None,
        sort_order_id=None,  # None, not 0
        spec_id=spec.spec_id,
    )

    return DeleteFileResult(
        data_file=data_file,
        referenced_data_file=referenced,
        record_count=len(deletes_sorted),
        file_size_in_bytes=file_size,
    )


def _delete_metrics(
    deletes: Sequence[PositionDelete],
) -> tuple[dict[int, bytes], dict[int, bytes], dict[int, int]]:
    """Bounds and counts for ``file_path``/``pos`` (list already sorted)."""
    path_id = POSITION_DELETE_SCHEMA.find_field("file_path").field_id
    pos_id = POSITION_DELETE_SCHEMA.find_field("pos").field_id
    positions = [d.pos for d in deletes]
    lower = {
        path_id: to_bytes(StringType(), deletes[0].data_file_path),
        pos_id: to_bytes(LongType(), min(positions)),
    }
    upper = {
        path_id: to_bytes(StringType(), deletes[-1].data_file_path),
        pos_id: to_bytes(LongType(), max(positions)),
    }
    return lower, upper, {path_id: len(deletes), pos_id: len(deletes)}


def write_position_deletes(
    io: FileIO,
    deletes: Sequence[PositionDelete],
    location: str,
    spec: PartitionSpec,
) -> list[DeleteFileResult]:
    """Write position delete files, one per partition.

    Args:
        io: PyIceberg FileIO
        deletes: All deleted positions
        location: Table base location
        spec: Table PartitionSpec

    Returns:
        List of DeleteFileResult, one per partition
    """
    if not deletes:
        return []

    # Group by partition
    by_partition = _group_by_partition(deletes)

    LOG.info(
        "Writing %d delete file(s) for %d partition(s)",
        len(by_partition),
        len(by_partition),
    )

    # Write one file per partition
    results = []
    for partition, group in by_partition.items():
        result = write_position_delete_file(io, group, location, partition, spec)
        results.append(result)

    total_positions = sum(r.record_count for r in results)
    total_size = sum(r.file_size_in_bytes for r in results)
    LOG.info(
        "Delete files written: %d position(s) in %d bytes",
        total_positions,
        total_size,
    )

    return results


def validate_deletes(
    deletes: Sequence[PositionDelete],
    live_data_files: Mapping[str, int],
) -> None:
    """Validate delete positions against live data files.

    Args:
        deletes: Deleted positions
        live_data_files: Map of path -> record_count for live data files

    Raises:
        InvalidDeletePositionError: position >= record_count
        MissingDataFileError: data file is not live
    """
    for d in deletes:
        # Check that the data file exists
        if d.data_file_path not in live_data_files:
            raise MissingDataFileError(d.data_file_path)

        # Check that the position is within range
        record_count = live_data_files[d.data_file_path]
        if d.pos >= record_count:
            raise InvalidDeletePositionError(d.pos, record_count, d.data_file_path)
