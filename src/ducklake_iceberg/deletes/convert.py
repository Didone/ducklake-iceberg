"""Conversion of a DuckLake delete file into an Iceberg v2 position delete file.

This is the path used by ``export``. It starts from the (data file, delete file)
pair already validated by ``attach_delete_files`` and produces **one** Iceberg
position delete file per DuckLake delete file:

- DuckLake keeps one live, cumulative delete file per data file (a new
  ``DELETE`` writes another file holding all positions), so "one to one"
  preserves the granularity of the source exactly and every Iceberg delete file
  references a single data file (equal ``file_path`` bounds);
- the Iceberg file name carries a **source key** derived from the path of the
  DuckLake delete file. The incremental sync compares it: if the DuckLake
  delete file changed path, Iceberg removes the old one and adds the new one;
  if the path is the same, there is nothing to rewrite (the same rule as for
  data files, applied to deletes);
- only positions visible in the exported snapshot are included: the DuckLake
  delete file may carry ``_ducklake_internal_snapshot_id`` per row, and a row
  with a snapshot greater than the target has not happened yet.

The application order follows from where the file enters: the delete file is
always written in the same snapshot as, or after, the data file it references,
so ``seq(delete) >= seq(data)`` and the reader applies it; since position
deletes name the data file path, they never reach rows of files added later.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from typing import TYPE_CHECKING, Final

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ducklake_iceberg.deletes.writer import (
    InvalidDeletePositionError,
    PositionDelete,
    write_position_delete_file,
)
from ducklake_iceberg.errors import DuckLakeIcebergError

if TYPE_CHECKING:
    from pyiceberg.io import FileIO
    from pyiceberg.manifest import DataFile
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.typedef import Record

    from ducklake_iceberg.catalog.files import LiveDeleteFile

#: Field IDs of the DuckLake delete file columns (verified at runtime):
#: ``file_path``/``pos`` have their own IDs (not Iceberg's) and the internal
#: snapshot column appears in partial files.
DUCKLAKE_DELETE_POS_FIELD_ID: Final[int] = 2147483645
DUCKLAKE_DELETE_SNAPSHOT_FIELD_ID: Final[int] = 2147483539

_KEY_LENGTH: Final[int] = 24
_NAME_RE: Final[re.Pattern[str]] = re.compile(
    rf"/deletes/(?P<key>[0-9a-f]{{{_KEY_LENGTH}}})-[0-9a-f-]{{36}}-deletes\.parquet$"
)


class DeleteConversionError(DuckLakeIcebergError):
    """DuckLake delete file is unreadable or violates the contract (exit 1)."""


def source_key(canonical_delete_path: str) -> str:
    """Stable key of the source DuckLake delete file."""
    digest = hashlib.sha256(canonical_delete_path.encode("utf-8")).hexdigest()
    return digest[:_KEY_LENGTH]


def delete_file_path(location: str, key: str) -> str:
    """``{location}/deletes/{key}-{uuid4}-deletes.parquet``."""
    return f"{location.rstrip('/')}/deletes/{key}-{uuid.uuid4()}-deletes.parquet"


def parse_source_key(path: str) -> str | None:
    """Source key of a delete file written by ducklake-iceberg, or ``None``."""
    match = _NAME_RE.search(path)
    return match.group("key") if match else None


def _column_by_field_id(schema: pa.Schema, field_id: int) -> str | None:
    for field in schema:
        raw = (field.metadata or {}).get(b"PARQUET:field_id")
        if raw is not None and int(raw) == field_id:
            return str(field.name)
    return None


def read_visible_positions(
    io: FileIO,
    delete: LiveDeleteFile,
    *,
    snapshot_id: int,
    record_count: int,
) -> list[int]:
    """Positions visible at ``snapshot_id``, sorted and de-duplicated.

    Raises ``InvalidDeletePositionError`` if any position falls outside
    ``[0, record_count)``: that would indicate a wrong pairing with the data
    file, and publishing it would delete the wrong rows.
    """
    with io.new_input(delete.path).open() as stream:
        table = pq.read_table(stream)

    pos_col = _column_by_field_id(table.schema, DUCKLAKE_DELETE_POS_FIELD_ID)
    if pos_col is None:
        raise DeleteConversionError(
            f"delete file {delete.path} has no position column "
            f"(field_id {DUCKLAKE_DELETE_POS_FIELD_ID})"
        )
    snap_col = _column_by_field_id(table.schema, DUCKLAKE_DELETE_SNAPSHOT_FIELD_ID)
    if snap_col is not None:
        table = table.filter(pc.less_equal(table[snap_col], snapshot_id))

    positions = sorted({int(p) for p in table[pos_col].to_pylist()})
    if positions and (positions[0] < 0 or positions[-1] >= record_count):
        bad = positions[0] if positions[0] < 0 else positions[-1]
        raise InvalidDeletePositionError(bad, record_count, delete.path)
    return positions


def convert_delete_file(
    io: FileIO,
    location: str,
    delete: LiveDeleteFile,
    *,
    data_file_path: str,
    record_count: int,
    partition: Record,
    spec: PartitionSpec,
    snapshot_id: int,
) -> DataFile | None:
    """Write the equivalent Iceberg position delete file; ``None`` if empty."""
    positions = read_visible_positions(
        io, delete, snapshot_id=snapshot_id, record_count=record_count
    )
    if not positions:
        return None
    result = write_position_delete_file(
        io,
        [PositionDelete(data_file_path, pos, partition) for pos in positions],
        location,
        partition,
        spec,
        path=delete_file_path(location, source_key(delete.canonical_path)),
        check_exists=False,  # the name contains a uuid4
    )
    return result.data_file
