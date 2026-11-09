"""Parquet footer statistics and ``DataFile`` assembly.

Produces an Iceberg ``DataFile`` with ``DATA`` content for each DuckLake
Parquet file, with ``record_count``, ``column_sizes``, ``value_counts``,
``null_value_counts``, ``nan_value_counts``, ``lower_bounds``, ``upper_bounds``
and ``split_offsets`` taken from the **Parquet file's own footer**, never from
the DuckLake catalog. We do not convert textual min/max to typed binary; we
read the bounds the original writer stored.

The workaround has two layers, both necessary (verified at runtime):
``data_file_statistics_from_parquet_metadata`` walks *all* physical columns in
the footer and resolves each one in two maps -- name -> field ID
(``parquet_column_mapping``) and field ID -> collector (``stats_columns``).
DuckLake's two internal columns (``_ducklake_internal_row_id`` and
``_ducklake_internal_snapshot_id``) are in the footer but **not** in the
Iceberg schema; extending only one of the maps just swaps one ``KeyError`` for
another. So we extend both, with ``NONE`` mode so no bound is collected for
them, and we **remove** their field IDs from the metric maps before building
the ``DataFile`` -- they are reserved Iceberg IDs (meaningful in v3) and must
not appear in published manifests.

``Table.add_files`` is never called: it fails with DuckLake Parquet files
exactly at this point (``KeyError: '_ducklake_internal_row_id'``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, cast

import pyarrow.parquet as pq
from pyiceberg.io.pyarrow import (
    MetricModeTypes,
    MetricsMode,
    StatisticsCollector,
    compute_statistics_plan,
    data_file_statistics_from_parquet_metadata,
    parquet_path_to_id_mapping,
    pyarrow_to_schema,
)
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat

from iceduck.concurrency import ContextThreadPoolExecutor

if TYPE_CHECKING:
    from pyiceberg.io import FileIO
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.typedef import Properties, Record
    from pyiceberg.types import PrimitiveType

#: Field IDs of DuckLake internal columns, absent from the Iceberg schema.
#: They collide with reserved Iceberg IDs (``_row_id`` = MAX-107,
#: ``_last_updated_sequence_number`` = MAX-108); inert in v2, dangerous in v3.
DUCKLAKE_INTERNAL_FIELD_IDS: Final[dict[str, int]] = {
    "_ducklake_internal_row_id": 2147483540,
    "_ducklake_internal_snapshot_id": 2147483539,
}

_NONE_MODE: Final[MetricsMode] = MetricsMode(MetricModeTypes.NONE)

#: Metric maps keyed by field ID that need to be filtered.
_METRIC_MAPS: Final[tuple[str, ...]] = (
    "column_sizes",
    "value_counts",
    "null_value_counts",
    "nan_value_counts",
    "lower_bounds",
    "upper_bounds",
)


@dataclass(frozen=True)
class FileToExport:
    """A Parquet file to export, with its partition values."""

    path: str  # absolute s3://...
    file_size_in_bytes: int
    record_count: int  # from ducklake_data_file, for cross-checking
    partition_values: Record  # typed Record (DataFile.partition)


class RecordCountMismatch(RuntimeError):
    """Footer record_count differs from ducklake_data_file."""

    exit_code = 1


class SchemaDesyncError(RuntimeError):
    """Parquet column outside the Iceberg schema and not an internal one."""

    exit_code = 1


def build_data_file(
    file: FileToExport,
    schema: Schema,
    spec: PartitionSpec,
    properties: Properties,
    io: FileIO,
) -> DataFile:
    """Read the Parquet footer and return a ``DataFile`` with ``DATA`` content.

    Raises ``RecordCountMismatch`` if the footer disagrees with the catalog and
    ``SchemaDesyncError`` if the footer has a non-internal column missing from
    the schema.
    """
    with io.new_input(file.path).open() as stream:
        footer = pq.read_metadata(stream)

    column_mapping, plan = _statistics_plan(footer, schema, properties, file.path)
    try:
        stats = data_file_statistics_from_parquet_metadata(
            parquet_metadata=footer,
            stats_columns=plan,
            parquet_column_mapping=column_mapping,
        )
    except KeyError as exc:
        # Physical column with no match in the map derived from the footer
        # itself: the file does not follow DuckLake's field ID convention.
        raise SchemaDesyncError(
            f"column {exc.args[0]!r} exists in Parquet {file.path} but not in "
            f"the Iceberg schema"
        ) from exc

    if stats.record_count != file.record_count:
        raise RecordCountMismatch(
            f"{file.path}: footer={stats.record_count} catalog={file.record_count}"
        )

    payload = stats.to_serialized_dict()
    # schema.field_ids covers the whole tree (struct/list/map), unlike
    # {f.field_id for f in schema.fields}, which is only the root.
    valid_ids = set(schema.field_ids)
    for key in _METRIC_MAPS:
        metric = payload.get(key)
        if isinstance(metric, dict):
            payload[key] = {k: v for k, v in metric.items() if k in valid_ids}

    return DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=file.path,
        file_format=FileFormat.PARQUET,
        partition=file.partition_values,
        file_size_in_bytes=file.file_size_in_bytes,
        spec_id=spec.spec_id,
        sort_order_id=None,
        equality_ids=None,
        key_metadata=None,
        **payload,
    )


def _statistics_plan(
    footer: pq.FileMetaData,
    schema: Schema,
    properties: Properties,
    path: str,
) -> tuple[dict[str, int], dict[int, StatisticsCollector]]:
    """Path -> field ID and field ID -> collector maps, from the **footer**.

    A column's physical path does not identify the field: after ``RENAME`` the
    old Parquet file keeps the old name, and after ``DROP`` it keeps a column
    the current schema does not have. What is stable is the ``field_id`` written
    by DuckLake (= ``column_id``), so the path map comes from the file's own
    schema. Columns not in the Iceberg schema (DuckLake internals and dropped
    columns) and columns whose physical type differs from the current type
    (e.g. ``int`` -> ``long`` promotion) get ``NONE`` mode: they produce no
    bound and their IDs are filtered out of the metrics afterwards.
    """
    try:
        file_schema = pyarrow_to_schema(footer.schema.to_arrow_schema())
    except ValueError as exc:
        raise SchemaDesyncError(
            f"Parquet {path} has no field_id: does not follow the DuckLake convention"
        ) from exc

    column_mapping = dict(parquet_path_to_id_mapping(file_schema))
    plan = dict(compute_statistics_plan(schema, properties))
    current_types = {fid: schema.find_field(fid).field_type for fid in schema.field_ids}
    for field_id in file_schema.field_ids:
        file_field = file_schema.find_field(field_id)
        if not file_field.field_type.is_primitive:
            continue
        if current_types.get(field_id) == file_field.field_type:
            continue
        plan[field_id] = StatisticsCollector(
            field_id=field_id,
            iceberg_type=cast("PrimitiveType", file_field.field_type),
            mode=_NONE_MODE,
            column_name=file_field.name,
        )
    return column_mapping, plan


def file_field_ids(path: str, io: FileIO) -> frozenset[int]:
    """Field IDs present in a Parquet footer (whole tree)."""
    with io.new_input(path).open() as stream:
        footer = pq.read_metadata(stream)
    try:
        file_schema = pyarrow_to_schema(footer.schema.to_arrow_schema())
    except ValueError as exc:
        raise SchemaDesyncError(
            f"Parquet {path} has no field_id: does not follow the DuckLake convention"
        ) from exc
    return frozenset(file_schema.field_ids)


def build_data_files(
    files: list[FileToExport],
    schema: Schema,
    spec: PartitionSpec,
    properties: Properties,
    io: FileIO,
    max_workers: int = 8,
) -> list[DataFile]:
    """Parallel version, preserving the order of ``files`` (bounded workers)."""
    if not files:
        return []
    if max_workers <= 1 or len(files) == 1:
        return [build_data_file(f, schema, spec, properties, io) for f in files]
    workers = min(max_workers, len(files))
    with ContextThreadPoolExecutor(max_workers=workers) as pool:
        return list(
            pool.map(
                lambda f: build_data_file(f, schema, spec, properties, io),
                files,
            )
        )
