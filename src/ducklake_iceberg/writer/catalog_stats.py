"""``stats_source: catalog`` -- ``DataFile`` built from the DuckLake catalog.

The default path reads ``ducklake_file_column_stats`` in the export's read
transaction, instead of a GET of the Parquet footer per new file on S3. The
catalog stores min/max as **text**, and conversion to the Iceberg binary form
is only done where it is exact (verified at runtime, DuckDB 2.0 / DuckLake 1.1):

| Iceberg type | Catalog text | Bound |
| --- | --- | --- |
| int / long | ``-128`` | yes |
| boolean | ``0`` / ``1`` | yes |
| date | ``1970-01-01`` | yes |
| time | ``12:34:56.789012`` | yes (up to us) |
| timestamp | ``2024-01-01 00:00:00.123456`` | yes (up to us) |
| timestamptz | ``2024-03-01 13:00:00.5+00`` | yes (up to us) |
| decimal(p, s) | ``-0.0000000001`` | yes, if exact at scale ``s`` |
| string | ``max`` **truncated at 256** chars | yes, truncated at 16 (below) |
| float / double | ``-0.0`` comes out ``0.0``; NaN not in min/max | **no** |
| uuid / binary / nested | hex, text, per leaf | **no** |

The string ``max`` truncated in the catalog is a prefix of the real maximum
and, on its own, is **not** a valid upper bound. Iceberg truncation to 16 is
applied (the same as ``truncate(16)``, the footer path default): a 16-char
prefix with the last code point incremented. Since 16 < 256, the result is
greater than the real maximum.

Counts: the catalog's ``value_count`` **excludes** nulls; Iceberg's
``value_counts`` includes them (= ``value_count + null_count``, checked
against ``record_count``). ``nan_value_counts`` does not exist in the catalog
(only ``contains_nan``) and is left empty, as in the footer path. A type or
value outside the table gets no bound: pruning is lost, correctness is kept.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Context, Decimal, InvalidOperation
from typing import TYPE_CHECKING, Final

from pyiceberg.conversions import to_bytes
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    PrimitiveType,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)
from pyiceberg.utils.truncate import truncate_upper_bound_text_string

from ducklake_iceberg.doctor.model import quote_ident

if TYPE_CHECKING:
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema

    from ducklake_iceberg.catalog.connection import DuckLakeConnection
    from ducklake_iceberg.writer.stats import FileToExport

#: string bounds truncation (``truncate(16)``, the PyIceberg default)
STRING_TRUNCATE: Final[int] = 16

_INT_MIN: Final[int] = -(2**31)
_INT_MAX: Final[int] = 2**31 - 1
_EPOCH: Final[datetime] = datetime(1970, 1, 1)
_EPOCH_UTC: Final[datetime] = datetime(1970, 1, 1, tzinfo=UTC)
_EPOCH_DATE: Final[date] = date(1970, 1, 1)
#: decimal context precision: comfortably covers DECIMAL(38, s)
_DECIMAL_PREC: Final[int] = 80
#: fractional seconds with more than 6 digits (ns) do not fit in us unrounded
_SUB_MICROS: Final[re.Pattern[str]] = re.compile(r"\.\d{7,}")


@dataclass(frozen=True, slots=True)
class ColumnStats:
    """One row of ``ducklake_file_column_stats``."""

    column_size_bytes: int | None
    value_count: int | None  # excludes nulls
    null_count: int | None
    min_value: str | None
    max_value: str | None
    contains_nan: bool | None


def read_file_column_stats(
    con: DuckLakeConnection, table_id: int, data_file_ids: Iterable[int]
) -> dict[int, dict[int, ColumnStats]]:
    """``data_file_id -> column_id -> ColumnStats`` for the requested files."""
    ids = sorted(set(data_file_ids))
    if not ids:
        return {}
    rows = con.query_arrow(
        "SELECT data_file_id, column_id, column_size_bytes, value_count, "
        "null_count, min_value, max_value, contains_nan "
        f"FROM {quote_ident(con.meta)}.ducklake_file_column_stats "
        "WHERE table_id = $table_id "
        "AND data_file_id IN (SELECT unnest($ids::BIGINT[]))",
        {"table_id": table_id, "ids": ids},
    ).to_pylist()
    out: dict[int, dict[int, ColumnStats]] = {}
    for r in rows:
        out.setdefault(int(r["data_file_id"]), {})[int(r["column_id"])] = ColumnStats(
            column_size_bytes=_opt_int(r["column_size_bytes"]),
            value_count=_opt_int(r["value_count"]),
            null_count=_opt_int(r["null_count"]),
            min_value=r["min_value"],
            max_value=r["max_value"],
            contains_nan=None if r["contains_nan"] is None else bool(r["contains_nan"]),
        )
    return out


def _opt_int(value: object) -> int | None:
    return None if value is None else int(str(value))


def build_data_file_from_catalog(
    file: FileToExport,
    schema: Schema,
    spec: PartitionSpec,
    stats: Mapping[int, ColumnStats],
) -> DataFile:
    """``DataFile`` with ``DATA`` content and the catalog's metrics.

    Only top-level columns with a primitive type get metrics: on nested
    leaves the catalog counts have different semantics (per element) and are
    left out, like bounds of types without an exact conversion. The result is
    a subset of the footer path (parity test).
    """
    column_sizes: dict[int, int] = {}
    value_counts: dict[int, int] = {}
    null_counts: dict[int, int] = {}
    lower: dict[int, bytes] = {}
    upper: dict[int, bytes] = {}
    for field in schema.fields:
        st = stats.get(field.field_id)
        if (
            st is None
            or not isinstance(field.field_type, PrimitiveType)
            or isinstance(
                field.field_type, UUIDType
            )  # the footer does not collect them
        ):
            continue
        fid = field.field_id
        if st.column_size_bytes is not None:
            column_sizes[fid] = st.column_size_bytes
        if st.null_count is not None:
            null_counts[fid] = st.null_count
            if (
                st.value_count is not None
                and st.value_count + st.null_count == file.record_count
            ):
                value_counts[fid] = st.value_count + st.null_count
        bounds = column_bounds(field.field_type, st.min_value, st.max_value)
        if bounds is not None:
            lower[fid], upper[fid] = bounds

    return DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=file.path,
        file_format=FileFormat.PARQUET,
        partition=file.partition_values,
        record_count=file.record_count,
        file_size_in_bytes=file.file_size_in_bytes,
        column_sizes=column_sizes,
        value_counts=value_counts,
        null_value_counts=null_counts,
        nan_value_counts={},
        lower_bounds=lower,
        upper_bounds=upper,
        split_offsets=None,
        spec_id=spec.spec_id,
        sort_order_id=None,
        equality_ids=None,
        key_metadata=None,
    )


def column_bounds(
    itype: PrimitiveType, min_text: str | None, max_text: str | None
) -> tuple[bytes, bytes] | None:
    """Serialized ``(lower, upper)``, or ``None`` if conversion is not exact."""
    if min_text is None or max_text is None:
        return None
    try:
        if isinstance(itype, StringType):
            return _string_bounds(min_text, max_text)
        low = _parse(itype, min_text)
        high = _parse(itype, max_text)
    except (ValueError, InvalidOperation, OverflowError):
        return None
    if low is None or high is None:
        return None
    return to_bytes(itype, low), to_bytes(itype, high)


def _string_bounds(min_text: str, max_text: str) -> tuple[bytes, bytes] | None:
    lower = min_text[:STRING_TRUNCATE]
    upper = (
        max_text
        if len(max_text) <= STRING_TRUNCATE
        else truncate_upper_bound_text_string(max_text, STRING_TRUNCATE)
    )
    if upper is None:  # only max code points: no finite upper bound
        return None
    return lower.encode("utf-8"), upper.encode("utf-8")


def _parse(itype: PrimitiveType, text: str) -> bool | int | Decimal | None:
    """Python value ``to_bytes`` accepts for ``itype``; ``None`` = no bound."""
    value = text.strip()
    if isinstance(itype, BooleanType):
        return {"0": False, "1": True, "false": False, "true": True}.get(value.lower())
    if isinstance(itype, IntegerType | LongType):
        number = int(value)
        if isinstance(itype, IntegerType) and not _INT_MIN <= number <= _INT_MAX:
            return None
        return number
    if isinstance(itype, DateType):
        return (date.fromisoformat(value) - _EPOCH_DATE).days
    if isinstance(itype, DecimalType):
        return _decimal(itype, value)
    if _SUB_MICROS.search(value):
        return None
    if isinstance(itype, TimeType):
        t = time.fromisoformat(value)
        if t.tzinfo is not None:
            return None
        return ((t.hour * 60 + t.minute) * 60 + t.second) * 1_000_000 + t.microsecond
    if isinstance(itype, TimestampType):
        dt = datetime.fromisoformat(value)
        return None if dt.tzinfo is not None else _micros(dt - _EPOCH)
    if isinstance(itype, TimestamptzType):
        dt = datetime.fromisoformat(value)
        return None if dt.tzinfo is None else _micros(dt - _EPOCH_UTC)
    return None


def _micros(delta: timedelta) -> int:
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def _decimal(itype: DecimalType, text: str) -> Decimal | None:
    value = Decimal(text)
    if not value.is_finite():
        return None
    # local context: the default (28 digits) cannot hold DECIMAL(38, s)
    quantized = value.quantize(
        Decimal(1).scaleb(-itype.scale), context=Context(prec=_DECIMAL_PREC)
    )
    if quantized != value or len(quantized.as_tuple().digits) > itype.precision:
        return None
    return quantized


__all__ = [
    "STRING_TRUNCATE",
    "ColumnStats",
    "build_data_file_from_catalog",
    "column_bounds",
    "read_file_column_stats",
]
