"""Conversion of textual DuckLake catalog min/max into Iceberg bounds.

Any input without an exact conversion returns ``None`` (no bound): losing
pruning is acceptable, a wrong bound is not -- the reader would skip files
that contain matching rows.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from pyiceberg.conversions import from_bytes
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    PrimitiveType,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)

from ducklake_iceberg.writer.catalog_stats import STRING_TRUNCATE, column_bounds


def _decoded(itype: PrimitiveType, low: str, high: str) -> tuple[object, object]:
    bounds = column_bounds(itype, low, high)
    assert bounds is not None
    return from_bytes(itype, bounds[0]), from_bytes(itype, bounds[1])


@pytest.mark.parametrize(
    ("itype", "low", "high", "expected"),
    [
        (IntegerType(), "-128", "127", (-128, 127)),
        (LongType(), "-1", "9223372036854775807", (-1, 2**63 - 1)),
        (BooleanType(), "0", "1", (False, True)),
        (BooleanType(), "false", "true", (False, True)),
        (DateType(), "1970-01-01", "1970-01-11", (0, 10)),
        (TimeType(), "00:00:00", "00:00:01.000002", (0, 1_000_002)),
        (
            TimestampType(),
            "1970-01-01 00:00:00",
            "1970-01-01 00:00:01.5",
            (0, 1_500_000),
        ),
        (
            TimestamptzType(),
            "1970-01-01 00:00:00+00",
            "1970-01-01 01:00:00+01",
            (0, 0),
        ),
        (
            DecimalType(38, 10),
            "-0.0000000001",
            "12345678901234567890.0123456789",
            (Decimal("-0.0000000001"), Decimal("12345678901234567890.0123456789")),
        ),
        (DecimalType(9, 2), "-1.25", "99.9", (Decimal("-1.25"), Decimal("99.90"))),
        (StringType(), "a", "a\u00e7\u00e3o", ("a", "a\u00e7\u00e3o")),
    ],
)
def test_exact_conversions(
    itype: PrimitiveType, low: str, high: str, expected: tuple[object, object]
) -> None:
    assert _decoded(itype, low, high) == expected


@pytest.mark.parametrize(
    ("itype", "low", "high"),
    [
        (IntegerType(), "0", "2147483648"),  # outside int32
        (DateType(), "-infinity", "infinity"),
        (TimestampType(), "2024-01-01 00:00:00", "2024-01-01 00:00:00.123456789"),
        (TimeType(), "00:00:00", "00:00:00.0000001"),
        (TimestampType(), "2024-01-01 00:00:00+00", "2024-01-02 00:00:00+00"),
        (TimestamptzType(), "2024-01-01 00:00:00", "2024-01-02 00:00:00"),
        (DecimalType(9, 2), "1.234", "2.00"),  # scale larger than the type's
        (DecimalType(4, 2), "0.00", "123.45"),  # digits beyond the precision
        (DecimalType(9, 2), "NaN", "1.00"),
        (BooleanType(), "0", "maybe"),
        (FloatType(), "1.5", "2.5"),
        (DoubleType(), "0.0", "inf"),
        (
            UUIDType(),
            "00000000-0000-0000-0000-000000000001",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ),
        (BinaryType(), "00FF", "01"),
        (StringType(), "a", "\U0010ffff" * (STRING_TRUNCATE + 1)),
    ],
)
def test_inexact_or_unsupported_has_no_bounds(
    itype: PrimitiveType, low: str, high: str
) -> None:
    assert column_bounds(itype, low, high) is None


@pytest.mark.parametrize("missing", [(None, "1"), ("1", None)])
def test_missing_stat_has_no_bounds(missing: tuple[str | None, str | None]) -> None:
    assert column_bounds(IntegerType(), *missing) is None


def test_truncated_catalog_max_still_bounds_the_real_max() -> None:
    """The catalog stores the string max truncated at 256: a prefix of the real one."""
    real_max = "z" * 300 + "\u00e7"
    catalog_max = real_max[:256]
    real_min = "b" * 40
    low, high = _decoded(StringType(), real_min[:256], catalog_max)
    assert isinstance(low, str)
    assert isinstance(high, str)
    assert low <= real_min
    assert real_max < high
    assert len(high) == STRING_TRUNCATE
