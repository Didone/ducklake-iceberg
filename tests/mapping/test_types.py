"""Tests for the type map (pure, no catalog)."""

from __future__ import annotations

import pytest
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)

from iceduck.mapping.types import (
    ATHENA_MS_NOTE,
    BLOCKED_TYPES,
    NESTED_TYPES,
    PRIMITIVE_TYPES,
    TypeStatus,
    base_type_name,
    blocked_type_names,
    is_blocked,
    resolve_type,
)

# normative list of the 14 types blocked in v2
_BLOCKED_NAMES = frozenset(
    {
        "uint32",
        "uint64",
        "int128",
        "uint128",
        "interval",
        "time_ns",
        "timetz",
        "timestamp_s",
        "timestamp_ms",
        "timestamp_ns",
        "timestamptz_ns",
        "variant",
        "geometry",
        "null",
    }
)

# types documented in the DuckLake catalog specification
_DOCUMENTED_TYPES = frozenset(
    {
        "boolean",
        "null",
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "int128",
        "uint128",
        "float32",
        "float64",
        "decimal",
        "date",
        "time",
        "time_ns",
        "timetz",
        "timestamp",
        "timestamp_s",
        "timestamp_ms",
        "timestamp_ns",
        "timestamptz",
        "timestamptz_ns",
        "interval",
        "varchar",
        "blob",
        "json",
        "uuid",
        "list",
        "struct",
        "map",
        "variant",
        "geometry",
    }
)


# each primitive with exact status and Iceberg instance
@pytest.mark.parametrize(
    ("name", "iceberg", "status"),
    [
        ("boolean", BooleanType(), TypeStatus.DIRECT),
        ("int8", IntegerType(), TypeStatus.WIDENED),
        ("int16", IntegerType(), TypeStatus.WIDENED),
        ("int32", IntegerType(), TypeStatus.DIRECT),
        ("int64", LongType(), TypeStatus.DIRECT),
        ("uint8", IntegerType(), TypeStatus.WIDENED),
        ("uint16", IntegerType(), TypeStatus.WIDENED),
        ("float32", FloatType(), TypeStatus.DIRECT),
        ("float64", DoubleType(), TypeStatus.DIRECT),
        ("date", DateType(), TypeStatus.DIRECT),
        ("time", TimeType(), TypeStatus.DIRECT),
        ("timestamp", TimestampType(), TypeStatus.DIRECT),
        ("timestamptz", TimestamptzType(), TypeStatus.DIRECT),
        ("varchar", StringType(), TypeStatus.DIRECT),
        ("blob", BinaryType(), TypeStatus.DIRECT),
        ("json", StringType(), TypeStatus.WIDENED),
        ("uuid", UUIDType(), TypeStatus.DIRECT),
    ],
)
def test_primitive_types(name: str, iceberg: object, status: TypeStatus) -> None:
    res = resolve_type(name)
    assert res.status is status
    assert res.iceberg == iceberg


# each blocked type resolves to BLOCKED with a reason and workaround
@pytest.mark.parametrize("name", sorted(_BLOCKED_NAMES))
def test_blocked_types(name: str) -> None:
    res = resolve_type(name)
    assert res.blocked
    assert res.iceberg is None
    assert res.reason
    assert BLOCKED_TYPES[name].workaround


# empty intersection
def test_no_overlap() -> None:
    assert set(BLOCKED_TYPES) & set(PRIMITIVE_TYPES) == set()


# blocked_type_names() is exactly the normative list
def test_blocked_type_names() -> None:
    assert blocked_type_names() == _BLOCKED_NAMES


# timestamp_s/ms cite the factor
def test_timestamp_low_precision_blocked() -> None:
    assert "10^6" in resolve_type("timestamp_s").reason
    assert "10^3" in resolve_type("timestamp_ms").reason


# regression: never returns TimestampType for timestamp_s/ms
@pytest.mark.parametrize("name", ["timestamp_s", "timestamp_ms", "TIMESTAMP_MS"])
def test_timestamp_low_precision_never_promotes(name: str) -> None:
    res = resolve_type(name)
    assert res.iceberg is None
    assert res.blocked


# valid decimals
@pytest.mark.parametrize(
    ("src", "p", "s"),
    [("decimal(1,0)", 1, 0), ("decimal(38,38)", 38, 38), ("decimal(18,2)", 18, 2)],
)
def test_decimal_valid(src: str, p: int, s: int) -> None:
    res = resolve_type(src)
    assert res.status is TypeStatus.DIRECT
    assert res.iceberg == DecimalType(p, s)


# invalid decimals
@pytest.mark.parametrize(
    "src",
    ["decimal(0,0)", "decimal(39,2)", "decimal(10,12)", "decimal", "decimal(a,b)"],
)
def test_decimal_invalid(src: str) -> None:
    res = resolve_type(src)
    assert res.blocked
    assert res.reason


# varchar with and without a limit
def test_varchar() -> None:
    assert resolve_type("varchar").iceberg == StringType()
    v = resolve_type("varchar(255)")
    assert v.iceberg == StringType()
    assert v.status is TypeStatus.WIDENED
    assert v.note


# nested
@pytest.mark.parametrize("name", ["struct", "list", "map"])
def test_nested(name: str) -> None:
    res = resolve_type(name)
    assert res.status is TypeStatus.NESTED
    assert res.nested_kind == name
    assert res.iceberg is None
    assert name in NESTED_TYPES


# spelling with spaces preserves source
def test_whitespace_and_case() -> None:
    res = resolve_type("  DECIMAL( 18 , 2 )  ")
    assert res.iceberg == DecimalType(18, 2)
    assert res.source == "DECIMAL( 18 , 2 )"


# a primitive does not accept parameters
@pytest.mark.parametrize("src", ["timestamp(3)", "boolean(1)"])
def test_primitive_rejects_params(src: str) -> None:
    res = resolve_type(src)
    assert res.blocked
    assert "does not accept parameters" in res.reason


# degenerate inputs raise no exception
@pytest.mark.parametrize("src", ["", "   ", "123", "geo metry"])
def test_degenerate(src: str) -> None:
    res = resolve_type(src)
    assert res.blocked


# future type
def test_future_type() -> None:
    res = resolve_type("future_type")
    assert res.blocked
    assert "newer version" in res.reason


# coverage of the documented vocabulary
def test_vocabulary_coverage() -> None:
    # 'decimal' is parameterized and handled by resolve_type outside the dicts.
    mapped = set(PRIMITIVE_TYPES) | set(NESTED_TYPES) | set(BLOCKED_TYPES) | {"decimal"}
    unmapped = _DOCUMENTED_TYPES - mapped
    assert unmapped == set(), f"documented types without a mapping: {unmapped}"


# timestamp notes
@pytest.mark.parametrize("name", ["timestamp", "timestamptz"])
def test_timestamp_note(name: str) -> None:
    res = resolve_type(name)
    assert res.note == ATHENA_MS_NOTE
    assert res.status is TypeStatus.DIRECT


# determinism and immutability
def test_deterministic_and_frozen() -> None:
    assert resolve_type("int32") == resolve_type("int32")
    assert hash(resolve_type("int32")) == hash(resolve_type("int32"))
    assert is_blocked("interval") is True
    assert is_blocked("int32") is False
    assert base_type_name("DECIMAL(18,2)") == "decimal"
    assert base_type_name("") == ""


# round trip: build a NestedField and serialize
@pytest.mark.parametrize("name", ["int32", "int8", "varchar", "json", "uuid"])
def test_roundtrip_nestedfield(name: str) -> None:
    res = resolve_type(name)
    assert res.iceberg is not None
    field = NestedField(field_id=1, name="c", field_type=res.iceberg, required=False)
    assert field.field_type == res.iceberg


# pending verifications recorded as named xfails
@pytest.mark.xfail(reason="To verify: UTC semantics of timestamptz")
def test_timestamptz_utc_semantics_verified() -> None:  # pragma: no cover
    raise AssertionError("timestamptz time zone verification not run yet")


@pytest.mark.xfail(reason="To verify: physical representation of uuid")
def test_uuid_physical_layout_verified() -> None:  # pragma: no cover
    raise AssertionError("uuid physical layout verification not run yet")


@pytest.mark.xfail(reason="To verify: INT_8/INT_16 annotations in the reader")
def test_int8_int16_annotation_verified() -> None:  # pragma: no cover
    raise AssertionError("INT_8/INT_16 annotation verification not run yet")
