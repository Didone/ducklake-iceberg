"""DuckLake -> Iceberg v2 type map.

Pure module (no connection, I/O or state). It is the single source of truth
for type mapping, consumed by the preflight `unsupported-type` rule and by the
export's `Schema` builder.

Central decision: **rejecting is the default**. An unknown type resolves to
`BLOCKED`, never to a guess - a false block costs a message, a false accept
costs silently wrong data. In particular, `timestamp_s`/`timestamp_ms` are
blocked instead of being promoted to micros.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)


class TypeStatus(StrEnum):
    DIRECT = "direct"  # exact match, no loss
    WIDENED = "widened"  # the source domain fits entirely in the target
    NESTED = "nested"  # struct/list/map: resolved through the tree
    BLOCKED = "blocked"  # no safe equivalent in Iceberg v2


@dataclass(frozen=True, slots=True)
class TypeResolution:
    source: str  # original column_type, as in the catalog
    status: TypeStatus
    iceberg: IcebergType | None  # None when BLOCKED or NESTED
    nested_kind: str | None = None  # 'struct' | 'list' | 'map' when NESTED
    reason: str = ""  # why it was blocked; empty when not blocked
    note: str = ""  # non-blocking warning (INFO)

    @property
    def blocked(self) -> bool:
        return self.status is TypeStatus.BLOCKED


@dataclass(frozen=True, slots=True)
class BlockedType:
    reason: str  # single sentence, used in the user-facing message
    workaround: str  # suggested ALTER TABLE at the source


# -- non-blocking notes ------------------------------------------

ATHENA_MS_NOTE: Final[str] = (
    "Athena reads timestamps with millisecond precision; the microseconds "
    "written by DuckLake are truncated on read, not in the file"
)
JSON_NOTE: Final[str] = (
    "json is exported as string; the JSON logical annotation remains in "
    "Parquet and may be rejected by strict Iceberg readers"
)
UUID_NOTE: Final[str] = (
    "uuid requires 16 big-endian bytes in Parquet; the physical layout is "
    "not verified yet"
)


# -- supported type map ---------------------------------------

PRIMITIVE_TYPES: Final[dict[str, tuple[IcebergType, TypeStatus, str]]] = {
    "boolean": (BooleanType(), TypeStatus.DIRECT, ""),
    "int8": (IntegerType(), TypeStatus.WIDENED, "int8 -> int32"),
    "int16": (IntegerType(), TypeStatus.WIDENED, "int16 -> int32"),
    "int32": (IntegerType(), TypeStatus.DIRECT, ""),
    "int64": (LongType(), TypeStatus.DIRECT, ""),
    "uint8": (IntegerType(), TypeStatus.WIDENED, "uint8 -> int32"),
    "uint16": (IntegerType(), TypeStatus.WIDENED, "uint16 -> int32"),
    "float32": (FloatType(), TypeStatus.DIRECT, ""),
    "float64": (DoubleType(), TypeStatus.DIRECT, ""),
    "date": (DateType(), TypeStatus.DIRECT, ""),
    "time": (TimeType(), TypeStatus.DIRECT, ""),
    "timestamp": (TimestampType(), TypeStatus.DIRECT, ATHENA_MS_NOTE),
    "timestamptz": (TimestamptzType(), TypeStatus.DIRECT, ATHENA_MS_NOTE),
    "varchar": (StringType(), TypeStatus.DIRECT, ""),
    "blob": (BinaryType(), TypeStatus.DIRECT, ""),
    "json": (StringType(), TypeStatus.WIDENED, JSON_NOTE),
    "uuid": (UUIDType(), TypeStatus.DIRECT, UUID_NOTE),
}

NESTED_TYPES: Final[frozenset[str]] = frozenset({"struct", "list", "map"})


# -- types blocked in v2 -----------------------------------------

BLOCKED_TYPES: Final[dict[str, BlockedType]] = {
    "uint32": BlockedType(
        reason=(
            "physical unsigned INT32: values >= 2^31 would be read as "
            "negative by an Iceberg reader, without any error"
        ),
        workaround="ALTER TABLE ... ALTER COLUMN <c> TYPE BIGINT",
    ),
    "uint64": BlockedType(
        reason="no Iceberg type covers 0..2^64-1",
        workaround="DECIMAL(20,0) or BIGINT if the real domain fits",
    ),
    "int128": BlockedType(
        reason="Iceberg integers go up to 64 bits",
        workaround="DECIMAL(38,0)",
    ),
    "uint128": BlockedType(
        reason="Iceberg integers go up to 64 bits, and are signed",
        workaround="DECIMAL(38,0) if the domain fits",
    ),
    "interval": BlockedType(
        reason="Iceberg has no interval type in any version",
        workaround="split into numeric columns",
    ),
    "time_ns": BlockedType(
        reason="Iceberg only has time in microseconds",
        workaround="TIME (micros), assuming the loss is acceptable",
    ),
    "timetz": BlockedType(
        reason="Iceberg has no time with time zone",
        workaround="TIME + offset column",
    ),
    "timestamp_s": BlockedType(
        reason=("relabeling as timestamp (micros) would multiply every value by 10^6"),
        workaround="ALTER TABLE ... ALTER COLUMN <c> TYPE TIMESTAMP",
    ),
    "timestamp_ms": BlockedType(
        reason=("relabeling as timestamp (micros) would multiply every value by 10^3"),
        workaround="ALTER TABLE ... ALTER COLUMN <c> TYPE TIMESTAMP",
    ),
    "timestamp_ns": BlockedType(
        reason="timestamp_ns only exists in Iceberg v3; Athena does not read v3",
        workaround="ALTER TABLE ... ALTER COLUMN <c> TYPE TIMESTAMP",
    ),
    "timestamptz_ns": BlockedType(
        reason="timestamptz_ns only exists in Iceberg v3",
        workaround="ALTER TABLE ... ALTER COLUMN <c> TYPE TIMESTAMPTZ",
    ),
    "variant": BlockedType(
        reason="variant only exists in Iceberg v3",
        workaround="materialize subfields into typed columns, or VARCHAR with JSON",
    ),
    "geometry": BlockedType(
        reason="geometry/geography only exist in Iceberg v3",
        workaround="BLOB with WKB, losing the spatial semantics in the reader",
    ),
    "null": BlockedType(
        reason="equivalent to unknown, which only exists in Iceberg v3",
        workaround="type the column; null means a column with no inferred type",
    ),
}


_TYPE_RE: Final[re.Pattern[str]] = re.compile(
    r"\A(?P<base>[a-z][a-z0-9_]*)\s*(?:\(\s*(?P<args>[^()]*)\s*\))?\Z"
)
_MAX_PRECISION: Final[int] = 38


def base_type_name(column_type: str) -> str:
    """'DECIMAL(18, 2)' -> 'decimal'. Does not validate arguments."""
    match = _TYPE_RE.match(column_type.strip().lower())
    return match.group("base") if match else ""


def resolve_type(column_type: str) -> TypeResolution:
    raw = column_type.strip()
    match = _TYPE_RE.match(raw.lower())
    if match is None:
        return _blocked(raw, f"unrecognized type: {raw!r}")

    base = match.group("base")
    args = match.group("args")

    if base in BLOCKED_TYPES:
        blocked = BLOCKED_TYPES[base]
        return TypeResolution(
            source=raw,
            status=TypeStatus.BLOCKED,
            iceberg=None,
            reason=blocked.reason,
        )

    if base in NESTED_TYPES:
        return TypeResolution(
            source=raw,
            status=TypeStatus.NESTED,
            iceberg=None,
            nested_kind=base,
        )

    if base == "decimal":
        return _resolve_decimal(raw, args)

    if base == "varchar" and args is not None:
        return TypeResolution(
            source=raw,
            status=TypeStatus.WIDENED,
            iceberg=StringType(),
            note=f"length limit ({args}) does not exist in Iceberg; dropped",
        )

    entry = PRIMITIVE_TYPES.get(base)
    if entry is None:
        return _blocked(
            raw,
            f"type {base!r} unknown to this tool; "
            f"it may come from a newer version of the DuckLake format",
        )
    if args is not None:
        return _blocked(raw, f"type {base!r} does not accept parameters")

    iceberg, status, note = entry
    return TypeResolution(source=raw, status=status, iceberg=iceberg, note=note)


def _resolve_decimal(raw: str, args: str | None) -> TypeResolution:
    if args is None:
        return _blocked(raw, "decimal requires explicit precision and scale")
    parts = [p.strip() for p in args.split(",")]
    if len(parts) != 2 or not all(p.isdigit() for p in parts):
        return _blocked(raw, f"unreadable decimal parameters: {args!r}")
    precision, scale = int(parts[0]), int(parts[1])
    if not 1 <= precision <= _MAX_PRECISION:
        return _blocked(
            raw,
            f"precision {precision} outside 1..{_MAX_PRECISION}; "
            f"Iceberg cannot represent wider decimals",
        )
    if not 0 <= scale <= precision:
        return _blocked(raw, f"scale {scale} invalid for precision {precision}")
    return TypeResolution(
        source=raw,
        status=TypeStatus.DIRECT,
        iceberg=DecimalType(precision, scale),
    )


def _blocked(raw: str, reason: str) -> TypeResolution:
    return TypeResolution(
        source=raw, status=TypeStatus.BLOCKED, iceberg=None, reason=reason
    )


def is_blocked(column_type: str) -> bool:
    return resolve_type(column_type).blocked


def blocked_type_names() -> frozenset[str]:
    """Consumed by the IN (...) clause of the unsupported-type rule."""
    return frozenset(BLOCKED_TYPES)
