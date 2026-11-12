"""DuckLake -> Iceberg v2 partition mapping.

Converts ``ducklake_partition_info`` and ``ducklake_partition_column`` into
Iceberg v2 ``PartitionSpec`` objects - including the **historical** specs,
because old manifests reference them - assigning deterministic synthetic field
IDs starting at 1000, mapping only the transforms whose semantics are identical
in both formats and downgrading the others to *unpartitioned* with a
degradation warning; and materializes the partition value of each live file as
a typed ``pyiceberg.typedef.Record``, ready for ``DataFile.partition``.

Two critical points of correctness, not performance:

- **Field ID stability**: the 1000+ ordinal is computed over **all** rows of
  ``ducklake_partition_column`` of the table, including those of degraded
  specs, ordered by ``(partition_id, partition_key_index)``; a field with the
  same source column and transform as an earlier one reuses its ID.
  Instability here breaks manifest reuse in incremental syncs and can make the
  reader prune by the wrong field, silently.
- **``epoch_*`` versus calendar**: DuckLake's ``year``/``month``/``day``/
  ``hour`` are calendar components, **not** units since the Unix epoch.
  Declaring them as the Iceberg transforms of the same name would produce
  incorrect pruning without any error. That is why they are degraded, never
  mapped.

``build_spec_mapping``, ``build_partition_record`` and ``map_transform`` are
pure and do not access the database; ``read_partition_specs`` and
``read_partition_values`` do the I/O.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Final, cast
from uuid import UUID

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    IdentityTransform,
    Transform,
)
from pyiceberg.typedef import Record
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    IcebergType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
    UUIDType,
)

from ducklake_iceberg.catalog.query import query_arrow
from ducklake_iceberg.doctor.model import Finding, Severity, quote_ident
from ducklake_iceberg.mapping.transforms import CALENDAR_TRANSFORMS
from ducklake_iceberg.mapping.transforms import PartitionSpecError as PartitionSpecError
from ducklake_iceberg.mapping.transforms import map_transform as map_transform

if TYPE_CHECKING:
    import duckdb
    from pyiceberg.io.pyarrow import DataFileStatistics

PARTITION_FIELD_ID_START: Final[int] = 1000
UNPARTITIONED_SPEC_ID: Final[int] = 0

#: Stable id of this module's Findings.
_RULE_ID: Final[str] = "partition"

#: days from 0001-01-01 (proleptic) to 1970-01-01, to convert date.toordinal.
_EPOCH_ORDINAL: Final[int] = date(1970, 1, 1).toordinal()


@dataclass(frozen=True, slots=True)
class DuckLakePartitionColumn:
    partition_id: int
    partition_key_index: int
    column_id: int
    transform: str
    #: name of the source column when the spec was created; ``None`` falls back
    #: to the synthetic name ``c<column_id>_<transform>``.
    column_name: str | None = None


@dataclass(frozen=True, slots=True)
class DuckLakePartitionInfo:
    partition_id: int
    begin_snapshot: int
    end_snapshot: int | None
    columns: tuple[DuckLakePartitionColumn, ...]


@dataclass(frozen=True, slots=True)
class SpecMapping:
    specs: Mapping[int, PartitionSpec]  # spec_id -> spec, including the one with id 0
    default_spec_id: int  # spec in effect at the target snapshot
    last_partition_id: int  # 999 if no ID was assigned
    spec_id_by_partition_id: Mapping[int, int]
    degraded: tuple[Finding, ...]  # DEGRADED severity
    field_ids: Mapping[tuple[int, int], int]  # (partition_id, key_index) -> id


class PartitionValueError(Exception):
    """partition_value missing, out of range or undecodable."""

    exit_code = 3


# -- catalog reads (I/O) ---------------------------------------------

_READ_SPECS_SQL: Final[str] = """\
SELECT
    pi.partition_id,
    pi.begin_snapshot,
    pi.end_snapshot,
    pc.partition_key_index,
    pc.column_id,
    pc."transform",
    c.column_name
FROM {info} AS pi
JOIN {column} AS pc
  ON pc.partition_id = pi.partition_id
 AND pc.table_id     = pi.table_id
LEFT JOIN {columns} AS c
  ON c.table_id  = pi.table_id
 AND c.column_id = pc.column_id
 AND c.begin_snapshot <= pi.begin_snapshot
 AND (c.end_snapshot IS NULL OR pi.begin_snapshot < c.end_snapshot)
WHERE pi.table_id = $table_id
ORDER BY pi.partition_id, pc.partition_key_index"""

_READ_VALUES_SQL: Final[str] = """\
SELECT
    fpv.data_file_id,
    fpv.partition_key_index,
    fpv.partition_value
FROM {fpv} AS fpv
WHERE fpv.table_id = $table_id
ORDER BY fpv.data_file_id, fpv.partition_key_index"""


def _rows(table: Any) -> list[dict[str, object]]:
    return cast("list[dict[str, object]]", table.to_pylist())


def _as_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    return int(str(value))


def _opt_int(value: object) -> int | None:
    return None if value is None else _as_int(value)


def _opt_str(value: object) -> str | None:
    return None if value is None else str(value)


def read_partition_specs(
    con: duckdb.DuckDBPyConnection, meta: str, table_id: int
) -> list[DuckLakePartitionInfo]:
    """All specs of the table, current and historical."""
    q = quote_ident(meta)
    sql = _READ_SPECS_SQL.format(
        info=f"{q}.ducklake_partition_info",
        column=f"{q}.ducklake_partition_column",
        columns=f"{q}.ducklake_column",
    )
    table = query_arrow(con, sql, {"table_id": table_id})
    grouped: dict[int, list[DuckLakePartitionColumn]] = {}
    bounds: dict[int, tuple[int, int | None]] = {}
    order: list[int] = []
    for row in _rows(table):
        pid = _as_int(row["partition_id"])
        if pid not in grouped:
            grouped[pid] = []
            bounds[pid] = (
                _as_int(row["begin_snapshot"]),
                _opt_int(row["end_snapshot"]),
            )
            order.append(pid)
        grouped[pid].append(
            DuckLakePartitionColumn(
                partition_id=pid,
                partition_key_index=_as_int(row["partition_key_index"]),
                column_id=_as_int(row["column_id"]),
                transform=str(row["transform"]),
                column_name=_opt_str(row.get("column_name")),
            )
        )
    result: list[DuckLakePartitionInfo] = []
    for pid in order:
        begin, end = bounds[pid]
        cols = tuple(sorted(grouped[pid], key=lambda c: c.partition_key_index))
        result.append(
            DuckLakePartitionInfo(
                partition_id=pid,
                begin_snapshot=begin,
                end_snapshot=end,
                columns=cols,
            )
        )
    return result


def read_partition_values(
    con: duckdb.DuckDBPyConnection, meta: str, table_id: int
) -> dict[int, dict[int, str | None]]:
    """``data_file_id -> {partition_key_index: partition_value}``.

    ``ducklake_file_partition_value`` is **not** versioned by snapshot; the
    temporal filtering happens in the join with the live ``data_file_id``
    values.
    """
    q = quote_ident(meta)
    sql = _READ_VALUES_SQL.format(fpv=f"{q}.ducklake_file_partition_value")
    table = query_arrow(con, sql, {"table_id": table_id})
    out: dict[int, dict[int, str | None]] = {}
    for row in _rows(table):
        data_file_id = _as_int(row["data_file_id"])
        key_index = _as_int(row["partition_key_index"])
        raw = row["partition_value"]
        value = None if raw is None else str(raw)
        out.setdefault(data_file_id, {})[key_index] = value
    return out


# -- spec construction (pure) -------------------------------------------


def _resolve_default_spec_id(
    infos: Sequence[DuckLakePartitionInfo],
    snapshot_id: int,
    spec_id_by_partition_id: Mapping[int, int],
) -> int:
    """Spec in effect at the target snapshot, by the catalog's half-open interval."""
    live = [
        info
        for info in infos
        if info.begin_snapshot <= snapshot_id
        and (info.end_snapshot is None or snapshot_id < info.end_snapshot)
    ]
    if len(live) > 1:
        raise PartitionSpecError(
            f"inconsistent catalog: {len(live)} specs in effect at "
            f"snapshot {snapshot_id}"
        )
    if not live:
        return UNPARTITIONED_SPEC_ID
    return spec_id_by_partition_id.get(live[0].partition_id, UNPARTITIONED_SPEC_ID)


def build_spec_mapping(
    infos: Sequence[DuckLakePartitionInfo],
    schema: Schema,
    *,
    snapshot_id: int,
    partial_degrade: bool = False,
    historical_schemas: Sequence[Schema] = (),
) -> SpecMapping:
    """Pure. Allocates spec-ids and field IDs; does not access the database."""
    field_ids = _allocate_field_ids(infos)
    last_partition_id = (
        max(field_ids.values()) if field_ids else PARTITION_FIELD_ID_START - 1
    )

    schema_field_ids = {f.field_id for f in schema.columns}
    column_names = {
        fid: schema.find_column_name(fid) or str(fid) for fid in schema_field_ids
    }
    # full schema names -> field id, for the collision rule
    schema_names = {name: fid for fid, name in column_names.items()}
    specs: dict[int, PartitionSpec] = {
        UNPARTITIONED_SPEC_ID: PartitionSpec(spec_id=UNPARTITIONED_SPEC_ID)
    }
    spec_id_by_partition_id: dict[int, int] = {}
    degraded: list[Finding] = []

    for info in infos:
        sources = {c.column_id for c in info.columns}
        available = next(
            (s for s in (schema, *historical_schemas) if sources <= s.field_ids),
            schema,
        )
        spec, findings = _build_one_spec(
            info,
            set(available.field_ids),
            field_ids,
            partial_degrade=partial_degrade,
            column_names=column_names,
            schema_names=schema_names,
        )
        degraded.extend(findings)
        if spec is None:
            # fully degraded spec -> collapses into the empty spec
            spec_id_by_partition_id[info.partition_id] = UNPARTITIONED_SPEC_ID
        else:
            spec_id_by_partition_id[info.partition_id] = info.partition_id
            specs[info.partition_id] = spec

    default_spec_id = _resolve_default_spec_id(
        infos, snapshot_id, spec_id_by_partition_id
    )
    return SpecMapping(
        specs=specs,
        default_spec_id=default_spec_id,
        last_partition_id=last_partition_id,
        spec_id_by_partition_id=spec_id_by_partition_id,
        degraded=tuple(degraded),
        field_ids=field_ids,
    )


def _allocate_field_ids(
    infos: Sequence[DuckLakePartitionInfo],
) -> dict[tuple[int, int], int]:
    """``1000 + ordinal`` over ALL rows, including degraded ones.

    The ordinal is deterministic: ``(partition_id, partition_key_index)`` in
    ascending order, regardless of the read order. A partition field with the
    same source column and transform as an earlier one (the same ``region``
    identity kept across a partition evolution) REUSES its field ID, as the
    Iceberg specification recommends: with distinct IDs for the same field,
    engines that union the partition types of every spec end up with duplicate
    field names.
    """
    keyed: list[tuple[tuple[int, int], tuple[int, str]]] = []
    for info in infos:
        for col in info.columns:
            keyed.append(
                (
                    (info.partition_id, col.partition_key_index),
                    (col.column_id, col.transform),
                )
            )
    keyed.sort(key=lambda item: item[0])
    by_field: dict[tuple[int, str], int] = {}
    allocated: dict[tuple[int, int], int] = {}
    for key, field in keyed:
        if field not in by_field:
            by_field[field] = PARTITION_FIELD_ID_START + len(by_field)
        allocated[key] = by_field[field]
    return allocated


def _build_one_spec(
    info: DuckLakePartitionInfo,
    schema_field_ids: set[int],
    field_ids: Mapping[tuple[int, int], int],
    *,
    partial_degrade: bool,
    column_names: Mapping[int, str] | None = None,
    schema_names: Mapping[str, int] | None = None,
) -> tuple[PartitionSpec | None, list[Finding]]:
    """Build one spec or return ``None`` (total degradation)."""
    findings: list[Finding] = []
    names = column_names or {}
    taken: set[str] = set()
    if not info.columns:
        findings.append(_degraded_finding(info.partition_id, "(empty)", "(empty)"))
        return None, findings

    mapped: list[PartitionField] = []
    any_degraded = False
    for col in info.columns:
        transform = map_transform(col.transform)
        if transform is None:
            any_degraded = True
            findings.append(
                _degraded_finding(
                    info.partition_id,
                    col.transform,
                    names.get(col.column_id, str(col.column_id)),
                    column_id=col.column_id,
                )
            )
            continue
        if col.column_id not in schema_field_ids:
            raise PartitionSpecError(
                f"spec {info.partition_id} references column_id={col.column_id}, "
                f"which is missing from the schema"
            )
        field_id = field_ids[(info.partition_id, col.partition_key_index)]
        name = _field_name(col, transform, schema_names or {}, taken)
        taken.add(name)
        mapped.append(
            PartitionField(
                source_id=col.column_id,
                field_id=field_id,
                transform=transform,
                name=name,
            )
        )

    if not mapped:
        return None, findings  # everything degraded -> empty spec
    if any_degraded and not partial_degrade:
        # partial degradation not enabled -> degrade the whole spec
        return None, findings

    return PartitionSpec(*mapped, spec_id=info.partition_id), findings


def _synthetic_name(
    col: DuckLakePartitionColumn, transform: Transform[Any, Any]
) -> str:
    """Old synthetic name, ``c<column_id>_<transform>`` (the fallback)."""
    # bucket[16] -> bucket, day -> day
    short = str(transform).split("[", 1)[0]
    return f"c{col.column_id}_{short}"


def _standard_name(source: str, transform: Transform[Any, Any]) -> str:
    """Iceberg/PyIceberg convention (``_PartitionNameGenerator``)."""
    if isinstance(transform, IdentityTransform):
        return source
    if isinstance(transform, BucketTransform):
        return f"{source}_bucket_{transform.num_buckets}"
    return f"{source}_{str(transform).split('[', 1)[0]}"


def _field_name(
    col: DuckLakePartitionColumn,
    transform: Transform[Any, Any],
    schema_names: Mapping[str, int],
    taken: set[str],
) -> str:
    """Name of the partition field following the Iceberg convention.

    ``<column at spec creation>`` for identity, ``<column>_<transform>`` for
    the others (``created_at_day``, ``id_bucket_16``). The partition name must
    not collide with a schema field - except identity on its own source
    column - nor repeat within the spec; in those cases, and when the column
    name at creation is unknown, it falls back to ``c<column_id>_<transform>``.
    Already published specs keep the name they had (``with_published_names``).
    """
    fallback = _synthetic_name(col, transform)
    if col.column_name is None:
        return fallback
    candidate = _standard_name(col.column_name, transform)
    owner = schema_names.get(candidate)
    identity_on_source = (
        isinstance(transform, IdentityTransform) and owner == col.column_id
    )
    if (owner is not None and not identity_on_source) or candidate in taken:
        return fallback if fallback not in taken else f"{fallback}_{col.partition_id}"
    return candidate


def with_published_names(
    mapping: SpecMapping, published: Mapping[int, PartitionSpec]
) -> SpecMapping:
    """Keep the field names of already published specs.

    A published spec is the same when each field has the same ``field_id``,
    ``source_id`` and transform; then the published name wins, so that the
    incremental export does not rename the field that ``$partitions`` and the
    engines already know. New specs use the standard name.
    """
    specs: dict[int, PartitionSpec] = {}
    changed = False
    for spec_id, spec in mapping.specs.items():
        old = published.get(spec_id)
        if old is None or not spec.fields or not _same_fields(spec, old):
            specs[spec_id] = spec
            continue
        old_names = {f.field_id: f.name for f in old.fields}
        fields = [
            PartitionField(
                source_id=f.source_id,
                field_id=f.field_id,
                transform=f.transform,
                name=old_names[f.field_id],
            )
            for f in spec.fields
        ]
        changed = changed or any(
            f.name != g.name for f, g in zip(fields, spec.fields, strict=True)
        )
        specs[spec_id] = PartitionSpec(*fields, spec_id=spec_id)
    if not changed:
        return mapping
    return SpecMapping(
        specs=specs,
        default_spec_id=mapping.default_spec_id,
        last_partition_id=mapping.last_partition_id,
        spec_id_by_partition_id=mapping.spec_id_by_partition_id,
        degraded=mapping.degraded,
        field_ids=mapping.field_ids,
    )


def _same_fields(new: PartitionSpec, old: PartitionSpec) -> bool:
    if len(new.fields) != len(old.fields):
        return False
    return all(
        (a.field_id, a.source_id, str(a.transform))
        == (b.field_id, b.source_id, str(b.transform))
        for a, b in zip(new.fields, old.fields, strict=True)
    )


def _degraded_finding(
    partition_id: int, transform: str, column: str, *, column_id: int | None = None
) -> Finding:
    unit = transform.strip().lower()
    if unit in CALENDAR_TRANSFORMS:
        remediation = (
            f"in DuckLake, replace {unit}({column}) with epoch_{unit}({column}) in "
            f"ALTER TABLE ... SET PARTITIONED BY (...): exact equivalence"
        )
    else:
        remediation = (
            "ALTER TABLE ... SET PARTITIONED BY (epoch_<unit>(<column>)) in "
            "DuckLake for exact equivalence"
        )
    return Finding(
        rule_id=_RULE_ID,
        severity=Severity.DEGRADED,
        subject=f"spec {partition_id}",
        detail=(
            f"spec {partition_id}: transform {transform!r} on column {column} "
            f"has no Iceberg equivalent; table exported without this "
            f"partition key"
        ),
        remediation=remediation,
        data={
            "partition_id": partition_id,
            "transform": transform,
            "column": column,
            "column_id": column_id,
        },
    )


# -- partition values (pure) --------------------------------------------


def build_partition_record(
    spec: PartitionSpec,
    schema: Schema,
    raw: Mapping[int, str | None],
) -> Record:
    """Materialize ``DataFile.partition`` as a typed ``Record``.

    ``raw`` is indexed by the synthetic ``field_id`` (1000+) of the partition
    field - the caller translates ``partition_key_index -> field_id`` via
    ``SpecMapping.field_ids``.
    """
    struct = spec.partition_type(schema)
    values: list[object] = []
    for field, nested in zip(spec.fields, struct.fields, strict=True):
        text = raw.get(field.field_id)
        if text is None:
            values.append(None)
            continue
        value = _decode_single_value(nested.field_type, text, field.field_id)
        _check_bucket_range(field.transform, value, field.field_id)
        values.append(value)
    return Record(*values)


def _check_bucket_range(
    transform: Transform[Any, Any], value: object, field_id: int
) -> None:
    if isinstance(transform, BucketTransform) and isinstance(value, int):
        n = transform.num_buckets
        if not 0 <= value < n:
            raise PartitionValueError(
                f"field {field_id}: bucket={value} outside range [0,{n})"
            )


def _decode_single_value(field_type: IcebergType, text: str, field_id: int) -> object:
    """Convert the catalog text into the type's internal representation."""
    try:
        return _decode_by_type(field_type, text)
    except (ValueError, TypeError, InvalidOperation) as exc:
        raise PartitionValueError(
            f"field {field_id}: {text!r} does not decode to {field_type}: {exc}"
        ) from exc


def _decode_by_type(field_type: IcebergType, text: str) -> object:
    if isinstance(field_type, (IntegerType, LongType)):
        return int(text)
    if isinstance(field_type, DateType):
        return _decode_date(text)
    if isinstance(field_type, (TimestampType, TimestamptzType)):
        return _decode_timestamp(text)
    if isinstance(field_type, StringType):
        return text
    if isinstance(field_type, DecimalType):
        return Decimal(text)
    if isinstance(field_type, BooleanType):
        return _decode_bool(text)
    if isinstance(field_type, UUIDType):
        return UUID(text)
    if isinstance(field_type, BinaryType):
        return bytes.fromhex(text)
    raise ValueError(f"unsupported partition type: {field_type}")


def _decode_date(text: str) -> int:
    """Days since 1970-01-01. Accepts an integer (epoch_day) or ISO (identity)."""
    stripped = text.strip()
    if _is_integer_literal(stripped):
        return int(stripped)
    return date.fromisoformat(stripped).toordinal() - _EPOCH_ORDINAL


def _decode_timestamp(text: str) -> int:
    """UTC microseconds. Accepts an integer (epoch) or ISO 8601 (identity)."""
    stripped = text.strip()
    if _is_integer_literal(stripped):
        return int(stripped)
    parsed = datetime.fromisoformat(stripped)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = parsed - epoch
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def _decode_bool(text: str) -> bool:
    lowered = text.strip().lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    raise ValueError(f"invalid boolean: {text!r}")


def _is_integer_literal(text: str) -> bool:
    candidate = text[1:] if text.startswith("-") else text
    return candidate.isdigit()


def partition_from_stats(
    stats: DataFileStatistics, spec: PartitionSpec, schema: Schema
) -> Record:
    """Fallback: derive the partition from the footer bounds."""
    return stats.partition(spec, schema)
