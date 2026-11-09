"""DuckLake -> Iceberg v2 schema mapping.

Converts the live rows of ``ducklake_column`` at the target snapshot into an
Iceberg **v2** ``pyiceberg.schema.Schema``, literally preserving
``field.id = ducklake_column.column_id``, rebuilding the ``struct``/``list``/
``map`` tree from ``parent_column``, filtering DuckLake internal columns and
aborting with exit code 2 on any type with no v2 equivalent.

The ``field_id`` is **not allocated** by this module: it is copied from
``column_id``. Any renumbering (which ``Catalog.create_table`` does) would
silently produce null columns in Iceberg readers - that is why this module
never calls ``create_table`` and the ``Schema`` built here goes straight to
``metadata.json``.

Field defaults (``initial-default``/``write-default``) are v3 and, by default
(``emit_defaults=False``), are **not** emitted in the v2 schema. The values
remain available in ``SchemaMapping.columns_by_id`` for schema evolution, but
do not reach ``metadata.json`` until it is verified that Athena's Iceberg
1.4.2 library accepts ``initial-default`` in ``format-version: 2``.

The split between I/O and pure logic is deliberate: ``read_columns`` and
``read_last_column_id`` touch the catalog; ``build_iceberg_schema`` is pure and
testable with literal lists of ``DuckLakeColumn``, without a database.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, cast

from pyiceberg.schema import Schema
from pyiceberg.types import (
    IcebergType,
    ListType,
    MapType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
)

from ducklake_iceberg.doctor.model import Finding, Severity, quote_ident
from ducklake_iceberg.mapping.types import (
    NESTED_TYPES,
    TypeStatus,
    base_type_name,
    resolve_type,
)

if TYPE_CHECKING:
    import duckdb

#: Reserved Iceberg field IDs that DuckLake physically writes into Parquet.
#: Currently absent from ``ducklake_column``; the filter is defensive.
_INTERNAL_IDS: Final[frozenset[int]] = frozenset({2147483540, 2147483539})

#: Conservative floor of the Iceberg reserved range. TO VERIFY: the exact
#: normative floor. Until confirmed, we abort instead of publishing IDs that
#: could collide with reserved fields (`file_path`, `pos`, ...).
_RESERVED_FLOOR: Final[int] = 2_147_483_000

#: Prefix reserved by the DuckLake 1.1 specification; only applies at the top
#: level.
_DUCKLAKE_PREFIX: Final[str] = "_ducklake_"

#: Stable id for this module's Findings.
_RULE_ID: Final[str] = "schema"


@dataclass(frozen=True, slots=True)
class DuckLakeColumn:
    """A live ``ducklake_column`` row at the target snapshot."""

    column_id: int
    parent_column: int | None
    column_order: int
    column_name: str
    column_type: str
    nulls_allowed: bool
    initial_default: str | None
    default_value: str | None
    default_value_type: str | None
    default_value_dialect: str | None
    begin_snapshot: int


@dataclass(frozen=True, slots=True)
class SchemaMapping:
    """Result of ``build_iceberg_schema``."""

    schema: Schema  # field.id == column_id, across the whole tree
    schema_id: int  # == ducklake_snapshot.schema_version
    last_column_id: int  # max(column_id) over the whole history
    columns_by_id: Mapping[int, DuckLakeColumn]
    paths_by_id: Mapping[int, str]  # 'items.element.price', for messages
    dropped: tuple[str, ...]  # filtered internal columns
    warnings: tuple[str, ...]


class BlockedTypeError(Exception):
    """At least one column has a type with no Iceberg v2 equivalent."""

    exit_code = 2

    def __init__(self, findings: Sequence[Finding]) -> None:
        self.findings = tuple(findings)
        detail = "; ".join(f.detail for f in self.findings) or "blocked type"
        super().__init__(detail)


class SchemaShapeError(Exception):
    """Inconsistent ``ducklake_column`` tree (cycle, orphan, arity)."""

    exit_code = 1


# -- catalog reads (I/O) ---------------------------------------------

_READ_COLUMNS_SQL: Final[str] = """\
SELECT
    c.column_id,
    c.parent_column,
    c.column_order,
    c.column_name,
    c.column_type,
    c.nulls_allowed,
    c.initial_default,
    c.default_value,
    c.default_value_type,
    c.default_value_dialect,
    c.begin_snapshot
FROM {column} AS c
WHERE c.table_id = $table_id
  AND $snapshot_id >= c.begin_snapshot
  AND (c.end_snapshot IS NULL OR $snapshot_id < c.end_snapshot)
ORDER BY c.parent_column NULLS FIRST, c.column_order, c.column_id"""

#: `last-column-id` deliberately ignores the snapshot: considering only the
#: current schema would allow reallocating an already retired ID.
_LAST_COLUMN_ID_SQL: Final[str] = """\
SELECT coalesce(max(c.column_id), 0) AS last_column_id
FROM {column} AS c
WHERE c.table_id = $table_id"""


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


def _row_to_column(row: dict[str, object]) -> DuckLakeColumn:
    nulls = row["nulls_allowed"]
    return DuckLakeColumn(
        column_id=_as_int(row["column_id"]),
        parent_column=_opt_int(row["parent_column"]),
        column_order=_as_int(row["column_order"]),
        column_name=str(row["column_name"]),
        column_type=str(row["column_type"]),
        nulls_allowed=True if nulls is None else bool(nulls),
        initial_default=_opt_str(row["initial_default"]),
        default_value=_opt_str(row["default_value"]),
        default_value_type=_opt_str(row["default_value_type"]),
        default_value_dialect=_opt_str(row["default_value_dialect"]),
        begin_snapshot=_as_int(row["begin_snapshot"]),
    )


def read_columns(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    table_id: int,
    snapshot_id: int,
) -> list[DuckLakeColumn]:
    """Live ``ducklake_column`` rows for ``(table_id, snapshot_id)``."""
    column = f"{quote_ident(meta)}.ducklake_column"
    sql = _READ_COLUMNS_SQL.format(column=column)
    params = {"table_id": table_id, "snapshot_id": snapshot_id}
    table = cast("Any", con).sql(sql, params=params).to_arrow_table()
    return [_row_to_column(row) for row in _rows(table)]


def read_last_column_id(
    con: duckdb.DuckDBPyConnection, meta: str, table_id: int
) -> int:
    """``max(column_id)`` over the table's **whole history**."""
    column = f"{quote_ident(meta)}.ducklake_column"
    sql = _LAST_COLUMN_ID_SQL.format(column=column)
    table = cast("Any", con).sql(sql, params={"table_id": table_id}).to_arrow_table()
    rows = _rows(table)
    return _as_int(rows[0]["last_column_id"]) if rows else 0


# -- primitive type map ----------------------------------------------


def map_primitive(column_type: str) -> PrimitiveType:
    """Resolve a DuckLake primitive type to Iceberg v2.

    Raises ``BlockedTypeError`` for a type with no v2 equivalent. The type
    table comes from ``ducklake_iceberg/mapping/types.py``; this module does not
    redefine it.
    """
    res = resolve_type(column_type)
    if res.status is TypeStatus.NESTED:
        raise SchemaShapeError(
            f"nested type {column_type!r} resolved as primitive; "
            f"the tree comes from parent_column, not from the type text"
        )
    if res.blocked or res.iceberg is None:
        raise BlockedTypeError([_blocked_finding(column_type, res.reason)])
    iceberg = res.iceberg
    if not isinstance(iceberg, PrimitiveType):
        raise SchemaShapeError(f"type {column_type!r} is not primitive")
    return iceberg


# -- findings ---------------------------------------------------------------


def _blocked_finding(column_type: str, reason: str, path: str = "") -> Finding:
    subject = f"column {path}" if path else "column"
    detail_path = f"{path} " if path else ""
    detail = f"{detail_path}({column_type!r}) has no equivalent in Iceberg v2"
    if reason:
        detail = f"{detail}: {reason}"
    return Finding(
        rule_id=_RULE_ID,
        severity=Severity.BLOCKER,
        subject=subject,
        detail=detail,
        remediation="change the type at the source",
        data={"column_type": column_type, "path": path},
    )


def _reserved_id_finding(col: DuckLakeColumn) -> Finding:
    return Finding(
        rule_id=_RULE_ID,
        severity=Severity.BLOCKER,
        subject=f"column {col.column_name}",
        detail=(
            f"column {col.column_name} uses column_id={col.column_id} "
            f"in the Iceberg reserved range (>= {_RESERVED_FLOOR})"
        ),
        remediation="open an issue; indicates an unsupported DuckLake version",
        data={"column_id": col.column_id, "column_name": col.column_name},
    )


# -- defaults --------------------------------------------------------


def _defaults(col: DuckLakeColumn, warnings: list[str], path: str) -> tuple[Any, Any]:
    """Decode ``initial_default``/``default_value`` into single-values.

    v2 is not normative for defaults; we only emit literal values of a mappable
    primitive type. SQL expressions (``default_value_dialect`` present) and
    nested types are dropped with a warning.
    """
    if col.default_value_dialect is not None:
        warnings.append(
            f"{path}: default_value_dialect="
            f"{col.default_value_dialect!r} is a SQL expression; dropped "
            f"(Iceberg cannot represent expressions)"
        )
        return None, None

    base = base_type_name(col.column_type)
    if base in NESTED_TYPES:
        if col.initial_default is not None or col.default_value is not None:
            warnings.append(
                f"{path}: default on nested type dropped (out of scope for v2)"
            )
        return None, None

    try:
        iceberg = map_primitive(col.column_type)
    except BlockedTypeError:
        return None, None

    initial = _decode_literal(col.initial_default, iceberg, warnings, path)
    write = _decode_literal(col.default_value, iceberg, warnings, path)
    return initial, write


def _decode_literal(
    raw: str | None, iceberg: PrimitiveType, warnings: list[str], path: str
) -> Any:
    """Convert the catalog's textual literal into the type's single-value."""
    if raw is None:
        return None
    try:
        from pyiceberg.conversions import partition_to_py

        return partition_to_py(iceberg, raw)
    except (ValueError, TypeError, AttributeError) as exc:
        warnings.append(
            f"{path}: default {raw!r} cannot be decoded for {iceberg}: {exc}"
        )
        return None


# -- resolving a column's type ---------------------------------------


def _type_kind(column_type: str) -> str:
    """'struct'|'list'|'map' for nested types; 'primitive' otherwise."""
    base = base_type_name(column_type)
    return base if base in NESTED_TYPES else "primitive"


# -- schema construction (pure) -------------------------------------------


def build_iceberg_schema(
    columns: Sequence[DuckLakeColumn],
    *,
    schema_id: int,
    last_column_id: int,
    emit_defaults: bool = False,
) -> SchemaMapping:
    """Single entry point. Does not access the database.

    Accumulates all blocked types into a single ``BlockedTypeError`` - a table
    with five invalid columns needs one report with all five, not five runs.
    """
    dropped: list[str] = []
    warnings: list[str] = []

    kept = _filter_internal(columns, dropped)

    by_id: dict[int, DuckLakeColumn] = {}
    for col in kept:
        if col.column_id in by_id:
            raise SchemaShapeError(
                f"column_id={col.column_id} duplicated in the same snapshot"
            )
        by_id[col.column_id] = col

    children: dict[int | None, list[DuckLakeColumn]] = {}
    for col in kept:
        children.setdefault(col.parent_column, []).append(col)
    for group in children.values():
        group.sort(key=lambda c: (c.column_order, c.column_id))

    for col in kept:
        if col.parent_column is not None and col.parent_column not in by_id:
            raise SchemaShapeError(
                f"column {col.column_name} (column_id={col.column_id}) "
                f"references parent_column={col.parent_column}, which is "
                f"missing from the snapshot"
            )

    roots = children.get(None, [])
    if not roots:
        raise SchemaShapeError("table has no top-level columns in the snapshot")

    blocked: list[Finding] = []
    paths: dict[int, str] = {}
    seen: set[int] = set()

    builder = _SchemaBuilder(
        children=children,
        emit_defaults=emit_defaults,
        blocked=blocked,
        paths=paths,
        seen=seen,
        warnings=warnings,
    )
    fields = [builder.build_field(c, "") for c in roots]

    if blocked:
        raise BlockedTypeError(blocked)

    missing = set(by_id) - seen
    if missing:
        raise SchemaShapeError(
            f"columns not reachable from the roots: {sorted(missing)}"
        )

    schema = Schema(*fields, schema_id=schema_id, identifier_field_ids=[])
    return SchemaMapping(
        schema=schema,
        schema_id=schema_id,
        last_column_id=last_column_id,
        columns_by_id=by_id,
        paths_by_id=paths,
        dropped=tuple(dropped),
        warnings=tuple(warnings),
    )


def _filter_internal(
    columns: Sequence[DuckLakeColumn], dropped: list[str]
) -> list[DuckLakeColumn]:
    """Three guards, in this order."""
    kept: list[DuckLakeColumn] = []
    for col in columns:
        if col.column_id in _INTERNAL_IDS or (
            col.parent_column is None
            and col.column_name.lower().startswith(_DUCKLAKE_PREFIX)
        ):
            dropped.append(col.column_name)
            continue
        if col.column_id > _RESERVED_FLOOR:
            raise BlockedTypeError([_reserved_id_finding(col)])
        kept.append(col)
    return kept


@dataclass(slots=True)
class _SchemaBuilder:
    """DFS from the roots, with field_id = column_id across the whole tree."""

    children: dict[int | None, list[DuckLakeColumn]]
    emit_defaults: bool
    blocked: list[Finding]
    paths: dict[int, str]
    seen: set[int]
    warnings: list[str]

    def build_field(self, col: DuckLakeColumn, parent_path: str) -> NestedField:
        path = f"{parent_path}.{col.column_name}" if parent_path else col.column_name
        self.seen.add(col.column_id)
        self.paths[col.column_id] = path
        if self.emit_defaults:
            initial, write = _defaults(col, self.warnings, path)
        else:
            initial, write = None, None
        return NestedField(
            field_id=col.column_id,
            name=col.column_name,
            field_type=self.build_type(col, path),
            required=not col.nulls_allowed,
            initial_default=initial,
            write_default=write,
        )

    def build_type(self, col: DuckLakeColumn, path: str) -> IcebergType:
        kind = _type_kind(col.column_type)
        kids = self.children.get(col.column_id, [])

        if kind == "struct":
            if not kids:
                raise SchemaShapeError(f"struct without children at {path}")
            return StructType(*(self.build_field(k, path) for k in kids))

        if kind == "list":
            if len(kids) != 1:
                raise SchemaShapeError(
                    f"list with {len(kids)} children at {path}, expected 1"
                )
            child = kids[0]
            self.seen.add(child.column_id)
            element_path = f"{path}.element"
            self.paths[child.column_id] = element_path
            return ListType(
                element_id=child.column_id,
                element_type=self.build_type(child, element_path),
                element_required=not child.nulls_allowed,
            )

        if kind == "map":
            if len(kids) != 2:
                raise SchemaShapeError(
                    f"map with {len(kids)} children at {path}, expected 2"
                )
            key, value = kids
            if key.nulls_allowed:
                self.warnings.append(
                    f"{path}.key: nulls_allowed=true ignored; "
                    f"a map key is always required in Iceberg"
                )
            key_path = f"{path}.key"
            value_path = f"{path}.value"
            self.seen.add(key.column_id)
            self.seen.add(value.column_id)
            self.paths[key.column_id] = key_path
            self.paths[value.column_id] = value_path
            return MapType(
                key_id=key.column_id,
                key_type=self.build_type(key, key_path),
                value_id=value.column_id,
                value_type=self.build_type(value, value_path),
                value_required=not value.nulls_allowed,
            )

        # primitive
        if kids:
            raise SchemaShapeError(
                f"{path}: primitive type {col.column_type} with children"
            )
        try:
            return map_primitive(col.column_type)
        except BlockedTypeError as exc:
            for finding in exc.findings:
                column_type = str(finding.data.get("column_type", col.column_type))
                res = resolve_type(col.column_type)
                self.blocked.append(_blocked_finding(column_type, res.reason, path))
            return StringType()  # placeholder; the schema will be discarded


# -- retroactive required column rule ----------------------------


def check_required_backfill(
    mapping: SchemaMapping, min_file_begin_snapshot: int | None
) -> list[Finding]:
    """``required`` column newer than some live file.

    In v2 there is no ``initial-default`` to fill the missing column in an old
    file: the metadata becomes inconsistent and reader behavior varies.
    ``min_file_begin_snapshot = None`` (table without files) disables the rule.
    """
    if min_file_begin_snapshot is None:
        return []
    findings: list[Finding] = []
    for column_id, col in mapping.columns_by_id.items():
        required = not col.nulls_allowed
        if required and col.begin_snapshot > min_file_begin_snapshot:
            path = mapping.paths_by_id.get(column_id, col.column_name)
            findings.append(
                Finding(
                    rule_id=_RULE_ID,
                    severity=Severity.BLOCKER,
                    subject=f"column {path}",
                    detail=(
                        f"column {path} is required and was added at snapshot "
                        f"{col.begin_snapshot}, later than live files "
                        f"(min begin_snapshot={min_file_begin_snapshot})"
                    ),
                    remediation=(
                        "make the column nullable at the source or compact the "
                        "table in DuckLake so that all live files "
                        "contain the column"
                    ),
                    data={
                        "column_id": column_id,
                        "path": path,
                        "begin_snapshot": col.begin_snapshot,
                        "min_file_begin_snapshot": min_file_begin_snapshot,
                    },
                )
            )
    return findings
