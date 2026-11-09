"""Schema change detection and classification.

Compares the **published** Iceberg schema with the schema built from DuckLake
at the target snapshot (``ducklake_iceberg.mapping.schema``) and classifies each
change as a safe evolution, a degradation or a blocker in Iceberg v2.

The comparison is done on ``pyiceberg.schema.Schema`` and not on DuckLake type
names (``int16``, ``int32``, ``float64``...): by construction the field ID is
the DuckLake ``column_id`` across the whole tree (including nested fields),
and the type is already the Iceberg type - ``int8/int16/int32`` all map to
``int``, so switching between them is not a schema change.

``export`` currently performs its own minimal check (``export._evolve_schema``);
this module is the complete classification matrix, plus the ``column_id`` reuse
detector (layers 1 and 2).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final

from pyiceberg.schema import Schema
from pyiceberg.types import (
    DecimalType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StructType,
)

from ducklake_iceberg.doctor.model import quote_ident
from ducklake_iceberg.errors import CompatibilityError

if TYPE_CHECKING:
    import duckdb

LOG = logging.getLogger("ducklake_iceberg")


class ChangeKind(StrEnum):
    """Kind of schema change detected."""

    ADD_OPTIONAL = "add_optional"
    ADD_REQUIRED = "add_required"
    DROP = "drop"
    RENAME = "rename"
    REORDER = "reorder"
    PROMOTE_TYPE = "promote_type"
    NARROW_TYPE = "narrow_type"
    RETYPE = "retype"
    RELAX_REQUIRED = "relax_required"  # required -> optional
    TIGHTEN_REQUIRED = "tighten_required"  # optional -> required
    CONTAINER_CHANGE = "container_change"
    ID_REUSE_SUSPECTED = "id_reuse_suspected"


class Verdict(StrEnum):
    """Verdict for a schema change."""

    SAFE = "safe"
    DEGRADED = "degraded"  # applicable with documented loss, only with a flag
    BLOCKER = "blocker"  # exit 2


@dataclass(frozen=True)
class ColumnChange:
    """A change to a schema field (``column_id`` == field ID)."""

    kind: ChangeKind
    verdict: Verdict
    column_id: int
    parent_column: int | None  # None at the root level
    name_before: str | None
    name_after: str | None
    type_before: str | None
    type_after: str | None
    detail: str
    remediation: str | None


@dataclass(frozen=True)
class SchemaChange:
    """Result of schema change detection."""

    changed: bool
    changes: tuple[ColumnChange, ...]
    new_schema: Schema | None  # None when changed is False
    new_schema_id: int | None
    last_column_id_before: int | None
    last_column_id_after: int

    @property
    def blockers(self) -> tuple[ColumnChange, ...]:
        """Only the blocking changes."""
        return tuple(c for c in self.changes if c.verdict is Verdict.BLOCKER)

    @property
    def degraded(self) -> tuple[ColumnChange, ...]:
        """Only the degradations accepted via an explicit flag."""
        return tuple(c for c in self.changes if c.verdict is Verdict.DEGRADED)


# -- exceptions ---------------------------------------------------------------


class SchemaBlockerError(CompatibilityError):
    """Blocking schema change detected (exit 2)."""

    def __init__(
        self,
        message: str,
        *,
        changes: Sequence[ColumnChange] = (),
        hint: str | None = None,
    ) -> None:
        super().__init__(message, hint=hint)
        self.changes = tuple(changes)


class IDReuseError(SchemaBlockerError):
    """Suspected ``column_id`` reuse (exit 2)."""


# -- tree index -------------------------------------------------------

_ROLE_FIELD: Final = "field"
_ROLE_ELEMENT: Final = "element"
_ROLE_KEY: Final = "key"
_ROLE_VALUE: Final = "value"


@dataclass(frozen=True)
class _Node:
    field: NestedField
    parent: int | None
    role: str  # field | element | key | value


def _children(t: IcebergType) -> Iterator[tuple[NestedField, str]]:
    if isinstance(t, StructType):
        for f in t.fields:
            yield f, _ROLE_FIELD
    elif isinstance(t, ListType):
        yield t.element_field, _ROLE_ELEMENT
    elif isinstance(t, MapType):
        yield t.key_field, _ROLE_KEY
        yield t.value_field, _ROLE_VALUE


def _index(schema: Schema) -> dict[int, _Node]:
    out: dict[int, _Node] = {}

    def walk(t: IcebergType, parent: int | None) -> None:
        for f, role in _children(t):
            out[f.field_id] = _Node(field=f, parent=parent, role=role)
            walk(f.field_type, f.field_id)

    walk(schema.as_struct(), None)
    return out


def _sibling_order(schema: Schema) -> dict[int | None, list[int]]:
    """Field IDs of each struct, in declared order (parent -> children)."""
    out: dict[int | None, list[int]] = {}

    def walk(t: IcebergType, parent: int | None) -> None:
        if isinstance(t, StructType):
            out[parent] = [f.field_id for f in t.fields]
        for f, _role in _children(t):
            walk(f.field_type, f.field_id)

    walk(schema.as_struct(), None)
    return out


def _shape(t: IcebergType) -> str:
    if isinstance(t, StructType):
        return "struct"
    if isinstance(t, ListType):
        return "list"
    if isinstance(t, MapType):
        return "map"
    return "primitive"


# -- types ------------------------------------------------------------

_PROMOTIONS: Final[frozenset[tuple[type[IcebergType], type[IcebergType]]]] = frozenset(
    {(IntegerType, LongType), (FloatType, DoubleType)}
)


def _type_change(old: IcebergType, new: IcebergType) -> tuple[ChangeKind, Verdict]:
    """Classify a primitive type change on the same field ID."""
    if (type(old), type(new)) in _PROMOTIONS:
        return ChangeKind.PROMOTE_TYPE, Verdict.SAFE
    if (type(new), type(old)) in _PROMOTIONS:
        return ChangeKind.NARROW_TYPE, Verdict.BLOCKER
    if (
        isinstance(old, DecimalType)
        and isinstance(new, DecimalType)
        and old.scale == new.scale
    ):
        if new.precision > old.precision:
            return ChangeKind.PROMOTE_TYPE, Verdict.SAFE
        return ChangeKind.NARROW_TYPE, Verdict.BLOCKER
    return ChangeKind.RETYPE, Verdict.BLOCKER


# -- diff -----------------------------------------------


def diff_schemas(
    before: Schema,
    after: Schema,
    *,
    relax_required: bool = False,
) -> tuple[ColumnChange, ...]:
    """Per-field-ID difference between the published and the new schema.

    Args:
        before: Current (published) Iceberg schema.
        after: Schema built from DuckLake at the target snapshot.
        relax_required: ``--relax-required``: a new ``NOT NULL`` column becomes
            ``DEGRADED`` (published as optional) instead of ``BLOCKER``.

    Returns:
        Changes in field ID order; empty when nothing changed.
    """
    old = _index(before)
    new = _index(after)

    # Common fields first: a field that changed structure absorbs the added or
    # removed children (they are a consequence of the change, not changes).
    compared: dict[int, list[ColumnChange]] = {
        fid: _compare(old[fid], new[fid]) for fid in old.keys() & new.keys()
    }
    reshaped = {
        fid
        for fid, found in compared.items()
        if any(c.kind is ChangeKind.CONTAINER_CHANGE for c in found)
    }

    changes: list[ColumnChange] = []
    for fid in sorted(old.keys() | new.keys()):
        o = old.get(fid)
        n = new.get(fid)
        if o is not None and n is not None:
            changes.extend(compared[fid])
        elif n is not None:
            if n.parent is None or (n.parent in old and n.parent not in reshaped):
                changes.append(_added(n, relax_required=relax_required))
        elif o is not None and (
            o.parent is None or (o.parent in new and o.parent not in reshaped)
        ):
            changes.append(_change(ChangeKind.DROP, Verdict.SAFE, o, None))

    changes.extend(_reorders(before, after, old, new))
    return tuple(changes)


def _change(
    kind: ChangeKind,
    verdict: Verdict,
    o: _Node | None,
    n: _Node | None,
    *,
    detail: str | None = None,
    remediation: str | None = None,
) -> ColumnChange:
    ref = n or o
    assert ref is not None
    name_before = o.field.name if o else None
    name_after = n.field.name if n else None
    return ColumnChange(
        kind=kind,
        verdict=verdict,
        column_id=ref.field.field_id,
        parent_column=ref.parent,
        name_before=name_before,
        name_after=name_after,
        type_before=str(o.field.field_type) if o else None,
        type_after=str(n.field.field_type) if n else None,
        detail=detail or f"{kind.value}: {name_before or name_after}",
        remediation=remediation,
    )


def _added(n: _Node, *, relax_required: bool) -> ColumnChange:
    name = n.field.name
    if not n.field.required:
        return _change(
            ChangeKind.ADD_OPTIONAL,
            Verdict.SAFE,
            None,
            n,
            detail=f"column {name!r} added (optional)",
        )
    if relax_required:
        return _change(
            ChangeKind.ADD_REQUIRED,
            Verdict.DEGRADED,
            None,
            n,
            detail=(
                f"column {name!r} is NOT NULL; published as optional (--relax-required)"
            ),
        )
    return _change(
        ChangeKind.ADD_REQUIRED,
        Verdict.BLOCKER,
        None,
        n,
        detail=(
            f"column {name!r} is NOT NULL; Iceberg v2 does not materialize a "
            f"default for old files"
        ),
        remediation="make the column nullable at the source or use --relax-required",
    )


def _compare(o: _Node, n: _Node) -> list[ColumnChange]:
    out: list[ColumnChange] = []
    old_t, new_t = o.field.field_type, n.field.field_type
    name = n.field.name

    if o.parent != n.parent or o.role != n.role or _shape(old_t) != _shape(new_t):
        return [
            _change(
                ChangeKind.CONTAINER_CHANGE,
                Verdict.BLOCKER,
                o,
                n,
                detail=(
                    f"field {name!r} (id {n.field.field_id}) changed structure: "
                    f"{_shape(old_t)} -> {_shape(new_t)}"
                ),
                remediation="recreate the Iceberg table (new prefix)",
            )
        ]

    if o.field.name != n.field.name and o.role == _ROLE_FIELD:
        out.append(
            _change(
                ChangeKind.RENAME,
                Verdict.SAFE,
                o,
                n,
                detail=f"column renamed: {o.field.name!r} -> {name!r}",
            )
        )

    if old_t.is_primitive and old_t != new_t:
        if o.role == _ROLE_KEY:
            kind, verdict = ChangeKind.CONTAINER_CHANGE, Verdict.BLOCKER
        else:
            kind, verdict = _type_change(old_t, new_t)
        out.append(
            _change(
                kind,
                verdict,
                o,
                n,
                detail=f"field {name!r}: {old_t} -> {new_t} ({kind.value})",
                remediation=(
                    None
                    if verdict is Verdict.SAFE
                    else "revert the type at the source or rewrite the files"
                ),
            )
        )

    if o.field.required and not n.field.required:
        out.append(
            _change(
                ChangeKind.RELAX_REQUIRED,
                Verdict.SAFE,
                o,
                n,
                detail=f"field {name!r}: NOT NULL -> NULL",
            )
        )
    elif not o.field.required and n.field.required:
        out.append(
            _change(
                ChangeKind.TIGHTEN_REQUIRED,
                Verdict.BLOCKER,
                o,
                n,
                detail=(
                    f"field {name!r} became NOT NULL without validation of the "
                    f"old files"
                ),
                remediation="keep the column nullable",
            )
        )
    return out


def _reorders(
    before: Schema,
    after: Schema,
    old: dict[int, _Node],
    new: dict[int, _Node],
) -> list[ColumnChange]:
    """Reordering of the common fields of a struct (SAFE: projection is by ID)."""
    out: list[ColumnChange] = []
    old_order = _sibling_order(before)
    for parent, new_ids in _sibling_order(after).items():
        old_ids = old_order.get(parent)
        if old_ids is None:
            continue
        common = set(old_ids) & set(new_ids)
        a = [i for i in old_ids if i in common]
        b = [i for i in new_ids if i in common]
        if a != b:
            first = next(x for x, y in zip(a, b, strict=True) if x != y)
            out.append(
                _change(
                    ChangeKind.REORDER,
                    Verdict.SAFE,
                    old[first],
                    new[first],
                    detail="column order changed (projection is by field ID)",
                )
            )
    return out


# -- reuse detector -----------------------------------------------


def detect_id_reuse(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    table_id: int,
) -> tuple[ColumnChange, ...]:
    """Layer 1: ``column_id`` with a gap in its lifetime.

    A legitimate ``column_id`` has contiguous versions (RENAME/ALTER close a
    row exactly where the next one begins); a gap means the ID died and was
    reborn.
    """
    sql = f"""
        WITH v AS (
            SELECT column_id, column_name, begin_snapshot, end_snapshot,
                   lag(end_snapshot) OVER (PARTITION BY column_id
                                           ORDER BY begin_snapshot) AS prev_end
            FROM {quote_ident(meta)}.ducklake_column
            WHERE table_id = ?
        )
        SELECT column_id, column_name, prev_end, begin_snapshot
        FROM v
        WHERE prev_end IS NOT NULL
          AND prev_end < begin_snapshot
        ORDER BY column_id, begin_snapshot
    """  # noqa: S608 - only interpolates the identifier validated by quote_ident
    rows = con.execute(sql, [table_id]).fetchall()
    return tuple(
        ColumnChange(
            kind=ChangeKind.ID_REUSE_SUSPECTED,
            verdict=Verdict.BLOCKER,
            column_id=int(column_id),
            parent_column=None,
            name_before=None,
            name_after=str(name),
            type_before=None,
            type_after=None,
            detail=(
                f"column_id {column_id} ({name!r}) has a discontinuous interval "
                f"({prev_end} -> {begin}): possible DROP+ADD with reuse"
            ),
            remediation="do not publish; column_id reuse must be resolved first",
        )
        for column_id, name, prev_end, begin in rows
    )


def check_id_watermark(
    before: Schema | None,
    after: Schema,
    last_column_id: int | None,
) -> tuple[ColumnChange, ...]:
    """Layer 2: new field with an ID below the watermark.

    ``L = max(ducklake.last-column-id, highest published field ID)``; every
    field missing from the published schema must have ID ``> L``.
    """
    existing = set(_index(before)) if before is not None else set()
    watermark = max(
        last_column_id or 0,
        before.highest_field_id if before is not None else 0,
    )
    return tuple(
        ColumnChange(
            kind=ChangeKind.ID_REUSE_SUSPECTED,
            verdict=Verdict.BLOCKER,
            column_id=fid,
            parent_column=node.parent,
            name_before=None,
            name_after=node.field.name,
            type_before=None,
            type_after=str(node.field.field_type),
            detail=(
                f"new field {node.field.name!r} has column_id {fid} <= watermark "
                f"{watermark}"
            ),
            remediation="do not publish; column_id reuse must be resolved first",
        )
        for fid, node in sorted(_index(after).items())
        if fid not in existing and fid <= watermark
    )


# -- main function -------------------------------------------------------


def detect_schema_change(
    before: Schema | None,
    after: Schema,
    *,
    existing_schema_ids: frozenset[int] = frozenset(),
    last_column_id: int | None = None,
    reuse: Sequence[ColumnChange] = (),
    relax_required: bool = False,
) -> SchemaChange:
    """Classify the change and build the new schema.

    Args:
        before: Published schema (``None`` on the first sync).
        after: DuckLake schema at the target snapshot.
        existing_schema_ids: ``schema-id`` values already in ``metadata.json``.
        last_column_id: Published ``ducklake.last-column-id``.
        reuse: Layer 1 findings (``detect_id_reuse``), when already collected.
        relax_required: Accept a new ``NOT NULL`` column as optional.

    Returns:
        ``SchemaChange``; ``changed`` is False when the diff is empty (only
        ``ducklake.schema-version`` changes, with no new ``schema-id``).

    Raises:
        IDReuseError: Layer 1 or 2 of the reuse detector (exit 2).
        SchemaBlockerError: Any ``BLOCKER`` change (exit 2).
    """
    suspects = (*reuse, *check_id_watermark(before, after, last_column_id))
    if suspects:
        raise IDReuseError(
            "; ".join(c.detail for c in suspects),
            changes=suspects,
            hint="column_id reuse detected; see the error details",
        )

    changes = (
        diff_schemas(before, after, relax_required=relax_required)
        if before is not None
        else ()
    )
    blockers = [c for c in changes if c.verdict is Verdict.BLOCKER]
    if blockers:
        raise SchemaBlockerError(
            "schema change incompatible with Iceberg v2: "
            + "; ".join(c.detail for c in blockers),
            changes=blockers,
            hint=blockers[0].remediation,
        )

    last_after = max(last_column_id or 0, after.highest_field_id)
    changed = before is None or bool(changes)
    if not changed:
        return SchemaChange(
            changed=False,
            changes=(),
            new_schema=None,
            new_schema_id=None,
            last_column_id_before=last_column_id,
            last_column_id_after=last_after,
        )

    new_schema_id = max(existing_schema_ids, default=-1) + 1
    relaxed = frozenset(
        c.column_id for c in changes if c.kind is ChangeKind.ADD_REQUIRED
    )
    new_schema = _rebuild(after, schema_id=new_schema_id, optional_ids=relaxed)
    return SchemaChange(
        changed=True,
        changes=changes,
        new_schema=new_schema,
        new_schema_id=new_schema_id,
        last_column_id_before=last_column_id,
        last_column_id_after=last_after,
    )


def _rebuild(schema: Schema, *, schema_id: int, optional_ids: frozenset[int]) -> Schema:
    """Copy the schema with a new ``schema_id``, making ``optional_ids`` optional."""

    def visit(f: NestedField) -> NestedField:
        return f.model_copy(
            update={
                "field_type": _visit_type(f.field_type, visit),
                "required": f.required and f.field_id not in optional_ids,
            }
        )

    return Schema(
        *[visit(f) for f in schema.fields],
        schema_id=schema_id,
        identifier_field_ids=[
            i for i in schema.identifier_field_ids if i not in optional_ids
        ],
    )


def _visit_type(
    t: IcebergType, fn: Callable[[NestedField], NestedField]
) -> IcebergType:
    if isinstance(t, StructType):
        return StructType(*[fn(f) for f in t.fields])
    if isinstance(t, ListType):
        element = fn(t.element_field)
        return ListType(
            element_id=element.field_id,
            element_type=element.field_type,
            element_required=element.required,
        )
    if isinstance(t, MapType):
        key = fn(t.key_field)
        value = fn(t.value_field)
        return MapType(
            key_id=key.field_id,
            key_type=key.field_type,
            value_id=value.field_id,
            value_type=value.field_type,
            value_required=value.required,
        )
    return t
