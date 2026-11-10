"""Schema evolution between the published Iceberg schema and the new one.

Iceberg v2 allows only the type promotions listed in ``_PROMOTIONS`` (plus
decimal precision widening); anything else is a blocker, because readers would
misread the older files.
"""

from __future__ import annotations

from collections.abc import Callable
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

from ducklake_iceberg.errors import (
    CompatibilityError,
)

if TYPE_CHECKING:
    from pyiceberg.table.metadata import TableMetadata


# -- schema --------------------------------------------------------------------

_PROMOTIONS: Final[frozenset[tuple[type[IcebergType], type[IcebergType]]]] = frozenset(
    {(IntegerType, LongType), (FloatType, DoubleType)}
)


def evolve_schema(schema: Schema, base: TableMetadata | None) -> Schema:
    """Check the evolution against the published schema and preserve nullability.

    Field IDs come from DuckLake's ``column_id``, which is never reused; what
    can still break readers is changing the type of an ID to something that is
    not an Iceberg promotion (exit 2) or making a previously optional column
    required - it is kept optional, the only valid form in Iceberg.
    """
    if base is None:
        return schema
    published: dict[int, NestedField] = {}
    for old in base.schemas:
        for fid in old.field_ids:
            published.setdefault(fid, old.find_field(fid))

    def check_type(fid: int, name: str, new: IcebergType) -> None:
        old_field = published.get(fid)
        if old_field is None or not new.is_primitive:
            return
        old = old_field.field_type
        if old == new or (type(old), type(new)) in _PROMOTIONS:
            return
        if (
            isinstance(old, DecimalType)
            and isinstance(new, DecimalType)
            and old.scale == new.scale
            and new.precision >= old.precision
        ):
            return
        raise CompatibilityError(
            f"column {name!r} (field id {fid}) changed from {old} to {new}; "
            f"that is not a valid Iceberg promotion",
            hint="recreate the Iceberg table (new prefix) or revert the type",
        )

    def visit(f: NestedField) -> NestedField:
        check_type(f.field_id, f.name, f.field_type)
        field_type = _visit_type(f.field_type, visit)
        old_field = published.get(f.field_id)
        required = f.required and (old_field is None or old_field.required)
        return f.model_copy(update={"field_type": field_type, "required": required})

    return Schema(
        *[visit(f) for f in schema.fields],
        schema_id=schema.schema_id,
        identifier_field_ids=schema.identifier_field_ids,
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
