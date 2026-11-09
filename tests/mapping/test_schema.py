"""Tests for `build_iceberg_schema` and helpers (pure, no catalog).

Covers the pure scenarios. Scenarios that depend on a real catalog/Parquet
live in the integration matrix and are left out of this file.
"""

from __future__ import annotations

import pytest
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    StringType,
    StructType,
)

from ducklake_iceberg.mapping.schema import (
    BlockedTypeError,
    DuckLakeColumn,
    SchemaMapping,
    SchemaShapeError,
    build_iceberg_schema,
    map_primitive,
)


def col(
    column_id: int,
    name: str,
    column_type: str,
    *,
    parent: int | None = None,
    order: int = 0,
    nulls: bool = True,
    initial_default: str | None = None,
    default_value: str | None = None,
    default_value_dialect: str | None = None,
    begin_snapshot: int = 1,
) -> DuckLakeColumn:
    return DuckLakeColumn(
        column_id=column_id,
        parent_column=parent,
        column_order=order,
        column_name=name,
        column_type=column_type,
        nulls_allowed=nulls,
        initial_default=initial_default,
        default_value=default_value,
        default_value_type=None,
        default_value_dialect=default_value_dialect,
        begin_snapshot=begin_snapshot,
    )


def build(columns: list[DuckLakeColumn], **kwargs: object) -> SchemaMapping:
    params = {"schema_id": 0, "last_column_id": max(c.column_id for c in columns)}
    params.update(kwargs)  # type: ignore[arg-type]
    return build_iceberg_schema(columns, **params)  # type: ignore[arg-type]


# -- flat table ------------------------------------------------------


def test_flat_table_field_ids_and_types() -> None:
    cols = [
        col(1, "id", "int32", order=0),
        col(2, "state", "varchar", order=1),
        col(3, "dt", "date", order=2),
        col(4, "value", "float64", order=3),
    ]
    mapping = build(cols)
    fields = mapping.schema.fields
    assert [f.field_id for f in fields] == [1, 2, 3, 4]
    assert [f.name for f in fields] == ["id", "state", "dt", "value"]
    assert isinstance(fields[0].field_type, IntegerType)
    assert isinstance(fields[1].field_type, StringType)
    assert isinstance(fields[2].field_type, DateType)
    assert isinstance(fields[3].field_type, DoubleType)


def test_flat_table_respects_column_order() -> None:
    cols = [
        col(1, "a", "int32", order=2),
        col(2, "b", "int32", order=0),
        col(3, "c", "int32", order=1),
    ]
    mapping = build(cols)
    assert [f.name for f in mapping.schema.fields] == ["b", "c", "a"]


# -- required ----------------------------------------------------------


def test_required_from_nulls_allowed() -> None:
    cols = [
        col(1, "id", "int32", nulls=False),
        col(2, "state", "varchar", nulls=True),
    ]
    mapping = build(cols)
    assert mapping.schema.fields[0].required is True
    assert mapping.schema.fields[1].required is False


# -- struct ------------------------------------------------------------


def test_struct_with_two_children() -> None:
    cols = [
        col(1, "address", "struct", order=0),
        col(2, "street", "varchar", parent=1, order=0),
        col(3, "number", "int32", parent=1, order=1),
    ]
    mapping = build(cols)
    struct = mapping.schema.fields[0].field_type
    assert isinstance(struct, StructType)
    assert [f.field_id for f in struct.fields] == [2, 3]


# -- list --------------------------------------------------------------


def test_list_of_int32() -> None:
    cols = [
        col(1, "tags", "list", order=0),
        col(2, "item", "int32", parent=1, order=0, nulls=False),
    ]
    mapping = build(cols)
    lst = mapping.schema.fields[0].field_type
    assert isinstance(lst, ListType)
    assert lst.element_id == 2
    assert lst.element_required is True
    assert mapping.paths_by_id[2] == "tags.element"


def test_list_element_required_reflects_nulls_allowed() -> None:
    cols = [
        col(1, "tags", "list"),
        col(2, "item", "int32", parent=1, nulls=True),
    ]
    mapping = build(cols)
    lst = mapping.schema.fields[0].field_type
    assert isinstance(lst, ListType)
    assert lst.element_required is False


# -- map ---------------------------------------------------------------


def test_map_key_value_order() -> None:
    cols = [
        col(1, "attributes", "map", order=0),
        col(2, "key", "varchar", parent=1, order=0),
        col(3, "value", "int64", parent=1, order=1),
    ]
    mapping = build(cols)
    mp = mapping.schema.fields[0].field_type
    assert isinstance(mp, MapType)
    assert mp.key_id == 2
    assert mp.value_id == 3
    assert isinstance(mp.key_type, StringType)
    assert isinstance(mp.value_type, LongType)


def test_map_order_inversion_swaps_key_value() -> None:
    cols = [
        col(1, "attributes", "map", order=0),
        col(2, "key", "varchar", parent=1, order=1),  # inverted
        col(3, "value", "int64", parent=1, order=0),
    ]
    mapping = build(cols)
    mp = mapping.schema.fields[0].field_type
    assert isinstance(mp, MapType)
    # now the child with the lowest column_order (value, order=0) is the key
    assert mp.key_id == 3
    assert mp.value_id == 2


# -- list with two children ----------------------------------------------


def test_list_with_two_children_raises() -> None:
    cols = [
        col(1, "tags", "list"),
        col(2, "a", "int32", parent=1, order=0),
        col(3, "b", "int32", parent=1, order=1),
    ]
    with pytest.raises(SchemaShapeError):
        build(cols)


# -- orphan parent ------------------------------------------------------


def test_orphan_parent_raises_with_id() -> None:
    cols = [
        col(1, "id", "int32"),
        col(2, "x", "int32", parent=99),
    ]
    with pytest.raises(SchemaShapeError) as exc:
        build(cols)
    assert "99" in str(exc.value)


# -- cycle -------------------------------------------------------------


def test_cycle_does_not_recurse_forever() -> None:
    # a(1) -> parent b(2); b(2) -> parent a(1): no root, no recursion
    cols = [
        col(1, "a", "struct", parent=2),
        col(2, "b", "struct", parent=1),
    ]
    with pytest.raises(SchemaShapeError):
        build(cols)


# -- two blocked types --------------------------------------------


def test_two_blocked_types_accumulate() -> None:
    cols = [
        col(1, "a", "uint32"),
        col(2, "b", "interval"),
    ]
    with pytest.raises(BlockedTypeError) as exc:
        build(cols)
    assert len(exc.value.findings) == 2


# -- timestamp_ms -----------------------------------------------------


def test_timestamp_ms_blocked_no_promotion_hint() -> None:
    cols = [col(1, "ts", "timestamp_ms")]
    with pytest.raises(BlockedTypeError) as exc:
        build(cols)
    detail = exc.value.findings[0].detail.lower()
    assert "micros" not in detail or "multiply" in detail or "10^" in detail
    # the message must not suggest silently promoting
    assert "promot" not in detail


# -- timestamp_ns -----------------------------------------------------


def test_timestamp_ns_blocked_in_v2() -> None:
    cols = [col(1, "ts", "timestamp_ns")]
    with pytest.raises(BlockedTypeError):
        build(cols)


# -- decimal(40,2) ----------------------------------------------------


def test_decimal_over_38_blocked_with_precision_message() -> None:
    cols = [col(1, "v", "decimal(40,2)")]
    with pytest.raises(BlockedTypeError) as exc:
        build(cols)
    detail = exc.value.findings[0].detail.lower()
    assert "precis" in detail or "38" in detail


# -- internal column_id ------------------------------------------------


def test_internal_column_id_filtered() -> None:
    cols = [
        col(1, "id", "int32"),
        col(2147483540, "_ducklake_internal_row_id", "int64"),
    ]
    mapping = build(cols)
    assert [f.field_id for f in mapping.schema.fields] == [1]
    assert "_ducklake_internal_row_id" in mapping.dropped


# -- top-level _ducklake_ column ----------------------------------------


def test_top_level_ducklake_prefix_filtered() -> None:
    cols = [
        col(1, "id", "int32"),
        col(5, "_ducklake_x", "int32"),
    ]
    mapping = build(cols)
    assert [f.field_id for f in mapping.schema.fields] == [1]
    assert "_ducklake_x" in mapping.dropped


# -- nested _ducklake_ column is NOT filtered --------------------------


def test_nested_ducklake_prefix_not_filtered() -> None:
    cols = [
        col(1, "s", "struct"),
        col(2, "_ducklake_x", "int32", parent=1),
    ]
    mapping = build(cols)
    struct = mapping.schema.fields[0].field_type
    assert isinstance(struct, StructType)
    assert [f.name for f in struct.fields] == ["_ducklake_x"]


# -- column_id in the reserved range -------------------------------------


def test_reserved_floor_column_id_blocked() -> None:
    cols = [
        col(1, "id", "int32"),
        col(2147483100, "x", "int32"),
    ]
    with pytest.raises(BlockedTypeError):
        build(cols)


# -- defaults -----------------------------------------------------


def test_initial_default_not_emitted_by_default() -> None:
    cols = [col(1, "n", "int32", initial_default="7")]
    mapping = build(cols)
    assert mapping.schema.fields[0].initial_default is None
    assert mapping.columns_by_id[1].initial_default == "7"


def test_initial_default_emitted_when_requested() -> None:
    cols = [col(1, "n", "int32", initial_default="7")]
    mapping = build(cols, emit_defaults=True)
    assert mapping.schema.fields[0].initial_default == 7


# -- default_value_dialect --------------------------------------------


def test_default_dialect_discarded_with_warning() -> None:
    cols = [
        col(1, "n", "int32", initial_default="now()", default_value_dialect="duckdb")
    ]
    mapping = build(cols, emit_defaults=True)
    assert mapping.schema.fields[0].initial_default is None
    assert any("duckdb" in w for w in mapping.warnings)


# -- columns added after the table was created ---------------------------


def test_not_null_column_added_later_is_published_as_optional() -> None:
    """Older files lack the column and Iceberg v2 has no initial-default."""
    cols = [
        col(1, "id", "int32", nulls=False, begin_snapshot=1),
        col(2, "newcol", "int32", nulls=False, begin_snapshot=5),
    ]
    mapping = build(cols)
    assert mapping.schema.find_field("id").required is True
    assert mapping.schema.find_field("newcol").required is False
    assert any("newcol" in w and "optional" in w for w in mapping.warnings)


def test_nested_field_added_later_is_published_as_optional() -> None:
    cols = [
        col(1, "addr", "struct", begin_snapshot=1),
        col(2, "city", "varchar", parent=1, order=0, begin_snapshot=1),
        col(3, "country", "varchar", parent=1, order=1, nulls=False, begin_snapshot=4),
    ]
    mapping = build(cols)
    assert mapping.schema.find_field(3).required is False


# -- extras: arity and edge cases --------------------------------------


def test_map_with_one_child_raises() -> None:
    cols = [
        col(1, "m", "map"),
        col(2, "k", "varchar", parent=1, order=0),
    ]
    with pytest.raises(SchemaShapeError):
        build(cols)


def test_zero_columns_raises() -> None:
    with pytest.raises(SchemaShapeError):
        build_iceberg_schema([], schema_id=0, last_column_id=0)


def test_duplicate_column_id_raises() -> None:
    cols = [
        col(1, "a", "int32"),
        col(1, "b", "int32"),
    ]
    with pytest.raises(SchemaShapeError):
        build(cols)


def test_map_nullable_key_warns_but_required() -> None:
    cols = [
        col(1, "m", "map"),
        col(2, "k", "varchar", parent=1, order=0, nulls=True),
        col(3, "v", "int64", parent=1, order=1),
    ]
    mapping = build(cols)
    assert any("key" in w for w in mapping.warnings)


def test_struct_without_children_raises() -> None:
    cols = [col(1, "s", "struct")]
    with pytest.raises(SchemaShapeError):
        build(cols)


def test_uppercase_type_normalized() -> None:
    cols = [col(1, "n", "INT32")]
    mapping = build(cols)
    assert isinstance(mapping.schema.fields[0].field_type, IntegerType)


def test_map_primitive_blocks_unknown_type() -> None:
    with pytest.raises(BlockedTypeError):
        map_primitive("no_such_type")


def test_map_primitive_resolves_known_type() -> None:
    assert isinstance(map_primitive("int64"), LongType)


def test_last_column_id_preserved_in_mapping() -> None:
    cols = [col(1, "a", "int32")]
    mapping = build(cols, last_column_id=42)
    assert mapping.last_column_id == 42


def test_schema_id_preserved() -> None:
    cols = [col(1, "a", "int32")]
    mapping = build(cols, schema_id=7)
    assert mapping.schema_id == 7
    assert mapping.schema.schema_id == 7
