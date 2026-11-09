"""Schema change classification by field ID."""

from __future__ import annotations

import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
)

from ducklake_iceberg.catalog.connection import open_catalog
from ducklake_iceberg.mapping.schema import build_iceberg_schema, read_columns
from ducklake_iceberg.sync.schema_evolution import (
    ChangeKind,
    IDReuseError,
    SchemaBlockerError,
    Verdict,
    check_id_watermark,
    detect_id_reuse,
    detect_schema_change,
    diff_schemas,
)
from tests.integration.conftest import CatalogHarness


def _f(fid: int, name: str, t: IcebergType, *, required: bool = False) -> NestedField:
    return NestedField(field_id=fid, name=name, field_type=t, required=required)


def _schema(*fields: NestedField, schema_id: int = 0) -> Schema:
    return Schema(*fields, schema_id=schema_id)


BASE = _schema(
    _f(1, "id", IntegerType(), required=True),
    _f(2, "name", StringType()),
    _f(3, "value", DecimalType(10, 2)),
)


def _kinds(
    before: Schema, after: Schema, **kw: bool
) -> list[tuple[ChangeKind, Verdict]]:
    return [(c.kind, c.verdict) for c in diff_schemas(before, after, **kw)]


def test_identical_schema_has_no_changes() -> None:
    assert diff_schemas(BASE, BASE) == ()
    result = detect_schema_change(BASE, BASE, existing_schema_ids=frozenset({0}))
    assert not result.changed
    assert result.new_schema is None
    assert result.new_schema_id is None
    assert result.last_column_id_after == 3


@pytest.mark.parametrize(
    ("after", "expected"),
    [
        (
            _schema(*BASE.fields, _f(4, "state", StringType())),
            [(ChangeKind.ADD_OPTIONAL, Verdict.SAFE)],
        ),
        (
            _schema(*BASE.fields, _f(4, "state", StringType(), required=True)),
            [(ChangeKind.ADD_REQUIRED, Verdict.BLOCKER)],
        ),
        (_schema(*BASE.fields[:2]), [(ChangeKind.DROP, Verdict.SAFE)]),
        (
            _schema(BASE.fields[0], _f(2, "customer", StringType()), BASE.fields[2]),
            [(ChangeKind.RENAME, Verdict.SAFE)],
        ),
        (
            _schema(BASE.fields[2], BASE.fields[0], BASE.fields[1]),
            [(ChangeKind.REORDER, Verdict.SAFE)],
        ),
        (
            _schema(_f(1, "id", LongType(), required=True), *BASE.fields[1:]),
            [(ChangeKind.PROMOTE_TYPE, Verdict.SAFE)],
        ),
        (
            _schema(*BASE.fields[:2], _f(3, "value", DecimalType(12, 2))),
            [(ChangeKind.PROMOTE_TYPE, Verdict.SAFE)],
        ),
        (
            _schema(*BASE.fields[:2], _f(3, "value", DecimalType(8, 2))),
            [(ChangeKind.NARROW_TYPE, Verdict.BLOCKER)],
        ),
        (
            _schema(*BASE.fields[:2], _f(3, "value", DecimalType(12, 3))),
            [(ChangeKind.RETYPE, Verdict.BLOCKER)],
        ),
        (
            _schema(_f(1, "id", StringType(), required=True), *BASE.fields[1:]),
            [(ChangeKind.RETYPE, Verdict.BLOCKER)],
        ),
        (
            _schema(_f(1, "id", IntegerType()), *BASE.fields[1:]),
            [(ChangeKind.RELAX_REQUIRED, Verdict.SAFE)],
        ),
        (
            _schema(
                BASE.fields[0],
                _f(2, "name", StringType(), required=True),
                BASE.fields[2],
            ),
            [(ChangeKind.TIGHTEN_REQUIRED, Verdict.BLOCKER)],
        ),
        (
            _schema(
                BASE.fields[0],
                _f(2, "name", ListType(5, StringType())),
                BASE.fields[2],
            ),
            [(ChangeKind.CONTAINER_CHANGE, Verdict.BLOCKER)],
        ),
    ],
)
def test_change_matrix(
    after: Schema, expected: list[tuple[ChangeKind, Verdict]]
) -> None:
    assert _kinds(BASE, after) == expected


def test_narrowing_and_float_promotion() -> None:
    wide = _schema(_f(1, "x", LongType()), _f(2, "y", FloatType()))
    narrow = _schema(_f(1, "x", IntegerType()), _f(2, "y", DoubleType()))
    assert _kinds(wide, narrow) == [
        (ChangeKind.NARROW_TYPE, Verdict.BLOCKER),
        (ChangeKind.PROMOTE_TYPE, Verdict.SAFE),
    ]
    assert _kinds(narrow, wide)[1] == (ChangeKind.NARROW_TYPE, Verdict.BLOCKER)
    date = _schema(_f(1, "d", DateType()))
    ts = _schema(_f(1, "d", TimestampType()))
    assert _kinds(date, ts) == [(ChangeKind.RETYPE, Verdict.BLOCKER)]


def test_rename_and_promote_same_column() -> None:
    after = _schema(_f(1, "code", LongType(), required=True), *BASE.fields[1:])
    assert _kinds(BASE, after) == [
        (ChangeKind.RENAME, Verdict.SAFE),
        (ChangeKind.PROMOTE_TYPE, Verdict.SAFE),
    ]


def test_relax_required_flag_degrades_and_publishes_optional() -> None:
    after = _schema(*BASE.fields, _f(4, "state", StringType(), required=True))
    assert _kinds(BASE, after, relax_required=True) == [
        (ChangeKind.ADD_REQUIRED, Verdict.DEGRADED)
    ]
    result = detect_schema_change(
        BASE, after, existing_schema_ids=frozenset({0, 1}), relax_required=True
    )
    assert result.changed
    assert result.new_schema_id == 2
    assert result.new_schema is not None
    assert result.new_schema.schema_id == 2
    assert not result.new_schema.find_field(4).required
    assert result.new_schema.find_field(1).required  # the others are preserved
    assert [c.column_id for c in result.degraded] == [4]
    assert result.blockers == ()


NESTED = _schema(
    _f(1, "id", IntegerType(), required=True),
    _f(
        2,
        "address",
        StructType(_f(3, "street", StringType()), _f(4, "number", IntegerType())),
    ),
    _f(5, "tags", ListType(6, StringType(), element_required=False)),
    _f(7, "attrs", MapType(8, StringType(), 9, IntegerType(), value_required=False)),
)


def test_nested_changes_by_field_id() -> None:
    after = _schema(
        NESTED.fields[0],
        _f(
            2,
            "address",
            StructType(
                _f(4, "num", LongType()),
                _f(3, "street", StringType()),
                _f(10, "zip", StringType()),
            ),
        ),
        _f(5, "tags", ListType(6, StringType(), element_required=False)),
        _f(7, "attrs", MapType(8, StringType(), 9, LongType(), value_required=False)),
    )
    changes = diff_schemas(NESTED, after)
    summary = [(c.kind, c.column_id, c.parent_column) for c in changes]
    assert (ChangeKind.RENAME, 4, 2) in summary
    assert (ChangeKind.PROMOTE_TYPE, 4, 2) in summary
    assert (ChangeKind.PROMOTE_TYPE, 9, 7) in summary  # map value
    assert (ChangeKind.ADD_OPTIONAL, 10, 2) in summary
    assert (ChangeKind.REORDER, 3, 2) in summary
    assert all(c.verdict is Verdict.SAFE for c in changes)


def test_nested_drop_reports_only_the_parent() -> None:
    after = _schema(NESTED.fields[0], NESTED.fields[2], NESTED.fields[3])
    assert [(c.kind, c.column_id) for c in diff_schemas(NESTED, after)] == [
        (ChangeKind.DROP, 2)
    ]
    added = _schema(*after.fields, _f(20, "s", StructType(_f(21, "a", StringType()))))
    assert [(c.kind, c.column_id) for c in diff_schemas(after, added)] == [
        (ChangeKind.ADD_OPTIONAL, 20)
    ]


def test_map_key_change_is_container_change() -> None:
    after = _schema(
        *NESTED.fields[:3],
        _f(
            7,
            "attrs",
            MapType(8, IntegerType(), 9, IntegerType(), value_required=False),
        ),
    )
    assert _kinds(NESTED, after) == [(ChangeKind.CONTAINER_CHANGE, Verdict.BLOCKER)]


def test_field_moved_between_structs_is_container_change() -> None:
    after = _schema(
        NESTED.fields[0],
        _f(2, "address", StructType(_f(4, "number", IntegerType()))),
        NESTED.fields[2],
        _f(7, "attrs", StructType(_f(3, "street", StringType()))),
    )
    kinds = _kinds(NESTED, after)
    assert (ChangeKind.CONTAINER_CHANGE, Verdict.BLOCKER) in kinds


def test_nested_rebuild_keeps_containers() -> None:
    after = _schema(*NESTED.fields, _f(11, "notes", StringType(), required=True))
    result = detect_schema_change(NESTED, after, relax_required=True)
    assert result.new_schema is not None
    assert result.new_schema.find_field(11).required is False
    assert result.new_schema.find_type(5) == NESTED.find_type(5)
    assert result.new_schema.find_type(7) == NESTED.find_type(7)
    assert result.new_schema.find_type(2) == NESTED.find_type(2)


def test_detect_blocker_raises_with_changes() -> None:
    after = _schema(_f(1, "id", StringType(), required=True), *BASE.fields[1:])
    with pytest.raises(SchemaBlockerError) as exc:
        detect_schema_change(BASE, after)
    assert exc.value.exit_code == 2
    assert [c.kind for c in exc.value.changes] == [ChangeKind.RETYPE]
    assert exc.value.hint is not None


def test_first_sync_builds_schema() -> None:
    result = detect_schema_change(None, BASE)
    assert result.changed
    assert result.new_schema_id == 0
    assert result.changes == ()
    assert result.new_schema is not None
    assert result.new_schema.as_struct() == BASE.as_struct()


def test_watermark_detects_reused_id() -> None:
    dropped = _schema(*BASE.fields[:2])  # id 3 removed
    readded = _schema(*dropped.fields, _f(3, "other", StringType()))
    assert check_id_watermark(dropped, readded, last_column_id=3)[0].column_id == 3
    assert check_id_watermark(dropped, readded, last_column_id=None) == ()
    assert check_id_watermark(None, BASE, last_column_id=5) != ()
    with pytest.raises(IDReuseError, match="watermark"):
        detect_schema_change(dropped, readded, last_column_id=3)


def test_layer1_findings_block() -> None:
    after = _schema(*BASE.fields, _f(4, "state", StringType()))
    fake = check_id_watermark(BASE, after, last_column_id=9)
    with pytest.raises(IDReuseError):
        detect_schema_change(BASE, after, reuse=fake)


# -- integration: real DuckLake catalog ------------------------------------


def _schema_at(lake: CatalogHarness, snapshot: int) -> Schema:
    with open_catalog(lake.settings()) as con:
        ((table_id,),) = con.raw.execute(
            f"SELECT table_id FROM {con.meta}.ducklake_table WHERE table_name = 't'"
        ).fetchall()
        columns = read_columns(con.raw, con.meta, int(table_id), snapshot)
        return build_iceberg_schema(columns, schema_id=0, last_column_id=0).schema


@pytest.mark.integration
def test_real_catalog_rename_and_alter_are_safe_and_contiguous(
    lake: CatalogHarness,
) -> None:
    lake.write("CREATE TABLE lake.main.t (id INTEGER, v VARCHAR)")  # S1
    lake.write("INSERT INTO lake.main.t VALUES (1, 'a')")  # S2
    lake.write("ALTER TABLE lake.main.t RENAME COLUMN v TO w")  # S3
    lake.write("ALTER TABLE lake.main.t ALTER COLUMN id TYPE BIGINT")  # S4
    lake.write("ALTER TABLE lake.main.t ADD COLUMN x SMALLINT")  # S5

    before, after = _schema_at(lake, 2), _schema_at(lake, 5)
    result = detect_schema_change(
        before, after, existing_schema_ids=frozenset({0}), last_column_id=2
    )
    assert sorted(c.kind for c in result.changes) == sorted(
        [ChangeKind.RENAME, ChangeKind.PROMOTE_TYPE, ChangeKind.ADD_OPTIONAL]
    )
    assert result.new_schema_id == 1

    with open_catalog(lake.settings()) as con:
        ((table_id,),) = con.raw.execute(
            f"SELECT table_id FROM {con.meta}.ducklake_table WHERE table_name = 't'"
        ).fetchall()
        assert detect_id_reuse(con.raw, con.meta, int(table_id)) == ()

    # forge a gap in the interval of column_id 2 (died at S3, reborn at S4)
    lake.meta_exec(
        "UPDATE ducklake_column SET begin_snapshot = 4 "
        "WHERE column_id = 2 AND begin_snapshot = 3"
    )
    with open_catalog(lake.settings()) as con:
        found = detect_id_reuse(con.raw, con.meta, int(table_id))
    assert [(c.kind, c.column_id) for c in found] == [
        (ChangeKind.ID_REUSE_SUSPECTED, 2)
    ]
    with pytest.raises(IDReuseError, match="discontinuous"):
        detect_schema_change(before, after, reuse=found)
