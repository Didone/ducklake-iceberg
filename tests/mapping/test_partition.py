"""Partition mapping tests (pure, no catalog).

Covers the pure scenarios. Scenarios that depend on the export orchestration
(--strict-partition) or on the integration/Athena matrix live elsewhere.
"""

from __future__ import annotations

import random

import pytest
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    IdentityTransform,
    MonthTransform,
    YearTransform,
)
from pyiceberg.types import (
    DateType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
)

from ducklake_iceberg.doctor.model import Severity
from ducklake_iceberg.mapping.partition import (
    PARTITION_FIELD_ID_START,
    UNPARTITIONED_SPEC_ID,
    DuckLakePartitionColumn,
    DuckLakePartitionInfo,
    PartitionSpecError,
    PartitionValueError,
    build_partition_record,
    build_spec_mapping,
    map_transform,
)


def _schema() -> Schema:
    return Schema(
        NestedField(2, "dt", DateType(), required=False),
        NestedField(3, "state", StringType(), required=False),
        NestedField(4, "n", IntegerType(), required=False),
        NestedField(5, "ts", TimestampType(), required=False),
        schema_id=0,
    )


def _col(
    partition_id: int, key_index: int, column_id: int, transform: str
) -> DuckLakePartitionColumn:
    return DuckLakePartitionColumn(
        partition_id=partition_id,
        partition_key_index=key_index,
        column_id=column_id,
        transform=transform,
    )


def _info(
    partition_id: int,
    columns: tuple[DuckLakePartitionColumn, ...],
    *,
    begin: int = 1,
    end: int | None = None,
) -> DuckLakePartitionInfo:
    return DuckLakePartitionInfo(
        partition_id=partition_id,
        begin_snapshot=begin,
        end_snapshot=end,
        columns=columns,
    )


# -- spec [epoch_day(dt)] ----------------------------------------------


def test_single_epoch_day_spec() -> None:
    info = _info(5, (_col(5, 0, 2, "epoch_day"),))
    mapping = build_spec_mapping([info], _schema(), snapshot_id=10)
    spec = mapping.specs[5]
    assert len(spec.fields) == 1
    field = spec.fields[0]
    assert field.source_id == 2
    assert field.field_id == 1000
    assert isinstance(field.transform, DayTransform)


# -- spec [bucket(16, state), epoch_day(dt)] ------------------------------


def test_bucket_and_epoch_day_field_ids() -> None:
    info = _info(
        7,
        (_col(7, 0, 3, "bucket(16, state)"), _col(7, 1, 2, "epoch_day")),
    )
    mapping = build_spec_mapping([info], _schema(), snapshot_id=10)
    spec = mapping.specs[7]
    assert [f.field_id for f in spec.fields] == [1000, 1001]
    assert isinstance(spec.fields[0].transform, BucketTransform)
    assert spec.fields[0].transform.num_buckets == 16
    assert isinstance(spec.fields[1].transform, DayTransform)


# -- partition_type of spec 1 ------------------------------------------


def test_partition_type_is_date() -> None:
    info = _info(5, (_col(5, 0, 2, "epoch_day"),))
    mapping = build_spec_mapping([info], _schema(), snapshot_id=10)
    struct = mapping.specs[5].partition_type(_schema())
    assert len(struct.fields) == 1
    assert isinstance(struct.fields[0].field_type, DateType)
    assert struct.fields[0].field_id == 1000


# -- epoch_day of 2023-07-15 -------------------------------------------


def test_epoch_day_value_passthrough() -> None:
    info = _info(5, (_col(5, 0, 2, "epoch_day"),))
    mapping = build_spec_mapping([info], _schema(), snapshot_id=10)
    spec = mapping.specs[5]
    rec = build_partition_record(spec, _schema(), {1000: "19553"})
    assert tuple(rec[i] for i in range(len(rec))) == (19553,)


# -- spec [day(dt)] (calendar) degrades -------------------------------


def test_calendar_day_degrades() -> None:
    info = _info(5, (_col(5, 0, 2, "day"),))
    mapping = build_spec_mapping([info], _schema(), snapshot_id=10)
    assert mapping.default_spec_id == UNPARTITIONED_SPEC_ID
    assert 5 not in mapping.specs  # collapsed into the empty one
    assert mapping.spec_id_by_partition_id[5] == UNPARTITIONED_SPEC_ID
    assert len(mapping.degraded) == 1
    f = mapping.degraded[0]
    assert f.severity is Severity.DEGRADED
    assert "epoch_" in f.remediation


# -- regression: year does NOT map ---------------------------------------


def test_year_calendar_not_mapped() -> None:
    assert map_transform("year") is None
    assert map_transform("month") is None
    assert map_transform("day") is None
    assert map_transform("hour") is None


def test_epoch_transforms_map() -> None:
    assert isinstance(map_transform("epoch_year"), YearTransform)
    assert isinstance(map_transform("epoch_month"), MonthTransform)
    assert isinstance(map_transform("epoch_day"), DayTransform)
    assert isinstance(map_transform("identity"), IdentityTransform)


# -- unknown transform degrades ------------------------------------


def test_unknown_transform_degrades_with_literal() -> None:
    info = _info(5, (_col(5, 0, 2, "foo(x)"),))
    mapping = build_spec_mapping([info], _schema(), snapshot_id=10)
    assert mapping.default_spec_id == UNPARTITIONED_SPEC_ID
    assert any("foo(x)" in f.detail for f in mapping.degraded)


# -- never-partitioned table -----------------------------------------


def test_unpartitioned_table() -> None:
    mapping = build_spec_mapping([], _schema(), snapshot_id=10)
    assert set(mapping.specs) == {0}
    assert mapping.specs[0].spec_id == 0
    assert len(mapping.specs[0].fields) == 0
    assert mapping.default_spec_id == 0
    assert mapping.last_partition_id == PARTITION_FIELD_ID_START - 1 == 999


# -- field_id determinism ----------------------------------------


def test_field_ids_deterministic_under_shuffle() -> None:
    cols = [
        _col(5, 0, 2, "epoch_day"),
        _col(5, 1, 3, "bucket(16)"),
        _col(8, 0, 4, "identity"),
    ]
    info_a = [
        _info(5, (cols[0], cols[1]), begin=1, end=50),
        _info(8, (cols[2],), begin=50, end=None),
    ]
    base = build_spec_mapping(info_a, _schema(), snapshot_id=100)

    # shuffle the order of the input rows and of the columns
    rng = random.Random(1234)  # noqa: S311 - test shuffling, not crypto
    shuffled_cols_5 = [cols[1], cols[0]]
    rng.shuffle(shuffled_cols_5)
    info_b = [
        _info(8, (cols[2],), begin=50, end=None),
        _info(5, tuple(shuffled_cols_5), begin=1, end=50),
    ]
    other = build_spec_mapping(info_b, _schema(), snapshot_id=100)
    assert dict(base.field_ids) == dict(other.field_ids)
    assert base.field_ids == {(5, 0): 1000, (5, 1): 1001, (8, 0): 1002}


# -- a new spec after a degraded one does not shift IDs -----------------------------


def test_degraded_spec_counts_in_ordinal() -> None:
    # spec 5 fully degraded (calendar), spec 8 mappable
    degraded = _info(5, (_col(5, 0, 2, "day"), _col(5, 1, 3, "month")), begin=1, end=50)
    good = _info(8, (_col(8, 0, 4, "identity"),), begin=50, end=None)
    mapping = build_spec_mapping([degraded, good], _schema(), snapshot_id=100)
    # the ordinal counts degraded rows: (5,0)->1000, (5,1)->1001, (8,0)->1002
    assert mapping.field_ids[(8, 0)] == 1002
    assert mapping.specs[8].fields[0].field_id == 1002


# -- three specs over time -------------------------------------


def test_three_specs_over_time() -> None:
    s1 = _info(5, (_col(5, 0, 2, "epoch_day"),), begin=1, end=10)
    s2 = _info(8, (_col(8, 0, 3, "bucket(16)"),), begin=10, end=20)
    s3 = _info(12, (_col(12, 0, 4, "identity"),), begin=20, end=None)
    mapping = build_spec_mapping([s1, s2, s3], _schema(), snapshot_id=25)
    assert set(mapping.specs) == {0, 5, 8, 12}
    assert mapping.default_spec_id == 12  # in effect at snapshot 25


def test_historic_spec_default_is_current() -> None:
    s1 = _info(5, (_col(5, 0, 2, "epoch_day"),), begin=1, end=10)
    s2 = _info(8, (_col(8, 0, 3, "bucket(16)"),), begin=10, end=None)
    mapping = build_spec_mapping([s1, s2], _schema(), snapshot_id=5)
    assert mapping.default_spec_id == 5  # snapshot 5 falls into spec 5


# -- file older than partitioning ------------------------------


def test_unpartitioned_record_is_empty() -> None:
    mapping = build_spec_mapping([], _schema(), snapshot_id=10)
    rec = build_partition_record(mapping.specs[0], _schema(), {})
    assert tuple(rec[i] for i in range(len(rec))) == ()


# -- NULL partition_value ---------------------------------------------


def test_null_partition_value() -> None:
    info = _info(5, (_col(5, 0, 2, "epoch_day"),))
    mapping = build_spec_mapping([info], _schema(), snapshot_id=10)
    rec = build_partition_record(mapping.specs[5], _schema(), {1000: None})
    assert tuple(rec[i] for i in range(len(rec))) == (None,)


# -- non-integer value in an int field -----------------------------------


def test_invalid_int_value_raises() -> None:
    info = _info(5, (_col(5, 0, 4, "identity"),))  # n: IntegerType
    mapping = build_spec_mapping([info], _schema(), snapshot_id=10)
    with pytest.raises(PartitionValueError):
        build_partition_record(mapping.specs[5], _schema(), {1000: "abc"})


# -- bucket out of range ---------------------------------------------


def test_bucket_out_of_range_raises() -> None:
    info = _info(5, (_col(5, 0, 3, "bucket(16)"),))
    mapping = build_spec_mapping([info], _schema(), snapshot_id=10)
    with pytest.raises(PartitionValueError):
        build_partition_record(mapping.specs[5], _schema(), {1000: "16"})


def test_bucket_in_range_ok() -> None:
    info = _info(5, (_col(5, 0, 3, "bucket(16)"),))
    mapping = build_spec_mapping([info], _schema(), snapshot_id=10)
    rec = build_partition_record(mapping.specs[5], _schema(), {1000: "15"})
    assert tuple(rec[i] for i in range(len(rec))) == (15,)


# -- identity on date, ISO value -----------------------------------


def test_identity_date_iso_converted_to_days() -> None:
    info = _info(5, (_col(5, 0, 2, "identity"),))  # dt: DateType
    mapping = build_spec_mapping([info], _schema(), snapshot_id=10)
    rec = build_partition_record(mapping.specs[5], _schema(), {1000: "2024-01-05"})
    assert tuple(rec[i] for i in range(len(rec))) == (19727,)


# -- extras: spec edge cases ------------------------------------------------


def test_bucket_zero_is_spec_error() -> None:
    with pytest.raises(PartitionSpecError):
        map_transform("bucket(0)")


def test_source_id_absent_from_schema_raises() -> None:
    info = _info(5, (_col(5, 0, 99, "epoch_day"),))  # column_id 99 does not exist
    with pytest.raises(PartitionSpecError):
        build_spec_mapping([info], _schema(), snapshot_id=10)


def test_two_live_specs_same_snapshot_raises() -> None:
    s1 = _info(5, (_col(5, 0, 2, "epoch_day"),), begin=1, end=None)
    s2 = _info(8, (_col(8, 0, 3, "bucket(16)"),), begin=1, end=None)
    with pytest.raises(PartitionSpecError):
        build_spec_mapping([s1, s2], _schema(), snapshot_id=5)


def test_empty_columns_spec_degrades() -> None:
    info = _info(5, ())
    mapping = build_spec_mapping([info], _schema(), snapshot_id=10)
    assert 5 not in mapping.specs
    assert mapping.default_spec_id == 0
    assert len(mapping.degraded) == 1


def test_partial_degrade_disabled_degrades_whole_spec() -> None:
    info = _info(5, (_col(5, 0, 2, "epoch_day"), _col(5, 1, 3, "day")))
    mapping = build_spec_mapping([info], _schema(), snapshot_id=10)
    # one mappable field, one degraded -> whole spec degraded (default)
    assert 5 not in mapping.specs
    assert mapping.default_spec_id == 0


def test_partial_degrade_enabled_keeps_mapped_subset() -> None:
    info = _info(5, (_col(5, 0, 2, "epoch_day"), _col(5, 1, 3, "day")))
    mapping = build_spec_mapping(
        [info], _schema(), snapshot_id=10, partial_degrade=True
    )
    assert 5 in mapping.specs
    spec = mapping.specs[5]
    assert len(spec.fields) == 1  # only epoch_day survives
    assert spec.fields[0].field_id == 1000


def test_identity_bucket_counts_ordinal_globally() -> None:
    s1 = _info(5, (_col(5, 0, 2, "epoch_day"),), begin=1, end=50)
    s2 = _info(6, (_col(6, 0, 3, "bucket(8)"),), begin=50, end=None)
    mapping = build_spec_mapping([s1, s2], _schema(), snapshot_id=100)
    assert mapping.field_ids == {(5, 0): 1000, (6, 0): 1001}
    assert mapping.last_partition_id == 1001


def test_timestamp_identity_iso_to_micros() -> None:
    info = _info(5, (_col(5, 0, 5, "identity"),))  # ts: TimestampType
    mapping = build_spec_mapping([info], _schema(), snapshot_id=10)
    rec = build_partition_record(
        mapping.specs[5], _schema(), {1000: "1970-01-02T00:00:00"}
    )
    assert tuple(rec[i] for i in range(len(rec))) == (86_400_000_000,)


# -- partition field name following the Iceberg convention -----------------------


def _named(
    partition_id: int,
    key_index: int,
    column_id: int,
    transform: str,
    name: str | None,
) -> DuckLakePartitionColumn:
    return DuckLakePartitionColumn(
        partition_id=partition_id,
        partition_key_index=key_index,
        column_id=column_id,
        transform=transform,
        column_name=name,
    )


def _names(columns: tuple[DuckLakePartitionColumn, ...]) -> list[str]:
    mapping = build_spec_mapping([_info(5, columns)], _schema(), snapshot_id=10)
    return [field.name for field in mapping.specs[5].fields]


def test_standard_names_for_time_bucket_and_identity() -> None:
    names = _names(
        (
            _named(5, 0, 2, "epoch_day", "dt"),
            _named(5, 1, 3, "bucket(16, state)", "state"),
            _named(5, 2, 4, "identity", "n"),
            _named(5, 3, 5, "epoch_hour", "ts"),
        )
    )
    assert names == ["dt_day", "state_bucket_16", "n", "ts_hour"]


def test_month_and_year_names() -> None:
    names = _names(
        (_named(5, 0, 2, "epoch_month", "dt"), _named(5, 1, 5, "epoch_year", "ts"))
    )
    assert names == ["dt_month", "ts_year"]


def test_name_colliding_with_schema_column_falls_back() -> None:
    schema = Schema(
        NestedField(2, "dt", DateType(), required=False),
        NestedField(3, "dt_day", IntegerType(), required=False),
        schema_id=0,
    )
    info = _info(5, (_named(5, 0, 2, "epoch_day", "dt"),))
    mapping = build_spec_mapping([info], schema, snapshot_id=10)
    assert mapping.specs[5].fields[0].name == "c2_day"


def test_identity_on_renamed_column_colliding_with_other_column() -> None:
    """Identity may only repeat the name of its **own** source column."""
    schema = Schema(
        NestedField(2, "status", StringType(), required=False),  # new column
        NestedField(3, "old_status", StringType(), required=False),  # renamed
        schema_id=0,
    )
    info = _info(5, (_named(5, 0, 3, "identity", "status"),))
    mapping = build_spec_mapping([info], schema, snapshot_id=10)
    assert mapping.specs[5].fields[0].name == "c3_identity"


def test_duplicate_name_within_spec_falls_back() -> None:
    names = _names(
        (
            _named(5, 0, 3, "bucket(16, state)", "state"),
            _named(5, 1, 3, "bucket(16, state)", "state"),
        )
    )
    assert names == ["state_bucket_16", "c3_bucket"]


def test_unknown_creation_name_keeps_synthetic_name() -> None:
    assert _names((_col(5, 0, 2, "epoch_day"),)) == ["c2_day"]


def test_published_names_are_kept_for_existing_specs() -> None:
    from pyiceberg.partitioning import PartitionField, PartitionSpec

    from ducklake_iceberg.mapping.partition import with_published_names

    old = _info(5, (_named(5, 0, 2, "epoch_day", "dt"),), begin=1, end=50)
    new = _info(6, (_named(6, 0, 3, "bucket(8)", "state"),), begin=50)
    mapping = build_spec_mapping([old, new], _schema(), snapshot_id=100)
    published = {
        5: PartitionSpec(
            PartitionField(
                source_id=2, field_id=1000, transform=DayTransform(), name="c2_day"
            ),
            spec_id=5,
        )
    }

    kept = with_published_names(mapping, published)

    assert kept.specs[5].fields[0].name == "c2_day"
    assert kept.specs[6].fields[0].name == "state_bucket_8"
    assert kept.default_spec_id == mapping.default_spec_id
    # a different published spec (another transform) does not inherit the name
    other = {
        5: PartitionSpec(
            PartitionField(
                source_id=2, field_id=1000, transform=MonthTransform(), name="x"
            ),
            spec_id=5,
        )
    }
    assert with_published_names(mapping, other).specs[5].fields[0].name == "dt_day"
    assert with_published_names(mapping, {}) is mapping
