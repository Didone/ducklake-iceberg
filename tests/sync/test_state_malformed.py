"""A malformed property never becomes "absent state"."""

from __future__ import annotations

import pytest

from ducklake_iceberg.sync.state import (
    PROP_DATA_PATH,
    PROP_ICEBERG_SNAPSHOT_ID,
    PROP_SNAPSHOT_ID,
    PROP_STATE_VERSION,
    PROP_SYNC_MODE,
    PROP_SYNCED_AT,
    PROP_TABLE_ID,
    PROP_TABLE_UUID,
    DuckLakeIdentity,
    malformed_properties,
    parse_sync_state,
    validate_identity,
)

IDENTITY = DuckLakeIdentity(
    table_id=42,
    table_uuid="550e8400-e29b-41d4-a716-446655440000",
    schema_name="s",
    table_name="t",
    data_path="s3://b/d/",
    partition_id=-1,
    last_column_id=3,
)

GOOD = {
    PROP_STATE_VERSION: "1",
    PROP_SNAPSHOT_ID: "7",
    PROP_TABLE_ID: "42",
    PROP_TABLE_UUID: IDENTITY.table_uuid,
    PROP_DATA_PATH: IDENTITY.data_path,
    PROP_ICEBERG_SNAPSHOT_ID: "123",
    PROP_SYNC_MODE: "full",
    PROP_SYNCED_AT: "2026-03-04T09:15:02.000000Z",
}


def test_malformed_snapshot_id_is_not_absent() -> None:
    state = parse_sync_state({**GOOD, PROP_SNAPSHOT_ID: "12a"})
    assert state.snapshot_id is None
    assert not state.is_absent
    mismatches = validate_identity(state, IDENTITY)
    assert [(m.code, m.property, m.found) for m in mismatches] == [
        ("MALFORMED", PROP_SNAPSHOT_ID, "12a")
    ]


def test_malformed_snapshot_id_alone() -> None:
    state = parse_sync_state({PROP_SNAPSHOT_ID: ""})
    assert not state.is_absent
    assert validate_identity(state, IDENTITY)[0].code == "MALFORMED"


@pytest.mark.parametrize("value", [" 7", "7 ", "+7", "1_000", "7.0", "0x7"])
def test_int_parse_is_strict(value: str) -> None:
    state = parse_sync_state({**GOOD, PROP_TABLE_ID: value})
    assert state.table_id is None
    assert [m.property for m in malformed_properties(state)] == [PROP_TABLE_ID]


@pytest.mark.parametrize(
    ("key", "value"),
    [
        (PROP_SYNCED_AT, "yesterday"),
        (PROP_SYNCED_AT, ""),
        (PROP_SYNC_MODE, "partial"),
        (PROP_TABLE_UUID, ""),
        (PROP_DATA_PATH, ""),
        (PROP_ICEBERG_SNAPSHOT_ID, "None"),
    ],
)
def test_other_malformed_keys(key: str, value: str) -> None:
    state = parse_sync_state({**GOOD, key: value})
    assert [m.property for m in malformed_properties(state)] == [key]
    assert validate_identity(state, IDENTITY)[0].code == "MALFORMED"


def test_good_state_has_no_mismatch() -> None:
    state = parse_sync_state(GOOD)
    assert not state.is_absent
    assert state.snapshot_id == 7
    assert malformed_properties(state) == []
    assert validate_identity(state, IDENTITY) == []


def test_negative_int_accepted() -> None:
    state = parse_sync_state({**GOOD, PROP_SNAPSHOT_ID: "-1"})
    assert state.snapshot_id == -1
    assert malformed_properties(state) == []


def test_absent_state() -> None:
    state = parse_sync_state({"write.format.default": "parquet"})
    assert state.is_absent
    assert malformed_properties(state) == []
