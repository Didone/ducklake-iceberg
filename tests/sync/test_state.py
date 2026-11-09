"""Tests for the sync state.

Coverage:

- Round-trip of build_sync_properties -> parse_sync_state
- Timestamp normalization to UTC
- Unpartitioned table (partition_id = -1)
- Preservation of unmanaged properties
- Removal of orphan properties
- Detection of unmanaged table (snapshots without properties)
- Malformed properties
- table_uuid mismatch
- Mirroring in the summary
"""

from __future__ import annotations

from datetime import UTC, datetime, timezone

from iceduck.sync.state import (
    PROP_DATA_PATH,
    PROP_ICEBERG_SNAPSHOT_ID,
    PROP_LAST_COLUMN_ID,
    PROP_PARTITION_ID,
    PROP_SCHEMA_VERSION,
    PROP_SNAPSHOT_ID,
    PROP_SNAPSHOT_TIME,
    PROP_STATE_VERSION,
    PROP_SYNC_MODE,
    PROP_SYNCED_AT,
    PROP_TABLE_ID,
    PROP_TABLE_UUID,
    PROP_VERSION,
    DuckLakeIdentity,
    DuckLakeSnapshot,
    SyncMode,
    SyncState,
    build_summary_properties,
    build_sync_properties,
    parse_sync_state,
    validate_identity,
)
from iceduck.writer.metadata import merge_properties


def test_build_sync_properties_round_trip() -> None:
    """Exact round-trip of all fields; all values are str."""
    identity = DuckLakeIdentity(
        table_id=42,
        table_uuid="550e8400-e29b-41d4-a716-446655440000",
        schema_name="sales",
        table_name="orders",
        data_path="s3://bucket/ducklake/data",
        partition_id=100,
        last_column_id=10,
    )
    snapshot = DuckLakeSnapshot(
        snapshot_id=123,
        snapshot_time=datetime(2024, 10, 27, 12, 30, 45, 123456, tzinfo=UTC),
        schema_version=5,
    )
    now = datetime(2024, 10, 27, 14, 0, 0, 0, tzinfo=UTC)

    props = build_sync_properties(
        identity,
        snapshot,
        mode=SyncMode.INCREMENTAL,
        iceberg_snapshot_id=999,
        now=now,
        tool_version="0.1.0",
    )

    # All values must be strings
    assert all(isinstance(v, str) for v in props.values())

    # Expected values
    assert props[PROP_STATE_VERSION] == "1"
    assert props[PROP_SNAPSHOT_ID] == "123"
    assert props[PROP_SNAPSHOT_TIME] == "2024-10-27T12:30:45.123456+00:00"
    assert props[PROP_SCHEMA_VERSION] == "5"
    assert props[PROP_TABLE_ID] == "42"
    assert props[PROP_TABLE_UUID] == "550e8400-e29b-41d4-a716-446655440000"
    assert props[PROP_LAST_COLUMN_ID] == "10"
    assert props[PROP_PARTITION_ID] == "100"
    assert props[PROP_DATA_PATH] == "s3://bucket/ducklake/data"
    assert props[PROP_ICEBERG_SNAPSHOT_ID] == "999"
    assert props[PROP_SYNC_MODE] == "incremental"
    assert props[PROP_SYNCED_AT] == "2024-10-27T14:00:00.000000+00:00"
    assert props[PROP_VERSION] == "0.1.0"

    # Round-trip
    state = parse_sync_state(props)
    assert state.state_version == 1
    assert state.snapshot_id == 123
    assert state.snapshot_time == snapshot.snapshot_time
    assert state.schema_version == 5
    assert state.table_id == 42
    assert state.table_uuid == "550e8400-e29b-41d4-a716-446655440000"
    assert state.last_column_id == 10
    assert state.partition_id == 100
    assert state.data_path == "s3://bucket/ducklake/data"
    assert state.iceberg_snapshot_id == 999
    assert state.sync_mode == SyncMode.INCREMENTAL
    assert state.synced_at == now
    assert state.iceduck_version == "0.1.0"
    assert not state.is_absent


def test_timestamp_non_utc_normalized() -> None:
    """Snapshot with snapshot_time in a non-UTC timezone."""
    identity = DuckLakeIdentity(
        table_id=1,
        table_uuid="550e8400-e29b-41d4-a716-446655440000",
        schema_name="s",
        table_name="t",
        data_path="s3://b/d",
        partition_id=-1,
        last_column_id=5,
    )
    # Timestamp in the UTC-3 time zone (America/Sao_Paulo)
    from datetime import timedelta

    tz_minus3 = timezone(timedelta(hours=-3))
    snapshot = DuckLakeSnapshot(
        snapshot_id=1,
        snapshot_time=datetime(2024, 10, 27, 12, 0, 0, 0, tzinfo=tz_minus3),
        schema_version=1,
    )
    now = datetime.now(tz=UTC)

    props = build_sync_properties(
        identity,
        snapshot,
        mode=SyncMode.FULL,
        iceberg_snapshot_id=1,
        now=now,
        tool_version="0.1.0",
    )

    # Property written in UTC with Z (15:00 UTC = 12:00 UTC-3)
    snapshot_time_str = props[PROP_SNAPSHOT_TIME]
    assert "+00:00" in snapshot_time_str or snapshot_time_str.endswith("Z")

    # Round-trip preserves the instant
    state = parse_sync_state(props)
    assert state.snapshot_time is not None
    # Compare instants (normalized to UTC)
    assert state.snapshot_time.astimezone(UTC) == snapshot.snapshot_time.astimezone(UTC)


def test_unpartitioned_table() -> None:
    """Unpartitioned table uses partition_id = -1, never omitted."""
    identity = DuckLakeIdentity(
        table_id=1,
        table_uuid="550e8400-e29b-41d4-a716-446655440000",
        schema_name="s",
        table_name="t",
        data_path="s3://b/d",
        partition_id=-1,  # unpartitioned
        last_column_id=5,
    )
    snapshot = DuckLakeSnapshot(
        snapshot_id=1,
        snapshot_time=datetime.now(tz=UTC),
        schema_version=1,
    )

    props = build_sync_properties(
        identity,
        snapshot,
        mode=SyncMode.FULL,
        iceberg_snapshot_id=1,
        now=datetime.now(tz=UTC),
        tool_version="0.1.0",
    )

    # partition_id must be present as "-1"
    assert PROP_PARTITION_ID in props
    assert props[PROP_PARTITION_ID] == "-1"

    state = parse_sync_state(props)
    assert state.partition_id == -1


def test_merge_properties_preserves_unmanaged() -> None:
    """User properties (write.*) are preserved after commit."""
    base = {
        "write.target-file-size-bytes": "134217728",
        "write.format.default": "parquet",
        "ducklake.snapshot-id": "100",  # orphan: will be removed
        "iceduck.version": "0.0.9",  # orphan: will be removed
        "custom.property": "user-value",
    }

    new_props = {
        "ducklake.snapshot-id": "200",
        "ducklake.table-id": "42",
        "iceduck.version": "0.1.0",
    }

    merged = merge_properties(base, new_props)

    # Unmanaged properties preserved
    assert merged["write.target-file-size-bytes"] == "134217728"
    assert merged["write.format.default"] == "parquet"
    assert merged["custom.property"] == "user-value"

    # Managed properties updated
    assert merged["ducklake.snapshot-id"] == "200"
    assert merged["ducklake.table-id"] == "42"
    assert merged["iceduck.version"] == "0.1.0"


def test_merge_properties_removes_orphaned() -> None:
    """A stale managed key is removed by the commit."""
    base = {
        "ducklake.snapshot-id": "100",
        "ducklake.old-property": "obsolete",  # orphan
        "write.format.default": "parquet",
    }

    new_props = {
        "ducklake.snapshot-id": "200",
        "iceduck.version": "0.1.0",
    }

    merged = merge_properties(base, new_props)

    # Orphan property removed
    assert "ducklake.old-property" not in merged
    # Unmanaged ones preserved
    assert merged["write.format.default"] == "parquet"
    # New managed ones present
    assert merged["ducklake.snapshot-id"] == "200"
    assert merged["iceduck.version"] == "0.1.0"


def test_parse_sync_state_absent() -> None:
    """State is absent when no managed property is present."""
    state = parse_sync_state({})
    assert state.is_absent
    assert state.snapshot_id is None
    assert state.table_uuid is None

    # Only unmanaged properties
    state2 = parse_sync_state({"write.format.default": "parquet"})
    assert state2.is_absent


def test_parse_sync_state_malformed_int() -> None:
    """ducklake.snapshot-id with a non-numeric value."""
    props = {
        PROP_SNAPSHOT_ID: "12a",  # malformed
        PROP_TABLE_UUID: "550e8400-e29b-41d4-a716-446655440000",
        PROP_TABLE_ID: "42",
    }

    state = parse_sync_state(props)
    # Parsing returns None for malformed values
    assert state.snapshot_id is None
    # Raw keeps the original value
    assert state.raw[PROP_SNAPSHOT_ID] == "12a"


def test_validate_identity_malformed() -> None:
    """Validation detects a malformed property."""
    identity = DuckLakeIdentity(
        table_id=42,
        table_uuid="550e8400-e29b-41d4-a716-446655440000",
        schema_name="s",
        table_name="t",
        data_path="s3://b/d",
        partition_id=-1,
        last_column_id=10,
    )

    state = SyncState(
        state_version=None,  # malformed: present in raw but None here
        snapshot_id=123,
        snapshot_time=None,
        schema_version=None,
        table_id=None,  # malformed
        table_uuid="550e8400-e29b-41d4-a716-446655440000",
        last_column_id=None,
        partition_id=None,
        data_path=None,
        iceberg_snapshot_id=None,
        sync_mode=None,
        synced_at=None,
        iceduck_version=None,
        raw={
            PROP_STATE_VERSION: "abc",  # present but malformed
            PROP_TABLE_ID: "xyz",  # present but malformed
        },
    )

    mismatches = validate_identity(state, identity)
    assert len(mismatches) > 0
    assert any(m.code == "MALFORMED" for m in mismatches)


def test_validate_identity_state_version_ahead() -> None:
    """Validation detects a state version that is ahead."""
    identity = DuckLakeIdentity(
        table_id=42,
        table_uuid="550e8400-e29b-41d4-a716-446655440000",
        schema_name="s",
        table_name="t",
        data_path="s3://b/d",
        partition_id=-1,
        last_column_id=10,
    )

    state = SyncState(
        state_version=999,  # ahead of STATE_VERSION (1)
        snapshot_id=123,
        snapshot_time=datetime.now(tz=UTC),
        schema_version=1,
        table_id=42,
        table_uuid="550e8400-e29b-41d4-a716-446655440000",
        last_column_id=10,
        partition_id=-1,
        data_path="s3://b/d",
        iceberg_snapshot_id=999,
        sync_mode=SyncMode.INCREMENTAL,
        synced_at=datetime.now(tz=UTC),
        iceduck_version="0.1.0",
        raw={},
    )

    mismatches = validate_identity(state, identity)
    assert len(mismatches) == 1
    assert mismatches[0].code == "STATE_VERSION_AHEAD"
    assert mismatches[0].property == PROP_STATE_VERSION


def test_validate_identity_table_uuid_mismatch() -> None:
    """Catalog table_uuid differs from the published one."""
    identity = DuckLakeIdentity(
        table_id=42,
        table_uuid="550e8400-e29b-41d4-a716-446655440001",  # different
        schema_name="s",
        table_name="t",
        data_path="s3://b/d",
        partition_id=-1,
        last_column_id=10,
    )

    state = SyncState(
        state_version=1,
        snapshot_id=123,
        snapshot_time=datetime.now(tz=UTC),
        schema_version=1,
        table_id=42,
        table_uuid="550e8400-e29b-41d4-a716-446655440000",  # published
        last_column_id=10,
        partition_id=-1,
        data_path="s3://b/d",
        iceberg_snapshot_id=999,
        sync_mode=SyncMode.INCREMENTAL,
        synced_at=datetime.now(tz=UTC),
        iceduck_version="0.1.0",
        raw={},
    )

    mismatches = validate_identity(state, identity)
    assert len(mismatches) == 1
    assert mismatches[0].code == "IDENTITY_MISMATCH"
    assert mismatches[0].property == PROP_TABLE_UUID
    assert "table was recreated" in mismatches[0].detail


def test_validate_identity_table_id_mismatch() -> None:
    """table_id mismatch."""
    identity = DuckLakeIdentity(
        table_id=43,  # different
        table_uuid="550e8400-e29b-41d4-a716-446655440000",
        schema_name="s",
        table_name="t",
        data_path="s3://b/d",
        partition_id=-1,
        last_column_id=10,
    )

    state = SyncState(
        state_version=1,
        snapshot_id=123,
        snapshot_time=datetime.now(tz=UTC),
        schema_version=1,
        table_id=42,  # published
        table_uuid="550e8400-e29b-41d4-a716-446655440000",
        last_column_id=10,
        partition_id=-1,
        data_path="s3://b/d",
        iceberg_snapshot_id=999,
        sync_mode=SyncMode.INCREMENTAL,
        synced_at=datetime.now(tz=UTC),
        iceduck_version="0.1.0",
        raw={},
    )

    mismatches = validate_identity(state, identity)
    assert len(mismatches) == 1
    assert mismatches[0].code == "IDENTITY_MISMATCH"
    assert mismatches[0].property == PROP_TABLE_ID


def test_validate_identity_data_path_mismatch() -> None:
    """data_path mismatch."""
    identity = DuckLakeIdentity(
        table_id=42,
        table_uuid="550e8400-e29b-41d4-a716-446655440000",
        schema_name="s",
        table_name="t",
        data_path="s3://bucket/other-path",  # different
        partition_id=-1,
        last_column_id=10,
    )

    state = SyncState(
        state_version=1,
        snapshot_id=123,
        snapshot_time=datetime.now(tz=UTC),
        schema_version=1,
        table_id=42,
        table_uuid="550e8400-e29b-41d4-a716-446655440000",
        last_column_id=10,
        partition_id=-1,
        data_path="s3://b/d",  # published
        iceberg_snapshot_id=999,
        sync_mode=SyncMode.INCREMENTAL,
        synced_at=datetime.now(tz=UTC),
        iceduck_version="0.1.0",
        raw={},
    )

    mismatches = validate_identity(state, identity)
    assert len(mismatches) == 1
    assert mismatches[0].code == "IDENTITY_MISMATCH"
    assert mismatches[0].property == PROP_DATA_PATH


def test_validate_identity_ok() -> None:
    """Validation passes when everything is correct."""
    identity = DuckLakeIdentity(
        table_id=42,
        table_uuid="550e8400-e29b-41d4-a716-446655440000",
        schema_name="s",
        table_name="t",
        data_path="s3://b/d",
        partition_id=-1,
        last_column_id=10,
    )

    state = SyncState(
        state_version=1,
        snapshot_id=123,
        snapshot_time=datetime.now(tz=UTC),
        schema_version=1,
        table_id=42,
        table_uuid="550e8400-e29b-41d4-a716-446655440000",
        last_column_id=10,
        partition_id=-1,
        data_path="s3://b/d",
        iceberg_snapshot_id=999,
        sync_mode=SyncMode.INCREMENTAL,
        synced_at=datetime.now(tz=UTC),
        iceduck_version="0.1.0",
        raw={},
    )

    mismatches = validate_identity(state, identity)
    assert len(mismatches) == 0


def test_build_summary_properties() -> None:
    """Snapshot summary contains ducklake.*/iceduck.* properties."""
    identity = DuckLakeIdentity(
        table_id=42,
        table_uuid="550e8400-e29b-41d4-a716-446655440000",
        schema_name="s",
        table_name="t",
        data_path="s3://b/d",
        partition_id=-1,
        last_column_id=10,
    )
    snapshot = DuckLakeSnapshot(
        snapshot_id=123,
        snapshot_time=datetime.now(tz=UTC),
        schema_version=1,
    )

    props = build_sync_properties(
        identity,
        snapshot,
        mode=SyncMode.FULL,
        iceberg_snapshot_id=999,
        now=datetime.now(tz=UTC),
        tool_version="0.1.0",
    )

    counters = {
        "added-data-files": "10",
        "total-records": "1000",
    }

    summary_props = build_summary_properties(props, counters)

    # Must contain managed properties
    assert PROP_SNAPSHOT_ID in summary_props
    assert summary_props[PROP_SNAPSHOT_ID] == "123"

    # Must contain counters
    assert "added-data-files" in summary_props
    assert summary_props["added-data-files"] == "10"

    # Must not contain unmanaged properties
    # (build_summary_properties keeps only managed ones)
    props_with_unmanaged = {**props, "write.format.default": "parquet"}
    summary_props2 = build_summary_properties(props_with_unmanaged, counters)
    assert "write.format.default" not in summary_props2


def test_sync_mode_enum() -> None:
    """SyncMode enum has the correct values."""
    assert SyncMode.FULL.value == "full"
    assert SyncMode.INCREMENTAL.value == "incremental"

    # String conversion
    assert SyncMode("full") == SyncMode.FULL
    assert SyncMode("incremental") == SyncMode.INCREMENTAL
