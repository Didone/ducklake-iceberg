"""Sync state.

Defines where ``iceduck`` records "up to which DuckLake snapshot this Iceberg
table was published", in what format, and how that record is read back.
The state is stored as **properties of the Iceberg table itself**
(``ducklake.*``, ``iceduck.*``), mirrored in the ``summary`` of each snapshot,
so that the state update and the snapshot publication are **the same atomic
commit**.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Final

STATE_VERSION: Final[int] = 1

PROP_STATE_VERSION: Final = "iceduck.state-version"
PROP_SNAPSHOT_ID: Final = "ducklake.snapshot-id"
PROP_SNAPSHOT_TIME: Final = "ducklake.snapshot-time"
PROP_SCHEMA_VERSION: Final = "ducklake.schema-version"
PROP_TABLE_ID: Final = "ducklake.table-id"
PROP_TABLE_UUID: Final = "ducklake.table-uuid"
PROP_LAST_COLUMN_ID: Final = "ducklake.last-column-id"
PROP_PARTITION_ID: Final = "ducklake.partition-id"
PROP_DATA_PATH: Final = "ducklake.data-path"
PROP_ICEBERG_SNAPSHOT_ID: Final = "iceduck.iceberg-snapshot-id"
PROP_SYNC_MODE: Final = "iceduck.sync-mode"
PROP_SYNCED_AT: Final = "iceduck.synced-at"
PROP_VERSION: Final = "iceduck.version"

MANAGED_PREFIXES: Final = ("ducklake.", "iceduck.")

#: integer keys: strict decimal, no spaces, ``+`` or ``_``.
_INT_KEYS: Final = (
    PROP_STATE_VERSION,
    PROP_SNAPSHOT_ID,
    PROP_SCHEMA_VERSION,
    PROP_TABLE_ID,
    PROP_LAST_COLUMN_ID,
    PROP_PARTITION_ID,
    PROP_ICEBERG_SNAPSHOT_ID,
)
_DATETIME_KEYS: Final = (PROP_SNAPSHOT_TIME, PROP_SYNCED_AT)
_TEXT_KEYS: Final = (PROP_TABLE_UUID, PROP_DATA_PATH, PROP_VERSION)
_INT_RE: Final = re.compile(r"-?[0-9]+")


class SyncMode(StrEnum):
    """Sync mode: full or incremental."""

    FULL = "full"
    INCREMENTAL = "incremental"


@dataclass(frozen=True)
class DuckLakeIdentity:
    """Table identity resolved at the target snapshot."""

    table_id: int
    table_uuid: str
    schema_name: str
    table_name: str
    data_path: str
    partition_id: int  # -1 when unpartitioned
    last_column_id: int


@dataclass(frozen=True)
class DuckLakeSnapshot:
    """Exported DuckLake snapshot."""

    snapshot_id: int
    snapshot_time: datetime  # tz-aware, normalized to UTC
    schema_version: int  # the table's, not the global one


@dataclass(frozen=True)
class SyncState:
    """State read back from the published Iceberg table."""

    state_version: int | None
    snapshot_id: int | None
    snapshot_time: datetime | None
    schema_version: int | None
    table_id: int | None
    table_uuid: str | None
    last_column_id: int | None
    partition_id: int | None
    data_path: str | None
    iceberg_snapshot_id: int | None
    sync_mode: SyncMode | None
    synced_at: datetime | None
    iceduck_version: str | None
    raw: dict[str, str]

    @property
    def is_absent(self) -> bool:
        """True if no state was found (first sync).

        A present but unreadable ``ducklake.snapshot-id`` is **not** absence:
        it is ``MALFORMED`` (see ``malformed_properties``); otherwise a read
        error would turn into a silent full re-export.
        """
        return self.snapshot_id is None and PROP_SNAPSHOT_ID not in self.raw


@dataclass(frozen=True)
class StateMismatch:
    """Identity or format mismatch in the state that was read."""

    code: str  # IDENTITY_MISMATCH | STATE_VERSION_AHEAD | MALFORMED
    property: str
    expected: str | None
    found: str | None
    detail: str


def build_sync_properties(
    identity: DuckLakeIdentity,
    snapshot: DuckLakeSnapshot,
    *,
    mode: SyncMode,
    iceberg_snapshot_id: int,
    now: datetime,
    tool_version: str,
) -> dict[str, str]:
    """Build the complete set of ducklake.* / iceduck.* properties.

    All values are ``str``. Integers use plain decimal without thousands
    separators; timestamps use ISO-8601 in UTC with a ``Z`` suffix and
    microsecond precision. **A missing key means unknown** - never write
    ``"None"``, ``""`` or ``"null"``.

    Args:
        identity: Resolved DuckLake identity.
        snapshot: Target snapshot of the export.
        mode: Sync mode (full or incremental).
        iceberg_snapshot_id: ID of the Iceberg snapshot created by this commit.
        now: Commit timestamp (process clock).
        tool_version: Version of the iceduck package.

    Returns:
        Complete property map, always complete, never a delta.
    """
    # Normalize timestamps to UTC
    snapshot_time_utc = snapshot.snapshot_time.astimezone(UTC)
    now_utc = now.astimezone(UTC)

    return {
        PROP_STATE_VERSION: str(STATE_VERSION),
        PROP_SNAPSHOT_ID: str(snapshot.snapshot_id),
        PROP_SNAPSHOT_TIME: snapshot_time_utc.isoformat(timespec="microseconds"),
        PROP_SCHEMA_VERSION: str(snapshot.schema_version),
        PROP_TABLE_ID: str(identity.table_id),
        PROP_TABLE_UUID: identity.table_uuid,
        PROP_LAST_COLUMN_ID: str(identity.last_column_id),
        PROP_PARTITION_ID: str(identity.partition_id),
        PROP_DATA_PATH: identity.data_path,
        PROP_ICEBERG_SNAPSHOT_ID: str(iceberg_snapshot_id),
        PROP_SYNC_MODE: mode.value,
        PROP_SYNCED_AT: now_utc.isoformat(timespec="microseconds"),
        PROP_VERSION: tool_version,
    }


def build_summary_properties(
    props: dict[str, str],
    counters: dict[str, str],
) -> dict[str, str]:
    """Combine state properties with counters for the snapshot summary.

    The ``summary`` mirrors the ``ducklake.*``/``iceduck.*`` keys of
    ``properties``, plus the operational counters (``added-data-files``, etc.).

    Args:
        props: State properties (result of build_sync_properties).
        counters: Operational counters (added-data-files, total-records, etc.).

    Returns:
        Complete map for the snapshot summary.
    """
    # Keep only managed properties
    state_props = {k: v for k, v in props.items() if k.startswith(MANAGED_PREFIXES)}
    # Merge with counters
    return {**state_props, **counters}


def parse_sync_state(properties: dict[str, str]) -> SyncState:
    """Read the state back from the Iceberg table properties.

    Applies strict conversion: ``int(value, 10)`` on a string without spaces;
    any failure becomes ``StateMismatch(code="MALFORMED")``, not ``None`` -
    confusing "malformed" with "absent" would turn a read error into a silent
    full re-export.

    Args:
        properties: Iceberg table properties (from metadata.json).

    Returns:
        Parsed state. If no managed property is present,
        ``SyncState.is_absent`` is True.
    """

    def _parse_int(key: str) -> int | None:
        # Malformed becomes None here; ``malformed_properties`` detects it in raw
        return _strict_int(properties.get(key))

    def _parse_datetime(key: str) -> datetime | None:
        return _strict_datetime(properties.get(key))

    def _parse_sync_mode(key: str) -> SyncMode | None:
        value = properties.get(key)
        if value is None:
            return None
        try:
            return SyncMode(value)
        except ValueError:
            return None

    state_version = _parse_int(PROP_STATE_VERSION)
    snapshot_id = _parse_int(PROP_SNAPSHOT_ID)
    snapshot_time = _parse_datetime(PROP_SNAPSHOT_TIME)
    schema_version = _parse_int(PROP_SCHEMA_VERSION)
    table_id = _parse_int(PROP_TABLE_ID)
    table_uuid = properties.get(PROP_TABLE_UUID)
    last_column_id = _parse_int(PROP_LAST_COLUMN_ID)
    partition_id = _parse_int(PROP_PARTITION_ID)
    data_path = properties.get(PROP_DATA_PATH)
    iceberg_snapshot_id = _parse_int(PROP_ICEBERG_SNAPSHOT_ID)
    sync_mode = _parse_sync_mode(PROP_SYNC_MODE)
    synced_at = _parse_datetime(PROP_SYNCED_AT)
    iceduck_version = properties.get(PROP_VERSION)

    return SyncState(
        state_version=state_version,
        snapshot_id=snapshot_id,
        snapshot_time=snapshot_time,
        schema_version=schema_version,
        table_id=table_id,
        table_uuid=table_uuid,
        last_column_id=last_column_id,
        partition_id=partition_id,
        data_path=data_path,
        iceberg_snapshot_id=iceberg_snapshot_id,
        sync_mode=sync_mode,
        synced_at=synced_at,
        iceduck_version=iceduck_version,
        raw=properties,
    )


def _strict_int(value: object) -> int | None:
    """Strict ``int(value, 10)``; ``None`` if absent or malformed."""
    if not isinstance(value, str) or not _INT_RE.fullmatch(value):
        return None
    return int(value, 10)


def _strict_datetime(value: object) -> datetime | None:
    """ISO-8601 (``Z`` suffix accepted); ``None`` if absent or malformed."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def malformed_properties(state: SyncState) -> list[StateMismatch]:
    """Managed properties present in raw but unreadable (``MALFORMED``).

    Looks at ``raw`` (not the already converted fields): a present key with an
    invalid value is always an error, even when ``ducklake.snapshot-id`` is
    also unreadable - that is precisely the case that must not become
    "absent state".
    """
    out: list[StateMismatch] = []

    def bad(key: str, expected: str) -> None:
        found = state.raw.get(key)
        out.append(
            StateMismatch(
                code="MALFORMED",
                property=key,
                expected=expected,
                found=found,
                detail=f"Malformed property {key}: {found!r}",
            )
        )

    for key in _INT_KEYS:
        if key in state.raw and _strict_int(state.raw[key]) is None:
            bad(key, "decimal integer")
    for key in _DATETIME_KEYS:
        if key in state.raw and _strict_datetime(state.raw[key]) is None:
            bad(key, "ISO-8601 timestamp")
    for key in _TEXT_KEYS:
        if key in state.raw and not state.raw[key]:
            bad(key, "non-empty text")
    if PROP_SYNC_MODE in state.raw:
        try:
            SyncMode(state.raw[PROP_SYNC_MODE])
        except ValueError:
            bad(PROP_SYNC_MODE, "full | incremental")
    return out


def validate_identity(
    state: SyncState,
    identity: DuckLakeIdentity,
) -> list[StateMismatch]:
    """Validate the identity of the state read against the resolved identity.

    Compares, in this order, and stops at the first mismatch:

    0. format: managed key present but unreadable (``MALFORMED``)
    1. ``iceduck.state-version`` <= ``STATE_VERSION``
    2. ``ducklake.table-uuid`` == ``identity.table_uuid``
    3. ``ducklake.table-id`` == ``identity.table_id``
    4. ``ducklake.data-path`` == ``identity.data_path``

    A mismatch in any of them **forbids incremental sync**: the delta would be
    computed over a timeline that is not the one of the published table. The
    remedy is ``--force-full``.

    Args:
        state: State read from the Iceberg table.
        identity: Resolved DuckLake identity.

    Returns:
        List of mismatches found. Empty if everything is OK.
    """
    # 0. Format: a present but unreadable key is never treated as absent
    mismatches = malformed_properties(state)
    if mismatches:
        return mismatches

    # 1. State version
    if state.state_version is not None and state.state_version > STATE_VERSION:
        mismatches.append(
            StateMismatch(
                code="STATE_VERSION_AHEAD",
                property=PROP_STATE_VERSION,
                expected=f"<= {STATE_VERSION}",
                found=str(state.state_version),
                detail=f"State is at version {state.state_version}, "
                f"tool supports {STATE_VERSION}",
            )
        )
        return mismatches

    # 2. Table UUID
    if state.table_uuid is not None and state.table_uuid != identity.table_uuid:
        mismatches.append(
            StateMismatch(
                code="IDENTITY_MISMATCH",
                property=PROP_TABLE_UUID,
                expected=identity.table_uuid,
                found=state.table_uuid,
                detail=f"catalog table_uuid ({identity.table_uuid}) "
                f"differs from the published one ({state.table_uuid}): "
                "table was recreated",
            )
        )
        return mismatches

    # 3. Table ID
    if state.table_id is not None and state.table_id != identity.table_id:
        mismatches.append(
            StateMismatch(
                code="IDENTITY_MISMATCH",
                property=PROP_TABLE_ID,
                expected=str(identity.table_id),
                found=str(state.table_id),
                detail=f"catalog table_id ({identity.table_id}) "
                f"differs from the published one ({state.table_id})",
            )
        )
        return mismatches

    # 4. Data path
    if state.data_path is not None and state.data_path != identity.data_path:
        mismatches.append(
            StateMismatch(
                code="IDENTITY_MISMATCH",
                property=PROP_DATA_PATH,
                expected=identity.data_path,
                found=state.data_path,
                detail=f"published data_path ({state.data_path}) "
                f"differs from the catalog ({identity.data_path})",
            )
        )
        return mismatches

    return mismatches
