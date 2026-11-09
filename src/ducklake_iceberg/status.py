"""The ``ducklake-iceberg status`` command.

Diagnoses, without writing anywhere, the relationship between the DuckLake
catalog and the Iceberg table published in Glue, by comparing four quantities
(D, I, P, Î) and classifying the result into named statuses with deterministic
severity and remediation.

- **D**: the current DuckLake snapshot;
- **I**: ``current-snapshot-id`` of the ``metadata.json`` Glue points to;
- **P**: the ``ducklake.snapshot-id`` property (from ``metadata.json``, never
  from the Glue ``Parameters``);
- **Î**: the ``ducklake_iceberg.iceberg-snapshot-id`` property.

The pending work is the same **set difference** as ``export``: live data files
in DuckLake (by canonical path) and delete files consolidated per partition
against the live entries of the current Iceberg snapshot. So ``status`` and
``sync`` agree: ``BEHIND`` means the next ``sync`` writes something, and a
DuckLake that advanced only in other tables stays ``IN_SYNC``.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final, cast

from botocore.exceptions import BotoCoreError, ClientError
from pyiceberg.manifest import ManifestContent

from ducklake_iceberg.catalog.files import read_path_layers
from ducklake_iceberg.catalog.reader import TableRef, current_snapshot, resolve_table
from ducklake_iceberg.checkpoint.detect import detect_inlined
from ducklake_iceberg.checkpoint.model import TableRef as CheckpointTableRef
from ducklake_iceberg.doctor.model import quote_ident
from ducklake_iceberg.errors import DuckLakeIcebergError, IdentityError
from ducklake_iceberg.export import _desired, _evolve_schema, _read_catalog
from ducklake_iceberg.mapping.schema import read_last_column_id
from ducklake_iceberg.sync.state import (
    DuckLakeIdentity,
    StateMismatch,
    SyncState,
    parse_sync_state,
    validate_identity,
)
from ducklake_iceberg.writer.commit import load_base_snapshot

if TYPE_CHECKING:
    from pyiceberg.catalog.glue import GlueCatalog
    from pyiceberg.table import Table
    from pyiceberg.table.metadata import TableMetadata

    from ducklake_iceberg.catalog.connection import DuckLakeConnection
    from ducklake_iceberg.catalog.reader import TableIdentity

LOG = logging.getLogger("ducklake_iceberg")

#: ``P`` within N snapshots of the oldest retained one raises a warning.
RETENTION_WARN_SNAPSHOTS: Final[int] = 5

#: Safety limit when walking ``parent-snapshot-id``.
_MAX_PATH: Final[int] = 100

_ICEBERG_TABLE_TYPE: Final[str] = "ICEBERG"
_CHANGE_RE: Final[re.Pattern[str]] = re.compile(r"([a-z_]+):(\d+)")


# -- enums and structures -------------------------------------------------------


class Severity(StrEnum):
    """Severity of a finding or status."""

    OK = "ok"
    INFO = "info"
    WARN = "warn"
    ERROR = "error"


class StatusCode(StrEnum):
    """Table status code."""

    IN_SYNC = "in_sync"
    NEVER_SYNCED = "never_synced"
    BEHIND = "behind"
    SCHEMA_DRIFT = "schema_drift"
    RETENTION_RISK = "retention_risk"
    CATALOG_REWOUND = "catalog_rewound"
    EXTERNAL_WRITE = "external_write"
    HISTORY_REWRITTEN = "history_rewritten"
    DIVERGED_BOTH = "diverged_both"
    STATE_WITHOUT_SNAPSHOT = "state_without_snapshot"
    IDENTITY_MISMATCH = "identity_mismatch"
    FOREIGN = "foreign"
    UNREADABLE = "unreadable"


#: Exit code per status: 4 only for ``IN_SYNC``, 1 for operational errors.
_EXIT_BY_STATUS: Final[Mapping[StatusCode, int]] = {
    StatusCode.IN_SYNC: 4,
    StatusCode.UNREADABLE: 1,
}

#: Statuses that suspend write/cleanup operations.
DIVERGENT_STATUSES: Final[frozenset[StatusCode]] = frozenset(
    {
        StatusCode.EXTERNAL_WRITE,
        StatusCode.HISTORY_REWRITTEN,
        StatusCode.DIVERGED_BOTH,
    }
)


@dataclass(frozen=True)
class Finding:
    """A single diagnostic finding."""

    code: StatusCode
    severity: Severity
    message: str
    detail: dict[str, object]
    remediation: str | None


@dataclass(frozen=True)
class Pending:
    """Pending work: set difference between DuckLake and Iceberg."""

    snapshots: int
    added_files: int
    removed_files: int
    added_records: int
    removed_records: int
    added_bytes: int
    added_delete_files: int = 0
    removed_delete_files: int = 0

    @property
    def has_changes(self) -> bool:
        """True if the next ``sync`` would have files to publish."""
        return bool(
            self.added_files
            or self.removed_files
            or self.added_delete_files
            or self.removed_delete_files
        )


@dataclass(frozen=True)
class TableStatus:
    """Full status of a table."""

    ducklake_schema: str
    ducklake_table: str
    glue_database: str
    glue_table: str
    # the four quantities
    d_snapshot_id: int | None  # current DuckLake
    i_snapshot_id: int | None  # Iceberg current-snapshot-id
    p_snapshot_id: int | None  # ducklake.snapshot-id
    i_hat_snapshot_id: int | None  # ducklake_iceberg.iceberg-snapshot-id
    d_schema_version: int | None
    p_schema_version: int | None
    oldest_retained_snapshot: int | None
    metadata_location: str | None
    status: StatusCode
    severity: Severity
    exit_code: int
    findings: tuple[Finding, ...]
    pending: Pending | None
    checked_at: datetime
    # details for the --json document
    ducklake_table_id: int | None = None
    ducklake_table_uuid: str | None = None
    d_snapshot_time: datetime | None = None
    sequence_number: int | None = None
    current_schema_id: int | None = None
    total_data_files: int | None = None
    total_records: int | None = None
    state_version: int | None = None
    synced_at: datetime | None = None
    sync_mode: str | None = None


# -- exceptions -----------------------------------------------------------------


class StatusError(DuckLakeIcebergError):
    """Error while collecting status."""


# -- observations (classification input) ----------------------------------------


class GlueSide(StrEnum):
    """Outcome of reading the Glue/Iceberg side."""

    ABSENT = "absent"
    FOREIGN = "foreign"
    UNREADABLE = "unreadable"
    OK = "ok"


@dataclass(frozen=True)
class Observed:
    """Everything the classification matrix needs, already collected.

    Input of the pure function ``classify`` below.
    """

    glue: GlueSide
    d_snapshot_id: int | None
    d_schema_version: int | None = None
    oldest_retained: int | None = None
    ducklake_missing: bool = False
    glue_message: str | None = None
    i_snapshot_id: int | None = None
    p_snapshot_id: int | None = None
    i_hat_snapshot_id: int | None = None
    p_schema_version: int | None = None
    has_ducklake_keys: bool = True
    state_mismatch: StateMismatch | None = None
    #: snapshot_id -> (parent, operation, summary)
    snapshots: Mapping[int, tuple[int | None, str | None, dict[str, str]]] = field(
        default_factory=dict
    )
    #: did the table change in DuckLake in (P, D]? ``None`` = not evaluated
    table_changed: bool | None = None
    pending: Pending | None = None
    remediation: str = "ducklake-iceberg sync --table <schema>.<table>"


def _finding(
    code: StatusCode,
    severity: Severity,
    message: str,
    remediation: str | None = None,
    **detail: object,
) -> Finding:
    return Finding(
        code=code,
        severity=severity,
        message=message,
        detail=dict(detail),
        remediation=remediation,
    )


def _exit_for(status: StatusCode) -> int:
    return _EXIT_BY_STATUS.get(status, 3)


def _verdict(
    findings: list[Finding], extra: list[Finding] | None = None
) -> tuple[StatusCode, Severity, int, list[Finding]]:
    """The first finding defines the status; the rest follow as additional ones."""
    head = findings[0]
    return head.code, head.severity, _exit_for(head.code), [*findings, *(extra or [])]


def classify(obs: Observed) -> tuple[StatusCode, Severity, int, list[Finding]]:
    """Classify the status using the rule matrix (evaluated in order).

    Returns:
        ``(status, severity, exit_code, findings)``.
    """
    remediation = obs.remediation
    D = obs.d_snapshot_id  # noqa: N806 - D/P/I/Î notation
    P = obs.p_snapshot_id  # noqa: N806
    I = obs.i_snapshot_id  # noqa: N806, E741
    I_hat = obs.i_hat_snapshot_id  # noqa: N806

    # 1. table absent from Glue
    if obs.glue is GlueSide.ABSENT:
        return _verdict(
            [
                _finding(
                    StatusCode.NEVER_SYNCED,
                    Severity.WARN,
                    "table was never published to Glue",
                    remediation,
                )
            ]
        )
    # 2. unreadable metadata_location, IAM, deleted object
    if obs.glue is GlueSide.UNREADABLE:
        return _verdict(
            [
                _finding(
                    StatusCode.UNREADABLE,
                    Severity.ERROR,
                    obs.glue_message or "unreadable metadata.json",
                    "check permissions (glue:GetTable, s3:GetObject) and the "
                    "metadata_location",
                )
            ]
        )
    # 3. foreign origin: non-Iceberg table_type or no ducklake.* key
    if obs.glue is GlueSide.FOREIGN or not obs.has_ducklake_keys:
        return _verdict(
            [
                _finding(
                    StatusCode.FOREIGN,
                    Severity.ERROR,
                    obs.glue_message
                    or "Iceberg table from another origin (no ducklake.* properties)",
                    "register the mirror under a different name",
                    current_snapshot_id=I,
                )
            ]
        )
    # 4. identity: table recreated at the source, reused name, malformed state
    # or state from a future version
    if obs.ducklake_missing:
        return _verdict(
            [
                _finding(
                    StatusCode.IDENTITY_MISMATCH,
                    Severity.ERROR,
                    "table published in Glue does not exist in DuckLake",
                    "investigate before any sync (table dropped or renamed)",
                    ducklake_table=None,
                )
            ]
        )
    mismatch = obs.state_mismatch
    if mismatch is not None:
        if mismatch.code == "IDENTITY_MISMATCH":
            return _verdict(
                [
                    _finding(
                        StatusCode.IDENTITY_MISMATCH,
                        Severity.ERROR,
                        mismatch.detail,
                        "after investigating, publish to another prefix/Glue table",
                        property=mismatch.property,
                        expected=mismatch.expected,
                        found=mismatch.found,
                    )
                ]
            )
        # MALFORMED / STATE_VERSION_AHEAD: operational error (exit 1)
        return _verdict(
            [
                _finding(
                    StatusCode.UNREADABLE,
                    Severity.ERROR,
                    mismatch.detail,
                    "upgrade ducklake-iceberg or inspect the table properties",
                    property=mismatch.property,
                    found=mismatch.found,
                    reason=mismatch.code.lower(),
                )
            ]
        )
    if P is None:
        return _verdict(
            [
                _finding(
                    StatusCode.UNREADABLE,
                    Severity.ERROR,
                    "ducklake.* properties present without ducklake.snapshot-id",
                    "inspect the metadata.json properties",
                )
            ]
        )
    # 5. P present and I null
    if I is None:
        return _verdict(
            [
                _finding(
                    StatusCode.STATE_WITHOUT_SNAPSHOT,
                    Severity.ERROR,
                    "ducklake.snapshot-id present but current-snapshot-id is null",
                    "inspect the metadata-log; possible metadata-only commit",
                    p=P,
                )
            ]
        )

    # 6/7. external write or rewritten history
    divergence: Finding | None = None
    if I_hat != I:
        if I_hat is not None and I_hat in obs.snapshots:
            divergence = _finding(
                StatusCode.EXTERNAL_WRITE,
                Severity.ERROR,
                f"current Iceberg snapshot {I} was not created by ducklake-iceberg "
                f"(last known {I_hat})",
                "review the external commits before any sync",
                i=I,
                i_hat=I_hat,
                path=build_snapshot_path(obs.snapshots, I_hat, I),
            )
        else:
            divergence = _finding(
                StatusCode.HISTORY_REWRITTEN,
                Severity.ERROR,
                f"ducklake-iceberg snapshot {I_hat} does not exist in the history "
                f"(rollback, external expire, or replaced metadata.json)",
                "inspect the metadata-log; republish to another prefix",
                i=I,
                i_hat=I_hat,
            )

    # DuckLake side: 8 rewound, 9 retention, 10 behind, 11 drift
    lake: list[Finding] = []
    rewound = D is not None and D < P
    if rewound:
        lake.append(
            _finding(
                StatusCode.CATALOG_REWOUND,
                Severity.ERROR,
                f"DuckLake catalog went backwards: D={D} < P={P}",
                "investigate a catalog restore/recreation before any sync",
                d=D,
                p=P,
            )
        )
    ahead = D is not None and D > P and _ducklake_ahead(obs)
    expired = obs.oldest_retained is not None and obs.oldest_retained > P
    if ahead and expired:
        lake.append(
            _finding(
                StatusCode.RETENTION_RISK,
                Severity.ERROR,
                f"published snapshot P={P} already expired in DuckLake "
                f"(oldest retained {obs.oldest_retained}): the mirror may "
                f"reference files that were already removed",
                remediation,
                p=P,
                oldest_retained=obs.oldest_retained,
            )
        )
    if ahead:
        behind_by = (D or 0) - P
        lake.append(
            _finding(
                StatusCode.BEHIND,
                Severity.WARN,
                f"mirror is {behind_by} snapshot(s) behind",
                remediation,
                d=D,
                p=P,
                snapshots_behind=behind_by,
            )
        )
    drift = (
        obs.d_schema_version is not None
        and obs.p_schema_version is not None
        and obs.d_schema_version != obs.p_schema_version
    )
    drift_finding = (
        _finding(
            StatusCode.SCHEMA_DRIFT,
            Severity.WARN,
            f"schema changed: DuckLake v{obs.d_schema_version} != mirror "
            f"v{obs.p_schema_version}",
            remediation,
            d_schema_version=obs.d_schema_version,
            p_schema_version=obs.p_schema_version,
        )
        if drift
        else None
    )
    extra = _extra_findings(obs, expired=expired)

    if divergence is not None:
        if rewound or ahead:
            both = _finding(
                StatusCode.DIVERGED_BOTH,
                Severity.ERROR,
                "divergence on both sides: external write and DuckLake lag/rewind",
                "review the external commits before any republish",
            )
            return _verdict([both, divergence, *lake], extra)
        return _verdict(
            [divergence], [*([drift_finding] if drift_finding else []), *extra]
        )
    if lake:
        return _verdict(lake, [*([drift_finding] if drift_finding else []), *extra])
    if drift_finding is not None:
        return _verdict([drift_finding], extra)

    # 12. up to date
    in_sync = _finding(
        StatusCode.IN_SYNC,
        Severity.OK,
        f"already synced at S{P} (Iceberg snapshot {I})",
        None,
        d=D,
        p=P,
        i=I,
    )
    if D is not None and D > P:
        extra.insert(
            0,
            _finding(
                StatusCode.IN_SYNC,
                Severity.INFO,
                f"DuckLake advanced {D - P} snapshot(s) with no changes to this table",
                None,
                d=D,
                p=P,
            ),
        )
    return _verdict([in_sync], extra)


def _ducklake_ahead(obs: Observed) -> bool:
    """Would the next sync write anything? Set difference when available."""
    if obs.pending is not None:
        return obs.pending.has_changes
    # without delta (--no-delta or counting failure): tokens of the (P, D]
    # window; without information, be conservative
    return obs.table_changed is not False


def _extra_findings(obs: Observed, *, expired: bool) -> list[Finding]:
    """Non-exclusive findings."""
    out: list[Finding] = []
    P = obs.p_snapshot_id  # noqa: N806
    oldest = obs.oldest_retained
    # only meaningful once DuckLake has expired something (snapshot 0 retained
    # = no expiration): otherwise every new catalog would raise the warning
    if (
        P is not None
        and oldest is not None
        and oldest > 0
        and not expired
        and P - oldest < RETENTION_WARN_SNAPSHOTS
    ):
        out.append(
            _finding(
                StatusCode.RETENTION_RISK,
                Severity.WARN,
                f"P={P} is within {RETENTION_WARN_SNAPSHOTS} snapshots of the "
                f"oldest retained ({oldest})",
                obs.remediation,
                p=P,
                oldest_retained=oldest,
            )
        )
    return out


def build_snapshot_path(
    snapshots: Mapping[int, tuple[int | None, str | None, dict[str, str]]],
    from_id: int,
    to_id: int,
) -> list[dict[str, object]]:
    """Path from ``from_id`` (exclusive) to ``to_id`` via ``parent-snapshot-id``."""
    path: list[dict[str, object]] = []
    current: int | None = to_id
    while current is not None and current != from_id and len(path) < _MAX_PATH:
        entry = snapshots.get(current)
        if entry is None:
            break
        parent, operation, summary = entry
        path.append(
            {
                "snapshot_id": current,
                "operation": operation,
                "added_data_files": summary.get("added-data-files"),
                "deleted_data_files": summary.get("deleted-data-files"),
            }
        )
        current = parent
    path.reverse()
    return path


# -- collection: DuckLake side -------------------------------------------------


@dataclass(frozen=True)
class _DuckLakeInfo:
    snapshot_id: int
    snapshot_time: datetime
    oldest_retained: int | None
    identity: TableIdentity | None
    missing_error: IdentityError | None
    data_path: str | None


def _collect_ducklake(
    con: DuckLakeConnection, schema_name: str, table_name: str
) -> _DuckLakeInfo:
    snapshot = current_snapshot(con)
    rows = con.query_arrow(
        f"SELECT min(snapshot_id) AS oldest "  # noqa: S608 - quoted identifier
        f"FROM {quote_ident(con.meta)}.ducklake_snapshot"
    ).to_pylist()
    oldest = rows[0]["oldest"] if rows else None
    try:
        identity = resolve_table(con, TableRef(schema_name, table_name), snapshot)
    except IdentityError as exc:
        return _DuckLakeInfo(
            snapshot_id=snapshot.snapshot_id,
            snapshot_time=snapshot.snapshot_time,
            oldest_retained=None if oldest is None else int(oldest),
            identity=None,
            missing_error=exc,
            data_path=None,
        )
    layers = read_path_layers(
        con.raw, con.meta, identity.table_id, snapshot.snapshot_id
    )
    return _DuckLakeInfo(
        snapshot_id=snapshot.snapshot_id,
        snapshot_time=snapshot.snapshot_time,
        oldest_retained=None if oldest is None else int(oldest),
        identity=identity,
        missing_error=None,
        data_path=layers.data_path,
    )


def _table_changed(
    con: DuckLakeConnection, table_id: int, previous: int, target: int
) -> bool:
    """Does any ``ducklake_snapshot_changes`` in window ``(prev, S]`` cite the table?"""
    rows = con.query_arrow(
        f"SELECT changes_made "  # noqa: S608 - quoted identifier
        f"FROM {quote_ident(con.meta)}.ducklake_snapshot_changes "
        f"WHERE snapshot_id > $prev AND snapshot_id <= $target",
        {"prev": previous, "target": target},
    ).to_pylist()
    for row in rows:
        for _kind, ident in _CHANGE_RE.findall(str(row["changes_made"] or "")):
            if int(ident) == table_id:
                return True
    return False


# -- collection: Glue / Iceberg side -------------------------------------------


@dataclass(frozen=True)
class _IcebergInfo:
    side: GlueSide
    message: str | None = None
    table: Table | None = None
    metadata_location: str | None = None


def _glue_error_message(exc: BaseException, action: str, resource: str) -> str:
    if isinstance(exc, ClientError):
        error = exc.response.get("Error", {})
        code, message = error.get("Code"), error.get("Message")
        return f"{action} on {resource} failed: {code}: {message}"
    return f"{action} on {resource} failed: {type(exc).__name__}: {exc}"


def _collect_iceberg(catalog: GlueCatalog, identifier: tuple[str, str]) -> _IcebergInfo:
    """``GetTable`` for triage and ``metadata.json`` as the source of truth."""
    from pyiceberg.exceptions import NoSuchTableError

    database, name = identifier
    resource = f"{database}.{name}"
    try:
        response = cast("Any", catalog).glue.get_table(DatabaseName=database, Name=name)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "EntityNotFoundException":
            return _IcebergInfo(side=GlueSide.ABSENT)
        return _IcebergInfo(
            side=GlueSide.UNREADABLE,
            message=_glue_error_message(exc, "glue:GetTable", resource),
        )
    except BotoCoreError as exc:
        return _IcebergInfo(
            side=GlueSide.UNREADABLE,
            message=_glue_error_message(exc, "glue:GetTable", resource),
        )
    params: dict[str, str] = dict(response.get("Table", {}).get("Parameters") or {})
    table_type = params.get("table_type", "")
    if table_type.upper() != _ICEBERG_TABLE_TYPE:
        return _IcebergInfo(
            side=GlueSide.FOREIGN,
            message=(
                f"Glue table {resource} is not Iceberg (table_type={table_type!r})"
            ),
        )
    location = params.get("metadata_location")
    if not location:
        return _IcebergInfo(
            side=GlueSide.UNREADABLE,
            message=f"Glue table {resource} has no metadata_location",
        )
    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        return _IcebergInfo(side=GlueSide.ABSENT)
    except Exception as exc:  # IAM, deleted object, invalid JSON
        LOG.debug("reading metadata.json of %s failed", resource, exc_info=exc)
        return _IcebergInfo(
            side=GlueSide.UNREADABLE,
            message=_glue_error_message(exc, "reading metadata.json", location),
            metadata_location=location,
        )
    return _IcebergInfo(
        side=GlueSide.OK, table=table, metadata_location=table.metadata_location
    )


def _snapshots_of(
    metadata: TableMetadata,
) -> dict[int, tuple[int | None, str | None, dict[str, str]]]:
    out: dict[int, tuple[int | None, str | None, dict[str, str]]] = {}
    for snap in metadata.snapshots:
        summary = snap.summary
        operation = str(summary.operation.value) if summary is not None else None
        props = dict(summary.additional_properties) if summary is not None else {}
        out[snap.snapshot_id] = (snap.parent_snapshot_id, operation, props)
    return out


# -- pending: set difference (applied as in export) ----------------------------


def compute_pending(
    con: DuckLakeConnection,
    table: Table,
    identity: TableIdentity,
    *,
    previous_snapshot_id: int,
) -> Pending:
    """Live DuckLake vs. live entries of the current Iceberg snapshot.

    Data files by canonical path and position deletes consolidated per
    partition, exactly as ``export`` decides what to write.
    """
    snapshot_id = identity.snapshot.snapshot_id
    view = _read_catalog(
        con,
        f"{identity.ref.schema_name}.{identity.ref.table_name}",
        previous_snapshot_id,
    )
    desired = _desired(
        view,
        _evolve_schema(view.schema_mapping.schema, table.metadata),
    )
    base = load_base_snapshot(table.io, table.metadata)
    live_data = base.live(ManifestContent.DATA)
    live_deletes = base.live(ManifestContent.DELETES)

    desired_data = {
        key: file for key, (file, _spec_id, _partition) in desired.data.items()
    }
    desired_deletes = set(desired.deletes)
    added = [desired_data[k] for k in sorted(desired_data.keys() - live_data.keys())]
    removed = [live_data[k] for k in sorted(live_data.keys() - desired_data.keys())]
    return Pending(
        snapshots=max(snapshot_id - previous_snapshot_id, 0),
        added_files=len(added),
        removed_files=len(removed),
        added_records=sum(f.record_count for f in added),
        removed_records=sum(e.data_file.record_count for e in removed),
        added_bytes=sum(f.file_size_in_bytes for f in added),
        added_delete_files=len(desired_deletes - live_deletes.keys()),
        removed_delete_files=len(live_deletes.keys() - desired_deletes),
    )


# -- main function --------------------------------------------------------------


def table_status(
    con: DuckLakeConnection,
    catalog: GlueCatalog,
    *,
    ducklake_schema: str,
    ducklake_table: str,
    glue_identifier: tuple[str, str],
    with_delta: bool = True,
) -> TableStatus:
    """Collect and classify the status of a table. Never writes.

    Args:
        con: Open DuckLake connection (``READ_ONLY``), preferably inside
            ``read_transaction()``.
        catalog: Glue catalog (``ducklake_iceberg.glue.build_catalog``).
        ducklake_schema: Schema in DuckLake.
        ducklake_table: Table in DuckLake.
        glue_identifier: ``(database, table)`` in Glue, with the normalized name.
        with_delta: If False (``--no-delta``), reads no files or manifests.

    Raises:
        IdentityError: Table absent from DuckLake **and** from Glue.
    """
    checked_at = datetime.now(UTC)
    qualified = f"{ducklake_schema}.{ducklake_table}"
    lake = _collect_ducklake(con, ducklake_schema, ducklake_table)
    ice = _collect_iceberg(catalog, glue_identifier)
    if ice.side is GlueSide.ABSENT and lake.missing_error is not None:
        raise lake.missing_error

    identity = lake.identity
    table = ice.table
    metadata = table.metadata if table is not None else None
    state: SyncState = parse_sync_state(dict(metadata.properties) if metadata else {})
    findings_extra: list[Finding] = []

    mismatch: StateMismatch | None = None
    if metadata is not None and identity is not None and lake.data_path is not None:
        mismatches = validate_identity(
            state,
            DuckLakeIdentity(
                table_id=identity.table_id,
                table_uuid=str(identity.table_uuid),
                schema_name=ducklake_schema,
                table_name=ducklake_table,
                data_path=lake.data_path,
                partition_id=-1,  # not compared by validate_identity
                last_column_id=read_last_column_id(
                    con.raw, con.meta, identity.table_id
                ),
            ),
        )
        mismatch = mismatches[0] if mismatches else None

    P = state.snapshot_id  # noqa: N806 - D/P/I/Î notation
    I = metadata.current_snapshot_id if metadata is not None else None  # noqa: N806, E741
    I_hat = state.iceberg_snapshot_id  # noqa: N806
    D = lake.snapshot_id  # noqa: N806

    usable = (
        table is not None
        and identity is not None
        and mismatch is None
        and P is not None
        and I is not None
        and I_hat == I
    )
    table_changed: bool | None = None
    pending: Pending | None = None
    if usable and identity is not None and P is not None and table is not None:
        if D > P:
            table_changed = _table_changed(con, identity.table_id, P, D)
        if with_delta and D >= P:
            try:
                pending = compute_pending(con, table, identity, previous_snapshot_id=P)
            except Exception as exc:  # the count is informative; the status is not
                LOG.debug("pending of %s not computed", qualified, exc_info=exc)
                findings_extra.append(
                    _finding(
                        StatusCode.UNREADABLE,
                        Severity.WARN,
                        f"pending not computed: {getattr(exc, 'message', exc)}",
                        None,
                    )
                )
    elif (
        identity is not None
        and P is not None
        and D > P
        and table is not None
        and mismatch is None
    ):
        # external write: the Iceberg set is polluted; use tokens only
        table_changed = _table_changed(con, identity.table_id, P, D)

    if identity is not None and ice.side is GlueSide.OK:
        findings_extra.extend(_inlined_finding(con, identity, qualified))

    obs = Observed(
        glue=ice.side,
        d_snapshot_id=D,
        d_schema_version=identity.schema_version if identity else None,
        oldest_retained=lake.oldest_retained,
        ducklake_missing=identity is None,
        glue_message=ice.message,
        i_snapshot_id=I,
        p_snapshot_id=P,
        i_hat_snapshot_id=I_hat,
        p_schema_version=state.schema_version,
        has_ducklake_keys=any(k.startswith("ducklake.") for k in state.raw),
        state_mismatch=mismatch,
        snapshots=_snapshots_of(metadata) if metadata is not None else {},
        table_changed=table_changed,
        pending=pending,
        remediation=f"ducklake-iceberg sync --table {qualified}",
    )
    status_code, severity, exit_code, findings = classify(obs)
    findings.extend(findings_extra)

    summary = _current_summary(metadata)
    return TableStatus(
        ducklake_schema=ducklake_schema,
        ducklake_table=ducklake_table,
        glue_database=glue_identifier[0],
        glue_table=glue_identifier[1],
        d_snapshot_id=D,
        i_snapshot_id=I,
        p_snapshot_id=P,
        i_hat_snapshot_id=I_hat,
        d_schema_version=identity.schema_version if identity else None,
        p_schema_version=state.schema_version,
        oldest_retained_snapshot=lake.oldest_retained,
        metadata_location=ice.metadata_location,
        status=status_code,
        severity=severity,
        exit_code=exit_code,
        findings=tuple(findings),
        pending=pending,
        checked_at=checked_at,
        ducklake_table_id=identity.table_id if identity else None,
        ducklake_table_uuid=str(identity.table_uuid) if identity else None,
        d_snapshot_time=lake.snapshot_time,
        sequence_number=getattr(metadata, "last_sequence_number", None),
        current_schema_id=metadata.current_schema_id if metadata is not None else None,
        total_data_files=_int_or_none(summary.get("total-data-files")),
        total_records=_int_or_none(summary.get("total-records")),
        state_version=state.state_version,
        synced_at=state.synced_at,
        sync_mode=state.sync_mode.value if state.sync_mode else None,
    )


def _current_summary(metadata: TableMetadata | None) -> dict[str, str]:
    if metadata is None:
        return {}
    snapshot = metadata.current_snapshot()
    if snapshot is None or snapshot.summary is None:
        return {}
    return dict(snapshot.summary.additional_properties)


def _int_or_none(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value, 10)
    except ValueError:
        return None


def _inlined_finding(
    con: DuckLakeConnection, identity: TableIdentity, qualified: str
) -> list[Finding]:
    """Edge case: unflushed inlined data is invisible to export."""
    try:
        inlined = detect_inlined(
            con.raw,
            con.meta,
            CheckpointTableRef(
                identity.ref.schema_name, identity.ref.table_name, identity.table_id
            ),
            identity.snapshot.snapshot_id,
        )
    except Exception as exc:  # auxiliary diagnostic; never breaks status
        LOG.debug("inlined detection failed for %s", qualified, exc_info=exc)
        return []
    if not inlined.has_pending:
        return []
    return [
        _finding(
            StatusCode.BEHIND,
            Severity.INFO,
            f"{inlined.pending_rows} inlined row(s) not flushed; "
            f"run ducklake-iceberg checkpoint before the next sync",
            f"ducklake-iceberg checkpoint --table {qualified}",
            pending_rows=inlined.pending_rows,
        )
    ]
