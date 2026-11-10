"""Registration in the AWS Glue Data Catalog.

Publishes to Glue the pointer to the ``metadata.json`` already written to S3,
via ``GlueCatalog.register_table(identifier, metadata_location)``. This is the
**only** module of the tool that writes to Glue.

Design points:

- Publication is exclusively through ``register_table``. ``create_table`` is
  forbidden: it reassigns field IDs and would silently produce null columns.
  An architecture test guarantees the symbol is absent from this module.
- The database is created when missing; a creation race
  (``AlreadyExistsException``) is treated as success.
- Table already registered: if it already points to the requested
  ``metadata.json``, ``TableAlreadyRegistered`` (exit 4); if it points to an
  ancestor of it (found in the ``metadata-log``), ``UpdateTable`` with
  ``VersionId`` (optimistic locking, the only strategy Athena supports);
  otherwise someone wrote outside ducklake-iceberg and registration stops with exit 3.
  A same-named **non-Iceberg** table is never overwritten (exit 1).
- A post-registration re-read checks the ``metadata_location``; a mismatch is
  exit 3.
- Retry with exponential backoff + jitter only for transient conditions
  (``ConcurrentModificationException``, ``ThrottlingException``,
  ``OperationTimeoutException``); ``AccessDenied`` and ``InvalidInput`` are
  never retried.
- Lake Formation: reactive detection (via ``GetTable`` or the ``AccessDenied``
  message) with an actionable message.

Minimum IAM permissions, also used by the ``moto`` tests:

``register`` - Glue:
    glue:GetDatabase    (catalog, database/<db>)        - always
    glue:CreateDatabase (catalog, database/<db>)        - only with create_database
    glue:GetTable       (catalog, database/<db>, table) - always
    glue:CreateTable    (catalog, database/<db>, table) - always
    glue:UpdateTable    (same)                          - subsequent syncs

``register`` - S3 (register_table READS the metadata.json to build the columns):
    s3:GetObject   arn:aws:s3:::<bucket>/<iceberg-prefix>/*
    s3:ListBucket  arn:aws:s3:::<bucket>  (s3:prefix limited to the Iceberg prefix)
    kms:Decrypt    bucket key, if SSE-KMS

Lake Formation, when active: lakeformation:GetDataAccess on the data location.

The ``register`` principal does **not** need ``s3:PutObject`` (``export`` does
the writing), and no principal needs write access to the DuckLake
``data_path``.
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final, cast

from botocore.exceptions import ClientError

from ducklake_iceberg import aws
from ducklake_iceberg.aws import s3_credential_properties, s3_endpoint_properties
from ducklake_iceberg.catalogs import normalize_table_name as normalize_table_name
from ducklake_iceberg.errors import AwsError, DivergenceError, ExitCode, NothingToDo
from ducklake_iceberg.sync.state import SyncState, parse_sync_state

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyiceberg.catalog.glue import GlueCatalog
    from pyiceberg.io import FileIO
    from pyiceberg.table import Table
    from pyiceberg.table.metadata import TableMetadata

LOG = logging.getLogger("ducklake_iceberg")

#: value written to `table_type` by PyIceberg (`ICEBERG.upper()`).
_ICEBERG_TABLE_TYPE: Final[str] = "ICEBERG"

#: transient conditions that deserve a retry with backoff.
_RETRYABLE: Final[frozenset[str]] = frozenset(
    {
        "ConcurrentModificationException",
        "ThrottlingException",
        "OperationTimeoutException",
    }
)

_MAX_ATTEMPTS: Final[int] = 5
_BACKOFF_BASE: Final[float] = 0.2
_BACKOFF_CAP: Final[float] = 5.0


@dataclass(frozen=True, slots=True)
class GlueTarget:
    database: str  # DuckLakeIcebergSettings.glue_database
    table: str  # normalized DuckLake name
    region: str
    profile: str | None
    catalog_id: str | None  # account that owns the catalog; None = current account
    s3_endpoint: str | None = None  # custom S3 endpoint for the metadata I/O

    @property
    def identifier(self) -> tuple[str, str]:
        return (self.database, self.table)


class RegisterOutcome(StrEnum):
    REGISTERED = "registered"  # created now
    UPDATED = "updated"  # pointer advanced to the new metadata.json
    ALREADY_EXISTS = "already-exists"  # already points to the requested metadata.json
    DRY_RUN = "dry-run"  # nothing written


@dataclass(frozen=True, slots=True)
class RegisterResult:
    outcome: RegisterOutcome
    database_created: bool
    metadata_location: str
    previous_metadata_location: str | None
    lake_formation: bool | None  # None = could not be determined


class GlueError(AwsError):
    """Failure talking to Glue. exit_code = ExitCode.ERROR (inherited)."""


class TableAlreadyRegistered(NothingToDo):
    """Table already published at exactly this ``metadata.json`` (exit 4)."""

    def __init__(
        self,
        message: str,
        *,
        hint: str | None = None,
        metadata_location: str | None = None,
    ) -> None:
        super().__init__(message, hint=hint)
        #: ``metadata.json`` Glue already points to
        self.metadata_location = metadata_location


class LakeFormationBlocked(GlueError):
    exit_code = ExitCode.BLOCKED


class RegisterDiverged(DivergenceError):
    """Post-registration re-read returned another metadata_location (exit 3)."""


# -- table name normalization -----------------------------------------------


# -- client / catalog construction ------------------------------------------


def _glue_properties(target: GlueTarget) -> dict[str, str]:
    props: dict[str, str] = {"glue.region": target.region}
    if target.profile is not None:
        props["glue.profile-name"] = target.profile
    if target.catalog_id is not None:
        props["glue.id"] = target.catalog_id
    return props


def _glue_client(target: GlueTarget) -> Any:
    """Process-wide Glue client (one Session, one credential; see ``aws``)."""
    return aws.client("glue", target.region, target.profile)


def build_catalog(target: GlueTarget) -> GlueCatalog:
    """Build the PyIceberg ``GlueCatalog`` from the target.

    The ``s3.*`` properties with the profile credential reach the FileIO that
    ``register_table`` uses to read the ``metadata.json``.
    """
    from pyiceberg.catalog.glue import GlueCatalog

    # With ``glue.id`` PyIceberg registers the CatalogId on the client it builds
    # itself; without it, it reuses the process-wide client (no new Session).
    client = _glue_client(target) if target.catalog_id is None else None
    return GlueCatalog(
        "ducklake-iceberg",
        client,
        **_glue_properties(target),
        **s3_credential_properties(target.region, target.profile),
        **s3_endpoint_properties(target.s3_endpoint),
    )


# -- error translation and retry -------------------------------------------


def _error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


def _error_message(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Message", ""))


def _is_lake_formation(exc: ClientError) -> bool:
    return "lake formation" in _error_message(exc).lower()


def _sleep_seconds(attempt: int) -> float:
    """``min(cap, base * 2^(n-1)) * random(0.5, 1.0)``."""
    raw = min(_BACKOFF_CAP, _BACKOFF_BASE * (2 ** (attempt - 1)))
    jitter = random.uniform(0.5, 1.0)  # noqa: S311 - jitter, not crypto
    return float(raw * jitter)


def _call_with_retry(func: Any, description: str) -> Any:
    """Run a Glue call, retrying only on transient conditions."""
    last: ClientError | None = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            return func()
        except ClientError as exc:
            code = _error_code(exc)
            if code not in _RETRYABLE:
                raise
            last = exc
            if attempt < _MAX_ATTEMPTS:
                time.sleep(_sleep_seconds(attempt))
    raise GlueError(
        f"Glue unavailable after {_MAX_ATTEMPTS} attempts in {description}: "
        f"{_error_code(last) if last else 'unknown'}"
    )


# -- database ---------------------------------------------------------------


def ensure_database(target: GlueTarget, *, create: bool, dry_run: bool) -> bool:
    """``True`` if created now. ``GlueError`` if missing and ``create=False``."""
    client = _glue_client(target)
    kwargs: dict[str, Any] = {"Name": target.database}
    if target.catalog_id is not None:
        kwargs["CatalogId"] = target.catalog_id

    try:
        client.get_database(**kwargs)
        return False
    except ClientError as exc:
        if _error_code(exc) != "EntityNotFoundException":
            raise _translate_access(exc, "GetDatabase", target.database) from exc

    if not create:
        raise GlueError(f"Glue database {target.database!r} does not exist")
    if dry_run:
        return False

    create_kwargs: dict[str, Any] = {
        "DatabaseInput": {
            "Name": target.database,
            "Description": "created by ducklake-iceberg from DuckLake",
        }
    }
    if target.catalog_id is not None:
        create_kwargs["CatalogId"] = target.catalog_id
    try:
        _call_with_retry(
            lambda: client.create_database(**create_kwargs), "CreateDatabase"
        )
        return True
    except ClientError as exc:
        if _error_code(exc) == "AlreadyExistsException":
            # created concurrently: success, not an error
            return False
        raise _translate_access(exc, "CreateDatabase", target.database) from exc


# -- table ------------------------------------------------------------------


def describe_table(target: GlueTarget) -> dict[str, object] | None:
    """Response of ``glue:GetTable``, or ``None`` if it does not exist."""
    client = _glue_client(target)
    kwargs: dict[str, Any] = {
        "DatabaseName": target.database,
        "Name": target.table,
    }
    if target.catalog_id is not None:
        kwargs["CatalogId"] = target.catalog_id
    try:
        response = client.get_table(**kwargs)
    except ClientError as exc:
        if _error_code(exc) == "EntityNotFoundException":
            return None
        raise _translate_access(
            exc, "GetTable", f"{target.database}.{target.table}"
        ) from exc
    table = response.get("Table")
    return dict(table) if table is not None else None


def _table_metadata_location(table: dict[str, object]) -> str | None:
    params = table.get("Parameters")
    if isinstance(params, dict):
        value = params.get("metadata_location")
        return str(value) if value is not None else None
    return None


def _table_type(table: dict[str, object]) -> str | None:
    params = table.get("Parameters")
    if isinstance(params, dict):
        value = params.get("table_type")
        return str(value) if value is not None else None
    return None


def _lake_formation_flag(table: dict[str, object]) -> bool | None:
    value = table.get("IsRegisteredWithLakeFormation")
    return bool(value) if value is not None else None


# -- metadata_location validation -------------------------------------------


def _validate_metadata_location(metadata_location: str, data_path: str | None) -> None:
    if not metadata_location.startswith("s3://"):
        raise GlueError(
            f"metadata_location with unsupported scheme (use s3://): "
            f"{metadata_location}"
        )
    if data_path is not None:
        norm_meta = metadata_location.rstrip("/") + "/"
        norm_data = data_path.rstrip("/") + "/"
        if norm_meta.startswith(norm_data):
            raise GlueError("metadata prefix inside the DuckLake data_path")


def _catalog_file_io(catalog: GlueCatalog, location: str) -> FileIO:
    """The same ``FileIO`` that ``register_table`` uses to read the metadata.json.

    ``_load_file_io`` is internal to PyIceberg (pinned in the lock); the
    catalog exposes the FileIO no other way, and using another FileIO could
    diverge in region/credential from what the registration itself will read.
    """
    return cast("Any", catalog)._load_file_io(location=location)  # type: ignore[no-any-return]


def _metadata_exists(catalog: GlueCatalog, metadata_location: str) -> bool:
    try:
        file_io = _catalog_file_io(catalog, metadata_location)
        return bool(file_io.new_input(metadata_location).exists())
    except Exception:
        LOG.debug("metadata.json probe failed", exc_info=True)
        return False


# -- registration -----------------------------------------------------------


def register(
    target: GlueTarget,
    metadata_location: str,
    *,
    create_database: bool = True,
    dry_run: bool = False,
    data_path: str | None = None,
    metadata: TableMetadata | None = None,
) -> RegisterResult:
    """Publish the pointer to the ``metadata.json`` in Glue.

    ``metadata``: the content of ``metadata_location`` that the caller has just
    written (``sync``). It skips the probe and the re-read of the file in S3;
    with it, the ``UpdateTable`` (compare-and-swap by ``VersionId``) is not
    confirmed by an extra ``GetTable``.
    """
    _validate_metadata_location(metadata_location, data_path)

    catalog: GlueCatalog | None = None

    def get_catalog() -> GlueCatalog:
        nonlocal catalog
        if catalog is None:
            catalog = build_catalog(target)
            if metadata is None and not _metadata_exists(catalog, metadata_location):
                raise GlueError(f"metadata.json not found at {metadata_location}")
        return catalog

    def load_metadata() -> TableMetadata:
        if metadata is not None:
            return metadata
        return _read_metadata(get_catalog(), metadata_location)

    existing = describe_table(target)
    if existing is not None:
        return _handle_existing(
            target,
            load_metadata,
            existing,
            metadata_location,
            dry_run=dry_run,
            verify=metadata is None,
        )
    catalog = get_catalog()

    if dry_run:
        return RegisterResult(
            outcome=RegisterOutcome.DRY_RUN,
            database_created=False,
            metadata_location=metadata_location,
            previous_metadata_location=None,
            lake_formation=None,
        )

    database_created = ensure_database(target, create=create_database, dry_run=False)

    try:
        _call_with_retry(
            lambda: catalog.register_table(target.identifier, metadata_location),
            "register_table",
        )
    except ClientError as exc:
        if _error_code(exc) == "AlreadyExistsException":
            # race: re-read and decide
            reread = describe_table(target)
            if reread is not None:
                return _handle_existing(
                    target,
                    load_metadata,
                    reread,
                    metadata_location,
                    database_created=database_created,
                    verify=metadata is None,
                )
        if _is_lake_formation(exc):
            raise _lake_formation_error(metadata_location) from exc
        raise _translate_access(
            exc, "CreateTable", f"{target.database}.{target.table}"
        ) from exc

    # post-registration check: re-read confirms the pointer
    reread = describe_table(target)
    published = _table_metadata_location(reread) if reread else None
    if published != metadata_location:
        raise RegisterDiverged(
            f"concurrent registration: expected {metadata_location}, "
            f"Glue has {published}"
        )

    return RegisterResult(
        outcome=RegisterOutcome.REGISTERED,
        database_created=database_created,
        metadata_location=metadata_location,
        previous_metadata_location=None,
        lake_formation=_lake_formation_flag(reread) if reread else None,
    )


def _handle_existing(
    target: GlueTarget,
    load_metadata: Callable[[], TableMetadata],
    table: dict[str, object],
    metadata_location: str,
    *,
    database_created: bool = False,
    dry_run: bool = False,
    verify: bool = True,
) -> RegisterResult:
    """Table already exists: nothing to do, advance the pointer, or divergence.

    ``verify=False`` skips the confirming ``GetTable``: the ``UpdateTable``
    with ``VersionId`` only commits over the version read, and any other writer
    in between becomes ``ConcurrentModificationException`` (handled below).
    """
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        table_type = _table_type(table)
        if table_type is None or table_type.upper() != _ICEBERG_TABLE_TYPE:
            raise GlueError(
                f"{target.database}.{target.table} exists and is not Iceberg "
                f"(table_type={table_type})"
            )
        previous = _table_metadata_location(table)
        if previous == metadata_location:
            raise TableAlreadyRegistered(
                f"{target.database}.{target.table} already published at {previous}",
                metadata_location=previous,
            )
        metadata = load_metadata()
        ancestors = {entry.metadata_file for entry in metadata.metadata_log}
        if previous not in ancestors:
            raise RegisterDiverged(
                f"{target.database}.{target.table} points to {previous}, which "
                f"is not an ancestor of {metadata_location}: there was a write "
                f"outside ducklake-iceberg or an export from another state",
                hint="the Iceberg table is a read-only mirror; stop external writers",
            )
        if dry_run:
            return RegisterResult(
                outcome=RegisterOutcome.DRY_RUN,
                database_created=False,
                metadata_location=metadata_location,
                previous_metadata_location=previous,
                lake_formation=_lake_formation_flag(table),
            )
        try:
            _update_table(target, table, metadata, metadata_location, previous)
        except ClientError as exc:
            if _error_code(exc) != "ConcurrentModificationException":
                if _is_lake_formation(exc):
                    raise _lake_formation_error(metadata_location) from exc
                raise _translate_access(
                    exc, "UpdateTable", f"{target.database}.{target.table}"
                ) from exc
            # another writer won the compare-and-swap: re-read and decide again
            if attempt < _MAX_ATTEMPTS:
                time.sleep(_sleep_seconds(attempt))
            reread = describe_table(target)
            if reread is None:
                raise GlueError(
                    f"{target.database}.{target.table} vanished during registration"
                ) from exc
            table = reread
            continue

        reread = describe_table(target) if verify else table
        published = (
            _table_metadata_location(reread)
            if reread is not None and verify
            else metadata_location
        )
        if published != metadata_location:
            raise RegisterDiverged(
                f"concurrent registration: expected {metadata_location}, "
                f"Glue has {published}"
            )
        return RegisterResult(
            outcome=RegisterOutcome.UPDATED,
            database_created=database_created,
            metadata_location=metadata_location,
            previous_metadata_location=previous,
            lake_formation=_lake_formation_flag(reread) if reread else None,
        )
    raise GlueError(
        f"Glue under concurrent modification after {_MAX_ATTEMPTS} attempts in "
        f"{target.database}.{target.table}"
    )


def _read_metadata(catalog: GlueCatalog, metadata_location: str) -> TableMetadata:
    from pyiceberg.serializers import FromInputFile

    file_io = _catalog_file_io(catalog, metadata_location)
    return FromInputFile.table_metadata(file_io.new_input(metadata_location))


def _update_table(
    target: GlueTarget,
    table: dict[str, object],
    metadata: TableMetadata,
    metadata_location: str,
    previous: str | None,
) -> None:
    """``UpdateTable`` with ``VersionId``: fails if another writer committed first."""
    from pyiceberg.catalog.glue import _construct_table_input

    table_input = _construct_table_input(
        table_name=target.table,
        metadata_location=metadata_location,
        properties={},
        metadata=metadata,
        glue_table=cast("Any", table),
        prev_metadata_location=previous,
    )
    kwargs: dict[str, Any] = {
        "DatabaseName": target.database,
        "TableInput": table_input,
        "VersionId": str(table.get("VersionId", "")),
    }
    if target.catalog_id is not None:
        kwargs["CatalogId"] = target.catalog_id
    _glue_client(target).update_table(**kwargs)


def _translate_access(exc: ClientError, action: str, resource: str) -> GlueError:
    code = _error_code(exc)
    if code == "AccessDeniedException":
        if _is_lake_formation(exc):
            return _lake_formation_error(resource)
        return GlueError(f"access denied in {action} on {resource}")
    if code == "InvalidInputException":
        return GlueError(f"invalid input in {action}: {_error_message(exc)}")
    return GlueError(f"failure in {action} on {resource}: {code}")


def _lake_formation_error(location: str) -> LakeFormationBlocked:
    return LakeFormationBlocked(
        f"Lake Formation active at {location}: registration in Glue works, "
        f"but Athena does not run DDL (ALTER, OPTIMIZE, VACUUM) on Iceberg "
        f"tables registered in Lake Formation. Grant the ducklake-iceberg principal "
        f"lakeformation:GetDataAccess on the data location and SELECT "
        f"(or ALL) on the table to consumers; keep writes restricted to "
        f"ducklake-iceberg."
    )


# -- state reading ----------------------------------------------------------


def load_table(target: GlueTarget) -> Table | None:
    """Load the Iceberg table from Glue using GlueCatalog.

    This is the source of truth for reading state: the ``metadata.json``
    pointed to by ``metadata_location``. Use ``load_table_state`` to read the
    parsed state.

    Args:
        target: Glue target (database, table, region, profile).

    Returns:
        The loaded Iceberg table, or None if it does not exist.

    Raises:
        GlueError: On access or communication errors.
    """
    catalog = build_catalog(target)
    try:
        return catalog.load_table(target.identifier)
    except Exception as exc:
        # PyIceberg raises NoSuchTableError for a missing table
        if "NoSuchTableError" in type(exc).__name__:
            return None
        if isinstance(exc, ClientError):
            raise _translate_access(
                exc, "LoadTable", f"{target.database}.{target.table}"
            ) from exc
        raise GlueError(
            f"Failed to load table {target.database}.{target.table}: {exc}"
        ) from exc


def load_table_state(target: GlueTarget) -> SyncState:
    """Read the sync state of the Iceberg table from Glue.

    Source of truth: ``GlueCatalog.load_table(identifier).properties``, which
    comes from the ``metadata.json`` pointed to by ``metadata_location``.

    Args:
        target: Glue target (database, table, region, profile).

    Returns:
        The parsed state. If the table does not exist or has no managed
        properties, ``SyncState.is_absent`` is True.

    Raises:
        GlueError: On access or communication errors.
    """
    table = load_table(target)
    if table is None:
        # Table does not exist: absent state
        return parse_sync_state({})

    return parse_sync_state(table.properties)


def load_table_state_fast(target: GlueTarget) -> SyncState | None:
    """Fast read path via GetTable Parameters.

    **Limitations:**
    - The Parameters may be stale (PyIceberg replicates properties, but an
      external write through Athena leaves the copy outdated).
    - It can only **discard** work (table clearly up to date), never confirm a
      commit.
    - Before any write, re-read from the metadata.json (use
      ``load_table_state``).

    Recommended use: only for ``status --all-tables``, where discarding
    synced tables saves time.

    Args:
        target: Glue target (database, table, region, profile).

    Returns:
        State parsed from the Parameters, or None if the table does not exist.

    Raises:
        GlueError: On access or communication errors.
    """
    table_desc = describe_table(target)
    if table_desc is None:
        return None

    params = table_desc.get("Parameters")
    if not isinstance(params, dict):
        return parse_sync_state({})

    return parse_sync_state(params)


# -- reconciliation and guard ---------------------------------------------------


def reconcile_commit(
    target: GlueTarget,
    *,
    metadata_location: str,
    target_snapshot_id: int,
) -> bool:
    """Reconcile unknown state by checking whether our commit won.

    Args:
        target: Glue target (database, table, region, profile).
        metadata_location: Location of the metadata.json we wrote.
        target_snapshot_id: DuckLake snapshot we wanted to publish.

    Returns:
        True if our commit won, False otherwise.

    Raises:
        GlueError: On access errors.
    """
    # 1. Fresh GetTable (no cache)
    table = load_table(target)
    if table is None:
        # Table vanished during the commit
        return False

    # 2. Check metadata_location
    current_location = table.metadata_location
    if current_location == metadata_location:
        # Our commit won
        LOG.info("Reconciliation: our commit won (metadata_location match)")
        return True

    # 3. Load the current metadata.json and check properties
    state = parse_sync_state(table.properties)

    # Check whether another sync published the same snapshot
    if (
        state.snapshot_id is not None
        and state.snapshot_id >= target_snapshot_id
        and state.iceberg_snapshot_id is not None
    ):
        # Another sync already published this snapshot or a later one
        LOG.info(
            "Reconciliation: another sync published snapshot %d (we wanted %d)",
            state.snapshot_id,
            target_snapshot_id,
        )
        return False

    # Check for ducklake_iceberg.* properties (indicates it is our mirror)
    if state.iceberg_snapshot_id is None:
        # ducklake_iceberg.* properties missing: external writer
        LOG.warning(
            "Reconciliation: external writer detected "
            "(no ducklake_iceberg.* properties)"
        )
        raise DivergenceError(
            f"External writer detected: snapshot "
            f"{table.metadata.current_snapshot_id} has no ducklake_iceberg.* properties"
        )

    # Otherwise, our commit did not win
    return False


def guard_external_write(target: GlueTarget) -> None:
    """Check for an external write and abort if there was one.

    Args:
        target: Glue target (database, table, region, profile).

    Raises:
        DivergenceError: If an external write was detected (exit 3).
        GlueError: On access errors.
    """
    table = load_table(target)
    if table is None:
        # Table does not exist: no external write
        return

    state = parse_sync_state(table.properties)

    # With no properties, it is not our mirror yet
    if state.is_absent:
        return

    # Check whether current-snapshot-id == ducklake_iceberg.iceberg-snapshot-id
    current_snapshot_id = table.metadata.current_snapshot_id
    recorded_snapshot_id = state.iceberg_snapshot_id

    if (
        current_snapshot_id is not None
        and recorded_snapshot_id is not None
        and current_snapshot_id != recorded_snapshot_id
    ):
        raise DivergenceError(
            f"External write detected: current Iceberg snapshot "
            f"{current_snapshot_id} differs from the last one published by "
            f"ducklake-iceberg {recorded_snapshot_id}"
        )
