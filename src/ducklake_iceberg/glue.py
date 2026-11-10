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
- Shared bounded retry/backoff for transient conditions. Pointer updates
  reconcile uncertain outcomes and refresh ancestry/VersionId after conflicts;
  ``AccessDenied`` and ``InvalidInput`` are never retried.
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

Registration alone does **not** need ``s3:PutObject``. Export writes the separate
metadata prefix; an inlined-data flush also needs to create source Parquet
objects under DuckLake's ``data_path``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final, TypeVar, cast

from botocore.exceptions import ClientError
from pyiceberg.exceptions import TableAlreadyExistsError

from ducklake_iceberg import aws
from ducklake_iceberg.aws import s3_credential_properties, s3_endpoint_properties
from ducklake_iceberg.errors import AwsError, DivergenceError, ExitCode, NothingToDo
from ducklake_iceberg.retry import (
    RetryClass,
    RetryPolicy,
    call_with_retry,
    classify_glue_error,
    commit_with_retry,
)

if TYPE_CHECKING:
    from pyiceberg.catalog.glue import GlueCatalog
    from pyiceberg.io import FileIO
    from pyiceberg.table.metadata import TableMetadata

LOG = logging.getLogger("ducklake_iceberg")

#: value written to `table_type` by PyIceberg (`ICEBERG.upper()`).
_ICEBERG_TABLE_TYPE: Final[str] = "ICEBERG"

#: transient conditions that deserve a retry with backoff.
GLUE_RETRY: Final = RetryPolicy()
T = TypeVar("T")


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


def _call_with_retry(func: Callable[[], T], description: str) -> T:
    try:
        return call_with_retry(func, phase="glue", policy=GLUE_RETRY)
    except Exception as exc:
        if classify_glue_error(exc) is RetryClass.FATAL:
            raise
        raise GlueError(
            f"Glue unavailable after {GLUE_RETRY.max_attempts} attempts "
            f"in {description}: {type(exc).__name__}: {exc}"
        ) from exc


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


def _require_iceberg(target: GlueTarget, table: dict[str, object]) -> None:
    table_type = _table_type(table)
    if table_type is None or table_type.upper() != _ICEBERG_TABLE_TYPE:
        raise GlueError(
            f"{target.database}.{target.table} exists and is not Iceberg "
            f"(table_type={table_type})"
        )


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
    except (ClientError, TableAlreadyExistsError) as exc:
        if (
            isinstance(exc, TableAlreadyExistsError)
            or _error_code(exc) == "AlreadyExistsException"
        ):
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
        if isinstance(exc, TableAlreadyExistsError):
            raise GlueError(
                f"{target.database}.{target.table} already exists but "
                "GetTable did not return it; retry after verifying catalog visibility"
            ) from exc
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
    _require_iceberg(target, table)
    initial_previous = _table_metadata_location(table)
    if initial_previous == metadata_location:
        raise TableAlreadyRegistered(
            f"{target.database}.{target.table} already published at {initial_previous}",
            metadata_location=initial_previous,
        )
    metadata = load_metadata()

    def guard() -> None:
        _require_iceberg(target, table)
        previous = _table_metadata_location(table)
        if previous == metadata_location:
            raise TableAlreadyRegistered(
                f"{target.database}.{target.table} already published at {previous}",
                metadata_location=previous,
            )
        ancestors = {entry.metadata_file for entry in metadata.metadata_log}
        if previous not in ancestors:
            raise RegisterDiverged(
                f"{target.database}.{target.table} points to {previous}, which "
                f"is not an ancestor of {metadata_location}: there was a write "
                f"outside ducklake-iceberg or an export from another state",
                hint="the Iceberg table is a read-only mirror; stop external writers",
            )

    def commit(plan: dict[str, object]) -> RegisterResult:
        previous = _table_metadata_location(plan)
        _update_table(target, plan, metadata, metadata_location, previous)
        reread = describe_table(target) if verify else plan
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

    def reconcile(_plan: dict[str, object]) -> bool:
        nonlocal table
        reread = describe_table(target)
        if reread is None:
            raise GlueError(
                f"{target.database}.{target.table} vanished during registration"
            )
        table = reread
        if (
            str(_table_type(table)).upper() == _ICEBERG_TABLE_TYPE
            and _table_metadata_location(table) == metadata_location
        ):
            return True
        guard()
        return False

    guard()
    if dry_run:
        return RegisterResult(
            outcome=RegisterOutcome.DRY_RUN,
            database_created=False,
            metadata_location=metadata_location,
            previous_metadata_location=initial_previous,
            lake_formation=_lake_formation_flag(table),
        )
    try:
        result = commit_with_retry(
            lambda: table,
            commit,
            policy=GLUE_RETRY,
            reconcile_fn=reconcile,
            guard_fn=guard,
        )
    except ClientError as exc:
        raise _translate_access(
            exc, "UpdateTable", f"{target.database}.{target.table}"
        ) from exc
    if result is not None:
        return result
    return RegisterResult(
        outcome=RegisterOutcome.UPDATED,
        database_created=database_created,
        metadata_location=metadata_location,
        previous_metadata_location=initial_previous,
        lake_formation=_lake_formation_flag(table),
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
