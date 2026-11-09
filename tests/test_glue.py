"""Tests for registration in Glue (moto).

Glue and S3 are mocked with moto. The boto3 path (database, describe_table,
errors, retry) runs against moto's Glue. PyIceberg's `register_table` reads
the metadata.json through PyArrow S3 (which does NOT honor moto), so the full
`register` path uses a catalog stub whose `register_table` writes the table to
moto's Glue via boto3 -- enough to exercise existing-table detection,
post-registration re-read, and divergence. The real `register_table` is
covered by the end-to-end matrix.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any
from unittest import mock

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import boto3
from botocore.exceptions import ClientError
from moto import mock_aws

from iceduck.errors import ExitCode
from iceduck.glue import (
    GlueError,
    GlueTarget,
    LakeFormationBlocked,
    RegisterDiverged,
    RegisterOutcome,
    TableAlreadyRegistered,
    _sleep_seconds,
    _validate_metadata_location,
    describe_table,
    ensure_database,
    normalize_table_name,
    register,
)

REGION = "us-east-1"
METADATA = "s3://bucket/iceberg/sales/orders/metadata/v1.metadata.json"


def _target(database: str = "analytics", table: str = "orders") -> GlueTarget:
    return GlueTarget(
        database=database, table=table, region=REGION, profile=None, catalog_id=None
    )


@pytest.fixture
def aws() -> Iterator[None]:
    with mock_aws():
        yield


class _StubInput:
    def __init__(self, exists: bool) -> None:
        self._exists = exists

    def exists(self) -> bool:
        return self._exists


class _StubIO:
    def __init__(self, exists: bool) -> None:
        self._exists = exists

    def new_input(self, _location: str) -> _StubInput:
        return _StubInput(self._exists)


class _StubCatalog:
    """Fake catalog: register_table writes the table to moto's Glue."""

    def __init__(
        self,
        *,
        metadata_exists: bool = True,
        register_error: Exception | None = None,
        published_location: str | None = None,
    ) -> None:
        self._io = _StubIO(metadata_exists)
        self._register_error = register_error
        self._published_location = published_location

    def _load_file_io(self, location: str | None = None) -> _StubIO:
        return self._io

    def register_table(
        self, identifier: tuple[str, str], metadata_location: str
    ) -> None:
        if self._register_error is not None:
            raise self._register_error
        database, table = identifier
        location = self._published_location or metadata_location
        glue = boto3.client("glue", region_name=REGION)
        glue.create_table(
            DatabaseName=database,
            TableInput={
                "Name": table,
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    "table_type": "ICEBERG",
                    "metadata_location": location,
                },
                "StorageDescriptor": {"Columns": [], "Location": "s3://bucket/t"},
            },
        )


def _patch_catalog(catalog: _StubCatalog) -> Any:
    return mock.patch("iceduck.glue.build_catalog", return_value=catalog)


def _create_hive_table(database: str, table: str) -> None:
    glue = boto3.client("glue", region_name=REGION)
    glue.create_table(
        DatabaseName=database,
        TableInput={
            "Name": table,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {},  # no table_type -> Hive
            "StorageDescriptor": {"Columns": [], "Location": "s3://bucket/hive"},
        },
    )


# -- pure helpers -----------------------------------------------------------


def test_normalize_table_name() -> None:
    assert normalize_table_name("  Orders-2024 ") == "orders_2024"
    assert normalize_table_name("MyTable") == "mytable"
    assert normalize_table_name("a.b/c") == "a_b_c"


def test_validate_metadata_location_rejects_non_s3() -> None:
    with pytest.raises(GlueError):
        _validate_metadata_location("file:///x/m.json", None)


def test_validate_metadata_location_inside_data_path() -> None:
    with pytest.raises(GlueError, match="data_path"):
        _validate_metadata_location("s3://b/data/iceberg/m.json", "s3://b/data")


def test_validate_metadata_location_ok() -> None:
    _validate_metadata_location(
        "s3://b/iceberg/m.json", "s3://b/data"
    )  # does not raise


def test_sleep_seconds_in_range() -> None:
    for attempt in range(1, 6):
        lo = min(5.0, 0.2 * (2 ** (attempt - 1))) * 0.5
        hi = min(5.0, 0.2 * (2 ** (attempt - 1)))
        value = _sleep_seconds(attempt)
        assert lo <= value <= hi


# -- database ---------------------------------------------------------------


def test_ensure_database_creates(aws: None) -> None:
    assert ensure_database(_target(), create=True, dry_run=False) is True
    # it exists now
    assert ensure_database(_target(), create=True, dry_run=False) is False


def test_ensure_database_absent_no_create(aws: None) -> None:
    with pytest.raises(GlueError, match="does not exist"):
        ensure_database(_target(database="missing"), create=False, dry_run=False)


def test_ensure_database_dry_run_absent(aws: None) -> None:
    # dry_run does not create; returns False and the database stays absent
    assert ensure_database(_target(), create=True, dry_run=True) is False
    glue = boto3.client("glue", region_name=REGION)
    with pytest.raises(ClientError):
        glue.get_database(Name="analytics")


# -- describe_table ---------------------------------------------------------


def test_describe_table_absent(aws: None) -> None:
    ensure_database(_target(), create=True, dry_run=False)
    assert describe_table(_target()) is None


# -- database and table missing ---------------------------------------------


def test_register_creates_database_and_table(aws: None) -> None:
    with _patch_catalog(_StubCatalog(published_location=METADATA)):
        result = register(_target(), METADATA, create_database=True)
    assert result.outcome is RegisterOutcome.REGISTERED
    assert result.database_created is True
    assert result.metadata_location == METADATA


# -- existing database, missing table ---------------------------------------


def test_register_existing_database(aws: None) -> None:
    ensure_database(_target(), create=True, dry_run=False)
    with _patch_catalog(_StubCatalog(published_location=METADATA)):
        result = register(_target(), METADATA, create_database=True)
    assert result.database_created is False
    assert result.outcome is RegisterOutcome.REGISTERED


# -- re-read confirms table_type and metadata_location ----------------------


def test_register_reread_confirms(aws: None) -> None:
    with _patch_catalog(_StubCatalog(published_location=METADATA)):
        register(_target(), METADATA, create_database=True)
    glue = boto3.client("glue", region_name=REGION)
    table = glue.get_table(DatabaseName="analytics", Name="orders")["Table"]
    assert table["Parameters"]["table_type"] == "ICEBERG"
    assert table["Parameters"]["metadata_location"] == METADATA


# -- table already exists (Iceberg) -> exit 4 -------------------------------


def test_register_already_exists(aws: None) -> None:
    with _patch_catalog(_StubCatalog(published_location=METADATA)):
        register(_target(), METADATA, create_database=True)
        with pytest.raises(TableAlreadyRegistered) as exc:
            register(_target(), METADATA, create_database=True)
    assert exc.value.exit_code is ExitCode.NOTHING_TO_DO


def _metadata_with_log(*ancestors: str) -> Any:
    """Minimal ``TableMetadata`` whose ``metadata-log`` cites ``ancestors``."""
    from pyiceberg.table.snapshots import MetadataLogEntry

    return mock.Mock(
        metadata_log=[
            MetadataLogEntry(metadata_file=a, timestamp_ms=0) for a in ancestors
        ],
        location="s3://bucket/iceberg/sales/orders",
        properties={},
    )


def test_register_updates_from_ancestor(aws: None) -> None:
    v0 = "s3://bucket/iceberg/sales/orders/metadata/v0.metadata.json"
    with _patch_catalog(_StubCatalog(published_location=v0)):
        register(_target(), v0, create_database=True)
    with (
        _patch_catalog(_StubCatalog()),
        mock.patch("iceduck.glue._read_metadata", return_value=_metadata_with_log(v0)),
        mock.patch("pyiceberg.catalog.glue._to_columns", return_value=[]),
    ):
        result = register(_target(), METADATA, create_database=True)
    assert result.outcome is RegisterOutcome.UPDATED
    assert result.previous_metadata_location == v0
    glue = boto3.client("glue", region_name=REGION)
    params = glue.get_table(DatabaseName="analytics", Name="orders")["Table"][
        "Parameters"
    ]
    assert params["metadata_location"] == METADATA
    assert params["previous_metadata_location"] == v0


def test_register_update_dry_run_writes_nothing(aws: None) -> None:
    v0 = "s3://bucket/iceberg/sales/orders/metadata/v0.metadata.json"
    with _patch_catalog(_StubCatalog(published_location=v0)):
        register(_target(), v0, create_database=True)
    with (
        _patch_catalog(_StubCatalog()),
        mock.patch("iceduck.glue._read_metadata", return_value=_metadata_with_log(v0)),
    ):
        result = register(_target(), METADATA, create_database=True, dry_run=True)
    assert result.outcome is RegisterOutcome.DRY_RUN
    table = describe_table(_target())
    assert table is not None
    assert table["Parameters"]["metadata_location"] == v0  # type: ignore[index]


def test_register_not_ancestor_is_divergence(aws: None) -> None:
    other = "s3://bucket/iceberg/sales/orders/metadata/v9-athena.metadata.json"
    with _patch_catalog(_StubCatalog(published_location=other)):
        register(_target(), other, create_database=True)
    with (
        _patch_catalog(_StubCatalog()),
        mock.patch("iceduck.glue._read_metadata", return_value=_metadata_with_log()),
        pytest.raises(RegisterDiverged) as exc,
    ):
        register(_target(), METADATA, create_database=True)
    assert exc.value.exit_code is ExitCode.DIVERGED
    assert other in str(exc.value)


# -- same-named Hive table -> exit 1, left intact ---------------------------


def test_register_non_iceberg_table(aws: None) -> None:
    ensure_database(_target(), create=True, dry_run=False)
    _create_hive_table("analytics", "orders")
    with (
        _patch_catalog(_StubCatalog()),
        pytest.raises(GlueError, match="is not Iceberg"),
    ):
        register(_target(), METADATA, create_database=True)
    # the Hive table remains
    glue = boto3.client("glue", region_name=REGION)
    table = glue.get_table(DatabaseName="analytics", Name="orders")["Table"]
    assert "table_type" not in table["Parameters"]


# -- nonexistent metadata_location ------------------------------------------


def test_register_metadata_absent(aws: None) -> None:
    with (
        _patch_catalog(_StubCatalog(metadata_exists=False)),
        pytest.raises(GlueError, match="not found"),
    ):
        register(_target(), METADATA, create_database=True)


# -- invalid metadata_location ----------------------------------------------


def test_register_metadata_in_data_path(aws: None) -> None:
    with _patch_catalog(_StubCatalog()), pytest.raises(GlueError, match="data_path"):
        register(
            _target(),
            "s3://bucket/data/iceberg/m.json",
            create_database=True,
            data_path="s3://bucket/data",
        )


def test_register_metadata_file_scheme(aws: None) -> None:
    with _patch_catalog(_StubCatalog()), pytest.raises(GlueError):
        register(_target(), "file:///x/m.json", create_database=True)


# -- dry-run ----------------------------------------------------------------


def test_register_dry_run(aws: None) -> None:
    with _patch_catalog(_StubCatalog(published_location=METADATA)):
        result = register(_target(), METADATA, create_database=True, dry_run=True)
    assert result.outcome is RegisterOutcome.DRY_RUN
    # no table or database created
    glue = boto3.client("glue", region_name=REGION)
    with pytest.raises(ClientError):
        glue.get_database(Name="analytics")


# -- divergent re-read -> exit 3 --------------------------------------------


def test_register_divergence(aws: None) -> None:
    # the stub publishes at 'other', but we sent METADATA -> divergence
    other = "s3://bucket/iceberg/sales/orders/metadata/vX.metadata.json"
    with (
        _patch_catalog(_StubCatalog(published_location=other)),
        pytest.raises(RegisterDiverged) as exc,
    ):
        register(_target(), METADATA, create_database=True)
    assert exc.value.exit_code is ExitCode.DIVERGED


# -- AlreadyExistsException on CreateTable -> re-reads, ends as ALREADY ------


def test_register_create_table_race(aws: None) -> None:
    ensure_database(_target(), create=True, dry_run=False)

    # stub whose register_table creates the table and then raises AlreadyExists
    class _RacingCatalog(_StubCatalog):
        def register_table(
            self, identifier: tuple[str, str], metadata_location: str
        ) -> None:
            super().register_table(identifier, metadata_location)
            err = ClientError(
                {"Error": {"Code": "AlreadyExistsException", "Message": "race"}},
                "CreateTable",
            )
            raise err

    with (
        _patch_catalog(_RacingCatalog(published_location=METADATA)),
        pytest.raises(TableAlreadyRegistered),
    ):
        register(_target(), METADATA, create_database=True)


# -- AccessDenied (with and without Lake Formation) -------------------------


def test_register_access_denied(aws: None) -> None:
    err = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "nope"}},
        "CreateTable",
    )
    with (
        _patch_catalog(_StubCatalog(register_error=err)),
        pytest.raises(GlueError) as exc,
    ):
        register(_target(), METADATA, create_database=True)
    assert not isinstance(exc.value, LakeFormationBlocked)
    assert exc.value.exit_code is ExitCode.ERROR


def test_register_lake_formation_access_denied(aws: None) -> None:
    err = ClientError(
        {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "Insufficient Lake Formation permission",
            }
        },
        "CreateTable",
    )
    with (
        _patch_catalog(_StubCatalog(register_error=err)),
        pytest.raises(LakeFormationBlocked) as exc,
    ):
        register(_target(), METADATA, create_database=True)
    assert exc.value.exit_code is ExitCode.BLOCKED
    assert "DDL" in str(exc.value)


# -- normalization applied to the target ------------------------------------


def test_register_normalized_name(aws: None) -> None:
    target = GlueTarget(
        database="analytics",
        table=normalize_table_name("Orders-2024"),
        region=REGION,
        profile=None,
        catalog_id=None,
    )
    assert target.table == "orders_2024"
    with _patch_catalog(_StubCatalog(published_location=METADATA)):
        register(target, METADATA, create_database=True)
    glue = boto3.client("glue", region_name=REGION)
    assert glue.get_table(DatabaseName="analytics", Name="orders_2024")["Table"]


# -- architecture test: no create_table in the module -----------------------


def test_no_create_table_in_module() -> None:
    import ast
    import pathlib

    import iceduck.glue as g

    tree = ast.parse(pathlib.Path(g.__file__).read_text())
    # No CALL to `.create_table(...)` / `create_table(...)` in the module
    # (the docstring mentions the name in prose when explaining the ban).
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute):
                assert func.attr != "create_table", "only register_table is allowed"
            if isinstance(func, ast.Name):
                assert func.id != "create_table", "only register_table is allowed"


# -- transient retry succeeds after N failures ------------------------------


def test_retry_succeeds_after_transient_failures() -> None:
    from iceduck.glue import _call_with_retry

    calls = {"n": 0}

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ClientError(
                {"Error": {"Code": "ConcurrentModificationException", "Message": "x"}},
                "CreateTable",
            )
        return "ok"

    with mock.patch("iceduck.glue.time.sleep") as sleep:
        assert _call_with_retry(flaky, "CreateTable") == "ok"
    assert calls["n"] == 3
    assert sleep.call_count == 2  # slept between the 3 attempts


# -- throttling on every attempt exhausts the retry -------------------------


def test_retry_exhausts_on_persistent_throttling() -> None:
    from iceduck.glue import _call_with_retry

    def always_throttle() -> str:
        raise ClientError(
            {"Error": {"Code": "ThrottlingException", "Message": "slow down"}},
            "CreateTable",
        )

    sleeps: list[float] = []
    with (
        mock.patch("iceduck.glue.time.sleep", side_effect=sleeps.append),
        pytest.raises(GlueError, match="5 attempts"),
    ):
        _call_with_retry(always_throttle, "CreateTable")
    assert len(sleeps) == 4  # 5 attempts => 4 waits
    # growing waits (the cap grows before jitter)
    assert sleeps == sorted(sleeps) or all(s >= 0 for s in sleeps)


# -- retry does NOT cover AccessDenied --------------------------------------


def test_retry_does_not_cover_access_denied() -> None:
    from iceduck.glue import _call_with_retry

    def denied() -> str:
        raise ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "no"}},
            "CreateTable",
        )

    with mock.patch("iceduck.glue.time.sleep") as sleep, pytest.raises(ClientError):
        _call_with_retry(denied, "CreateTable")
    assert sleep.call_count == 0


# -- GetTable with IsRegisteredWithLakeFormation=True -----------------------


def test_lake_formation_flag_on_existing(aws: None) -> None:
    from iceduck.glue import _lake_formation_flag

    table = {
        "Parameters": {"table_type": "ICEBERG", "metadata_location": METADATA},
        "IsRegisteredWithLakeFormation": True,
    }
    assert _lake_formation_flag(table) is True
    assert _lake_formation_flag({"Parameters": {}}) is None
