"""Translation of DuckDB errors into tool errors, without opening a connection."""

from __future__ import annotations

from unittest import mock

import duckdb
import pytest

from ducklake_iceberg.catalog.connection import (
    DuckLakeConnection,
    Environment,
    _translate,
)
from ducklake_iceberg.config import DuckLakeIcebergSettings
from ducklake_iceberg.errors import AwsError, CatalogError


@pytest.mark.parametrize(
    ("message", "expected_type", "fragment"),
    [
        (
            'Cannot execute statement of type "CREATE" on database "lake" '
            "which is attached in read-only mode!",
            CatalogError,
            "READ_ONLY",
        ),
        (
            'Secret provider "credential_chain" for type "s3" does not exist, '
            "but it exists in the aws extension",
            CatalogError,
            "aws",
        ),
        (
            "Secret Validation Failure: no profile 'x' found in config file "
            "~/.aws/config",
            AwsError,
            "AWS profile",
        ),
        (
            'Failed to probe DuckLake metadata: Unable to open database "/x"',
            CatalogError,
            "catalog unreachable",
        ),
        (
            "cannot start a transaction within a transaction",
            CatalogError,
            "nested",
        ),
        (
            "Catalog Error: Table with name foo does not exist!",
            CatalogError,
            "foo",
        ),
    ],
)
def test_translate_branches(
    message: str, expected_type: type[Exception], fragment: str
) -> None:
    result = _translate(duckdb.Error(message), "SELECT 1")
    assert isinstance(result, expected_type)
    assert fragment in result.message


def test_translate_http_exception_is_aws_error() -> None:
    result = _translate(duckdb.HTTPException("s3 http failure 403"), "SELECT 1")
    assert isinstance(result, AwsError)
    assert "S3 access failure" in result.message


def test_translate_preserves_generic_message() -> None:
    result = _translate(duckdb.Error("something unexpected"), "SELECT 1")
    assert isinstance(result, CatalogError)
    assert result.message == "something unexpected"


@pytest.mark.parametrize("missing", [True, False])
def test_extension_install_only_recovers_a_missing_extension(
    missing: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    con = DuckLakeConnection(
        DuckLakeIcebergSettings(
            catalog_dsn="ducklake:/catalog",
            data_path="/data/",
            iceberg_metadata_prefix="/iceberg/",
            glue_database="analytics",
            aws_region="us-east-1",
        )
    )
    raw = mock.Mock()
    error = duckdb.IOException(
        'Extension "/extensions/ducklake.duckdb_extension" not found. '
        'Install it first using "INSTALL ducklake".'
        if missing
        else "Extension version mismatch"
    )
    raw.execute.side_effect = [error, None, None]
    con._con = raw
    monkeypatch.setattr(con, "_required_extensions", lambda: ("ducklake",))
    monkeypatch.setattr(
        con,
        "environment",
        lambda: Environment("v2.0.0", {"ducklake": True}, (), (), True),
    )
    if missing:
        con._load_extensions()
        assert raw.execute.call_args_list == [
            mock.call("LOAD ducklake"),
            mock.call("INSTALL ducklake"),
            mock.call("LOAD ducklake"),
        ]
    else:
        with pytest.raises(CatalogError, match="could not load extension"):
            con._load_extensions()
        raw.execute.assert_called_once_with("LOAD ducklake")
