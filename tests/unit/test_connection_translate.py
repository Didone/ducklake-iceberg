"""Translation of DuckDB errors into iceduck errors, without opening a connection."""

from __future__ import annotations

import duckdb
import pytest

from iceduck.catalog.connection import _translate
from iceduck.errors import AwsError, CatalogError


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
