"""Exit code contract of the exception hierarchy."""

from __future__ import annotations

import pytest

from ducklake_iceberg.errors import (
    AwsError,
    CatalogError,
    CompatibilityError,
    ConfigError,
    DivergenceError,
    DuckLakeIcebergError,
    ExitCode,
    IdentityError,
    NothingToDo,
)


def test_exit_code_values() -> None:
    assert ExitCode.SUCCESS.value == 0
    assert ExitCode.ERROR.value == 1
    assert ExitCode.BLOCKED.value == 2
    assert ExitCode.DIVERGED.value == 3
    assert ExitCode.NOTHING_TO_DO.value == 4


@pytest.mark.parametrize(
    ("exc_type", "expected"),
    [
        (DuckLakeIcebergError, ExitCode.ERROR),
        (ConfigError, ExitCode.ERROR),
        (CatalogError, ExitCode.ERROR),
        (IdentityError, ExitCode.ERROR),
        (AwsError, ExitCode.ERROR),
        (CompatibilityError, ExitCode.BLOCKED),
        (DivergenceError, ExitCode.DIVERGED),
        (NothingToDo, ExitCode.NOTHING_TO_DO),
    ],
)
def test_exit_code_per_exception(
    exc_type: type[DuckLakeIcebergError], expected: ExitCode
) -> None:
    exc = exc_type("boom")
    assert exc.exit_code == expected


def test_message_and_hint() -> None:
    exc = ConfigError("missing DSN", hint="set CATALOG_DSN")
    assert exc.message == "missing DSN"
    assert exc.hint == "set CATALOG_DSN"
    assert str(exc) == "missing DSN"


def test_hint_defaults_to_none() -> None:
    assert DuckLakeIcebergError("x").hint is None


def test_identity_is_a_catalog_error() -> None:
    assert issubclass(IdentityError, CatalogError)


def test_new_subclass_without_exit_code_inherits_error() -> None:
    class BrandNewError(DuckLakeIcebergError):
        pass

    assert BrandNewError("x").exit_code == ExitCode.ERROR
    assert BrandNewError("x").exit_code != ExitCode.SUCCESS
