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
    ProfileNotFoundError,
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


@pytest.mark.parametrize(
    ("exc_type", "code"),
    [
        (DuckLakeIcebergError, "error"),
        (CatalogError, "error"),
        (ConfigError, "config_invalid"),
        (IdentityError, "table_not_found"),
        (AwsError, "aws_error"),
        (ProfileNotFoundError, "profile_not_found"),
        (CompatibilityError, "blocked"),
        (DivergenceError, "catalog_diverged"),
        (NothingToDo, "nothing_to_do"),
    ],
)
def test_problem_code_per_exception(
    exc_type: type[DuckLakeIcebergError], code: str
) -> None:
    assert exc_type("boom").problem_code == code


def test_problem_code_does_not_depend_on_the_message() -> None:
    """A message mentioning a profile or 'not found' keeps its class code."""
    from ducklake_iceberg.pipeline import problem_from_error

    assert problem_from_error(ConfigError("configuration file not found")).code == (
        "config_invalid"
    )
    assert problem_from_error(AwsError("profile 'x' lacks s3:GetObject")).code == (
        "aws_error"
    )

    class ForeignError(Exception):
        exit_code = 3

    assert problem_from_error(ForeignError("x")).code == "catalog_diverged"
    assert problem_from_error(ValueError("not found")).code == "error"
