from __future__ import annotations

import enum


class ExitCode(enum.IntEnum):
    """Process exit codes of the tool."""

    SUCCESS = 0
    ERROR = 1
    BLOCKED = 2
    DIVERGED = 3
    NOTHING_TO_DO = 4


class DuckLakeIcebergError(Exception):
    """Base class of all handled errors of the tool.

    ``exit_code`` is the process exit code and ``problem_code`` the stable
    ``problems[].code`` of the ``--json`` envelope; both are part of the public
    contract and are declared per class, never derived from the message.
    """

    exit_code: ExitCode = ExitCode.ERROR
    problem_code: str = "error"

    def __init__(self, message: str, *, hint: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.hint = hint


class ConfigError(DuckLakeIcebergError):
    """Missing, invalid, or inconsistent configuration."""

    problem_code = "config_invalid"


class CatalogError(DuckLakeIcebergError):
    """Connection, ATTACH, or catalog read failure."""


class IdentityError(CatalogError):
    """Table, schema, or snapshot missing from the catalog."""

    problem_code = "table_not_found"


class AwsError(DuckLakeIcebergError):
    """Credential, S3, or Glue failure."""

    problem_code = "aws_error"


class ProfileNotFoundError(AwsError):
    """The configured AWS profile does not exist."""

    problem_code = "profile_not_found"


class CompatibilityError(DuckLakeIcebergError):
    """Iceberg v2 compatibility blocker."""

    problem_code = "blocked"

    exit_code = ExitCode.BLOCKED


class DivergenceError(DuckLakeIcebergError):
    """DuckLake and Iceberg have diverged."""

    problem_code = "catalog_diverged"

    exit_code = ExitCode.DIVERGED


class NothingToDo(DuckLakeIcebergError):
    """Idempotent operation with no effect."""

    problem_code = "nothing_to_do"

    exit_code = ExitCode.NOTHING_TO_DO
