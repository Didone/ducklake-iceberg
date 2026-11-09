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
    """Base class of all handled errors of the tool."""

    exit_code: ExitCode = ExitCode.ERROR

    def __init__(self, message: str, *, hint: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.hint = hint


class ConfigError(DuckLakeIcebergError):
    """Missing, invalid, or inconsistent configuration."""


class CatalogError(DuckLakeIcebergError):
    """Connection, ATTACH, or catalog read failure."""


class IdentityError(CatalogError):
    """Table, schema, or snapshot missing from the catalog."""


class AwsError(DuckLakeIcebergError):
    """Credential, S3, or Glue failure."""


class CompatibilityError(DuckLakeIcebergError):
    """Iceberg v2 compatibility blocker."""

    exit_code = ExitCode.BLOCKED


class DivergenceError(DuckLakeIcebergError):
    """DuckLake and Iceberg have diverged."""

    exit_code = ExitCode.DIVERGED


class NothingToDo(DuckLakeIcebergError):
    """Idempotent operation with no effect."""

    exit_code = ExitCode.NOTHING_TO_DO


class OperationAborted(DuckLakeIcebergError):
    """Operation canceled by the user at the interactive confirmation."""

    exit_code = ExitCode.SUCCESS
