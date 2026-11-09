"""CLI tests using CliRunner + moto.

The SAME suite runs against SQLite and Postgres via `seeded_catalog`/`catalog`.
AWS is simulated with moto. Marked `integration` and `aws`.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from moto import mock_aws
from typer.testing import CliRunner

from ducklake_iceberg.cli import app
from tests.integration.conftest import CatalogHarness, cli_env_for

pytestmark = [pytest.mark.integration, pytest.mark.aws]

runner = CliRunner()


def _json(output: str) -> dict[str, Any]:
    payload: dict[str, Any] = json.loads(output)
    return payload


# --- help lists only sync (backend independent) ------------------------------
def test_help_lists_only_sync() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "sync" in result.stdout
    for removed in (
        "doctor",
        "checkpoint",
        "export",
        "register",
        "status",
        "validate",
        "clean",
    ):
        assert f"│ {removed} " not in result.stdout


@pytest.mark.parametrize(
    "removed", ["doctor", "checkpoint", "export", "register", "status", "validate"]
)
def test_removed_commands_are_usage_errors(removed: str) -> None:
    result = runner.invoke(app, [removed, "--table", "main.t"])
    assert result.exit_code == 2
    assert result.stdout.strip() == ""


def test_sync_help_lists_flags() -> None:
    result = runner.invoke(app, ["sync", "--help"])
    assert result.exit_code == 0
    for flag in (
        "--table",
        "--dry-run",
        "--json",
        "--profile",
        "--log-level",
        "--log-format",
        "--config",
    ):
        assert flag in result.stdout
    for removed in ("--all-tables", "--metrics", "--compact", "--yes", "--no-lock"):
        assert removed not in result.stdout


def test_removed_flag_is_a_usage_error() -> None:
    result = runner.invoke(app, ["sync", "--table", "main.t", "--all-tables"])
    assert result.exit_code == 2


# --- --version ---------------------------------------------------------------
def test_version() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert result.stdout.strip().startswith("ducklake-iceberg ")
    assert "(duckdb " in result.stdout
    assert ", pyiceberg " in result.stdout


def test_table_is_required() -> None:
    result = runner.invoke(app, ["sync", "--json"])
    assert result.exit_code == 2


# --- sync --json (includes the config field = redacted()) --------------------
@pytest.mark.usefixtures("aws_credentials")
def test_sync_json(flushed_catalog: CatalogHarness) -> None:
    env = cli_env_for(flushed_catalog)
    with mock_aws():
        result = runner.invoke(
            app, ["sync", "--table", "main.pedidos", "--dry-run", "--json"], env=env
        )
    assert result.exit_code == 0, result.stdout
    payload = _json(result.stdout)
    assert payload["version"] == 1
    assert payload["command"] == "sync"
    assert payload["status"] == "ok"
    assert len(payload["results"]) == 1
    assert payload["results"][0]["table"] == "main.pedidos"
    assert payload["results"][0]["steps"][0]["step"] == "doctor"
    assert "metrics" in payload["results"][0]
    # redacted() is the `config` payload in the envelope, without the host
    assert payload["config"] is not None
    assert payload["config"]["glue_database"] == "analytics"
    assert "host=***" in payload["config"]["catalog_dsn"] or (
        "host=" not in payload["config"]["catalog_dsn"]
    )


# --- flag order: options after the subcommand --------------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_log_level_flag(flushed_catalog: CatalogHarness) -> None:
    env = cli_env_for(flushed_catalog)
    with mock_aws():
        result = runner.invoke(
            app,
            ["sync", "--table", "main.pedidos", "--dry-run", "--log-level", "DEBUG"],
            env=env,
        )
    assert result.exit_code == 0


# --- unqualified table -------------------------------------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_unqualified_table_fails(seeded_catalog: CatalogHarness) -> None:
    env = cli_env_for(seeded_catalog)
    with mock_aws():
        result = runner.invoke(app, ["sync", "--table", "pedidos", "--json"], env=env)
    assert result.exit_code == 1
    payload = _json(result.stdout)
    assert payload["status"] == "error"
    assert payload["problems"]


# --- table that does not exist -----------------------------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_missing_table_fails(seeded_catalog: CatalogHarness) -> None:
    env = cli_env_for(seeded_catalog)
    with mock_aws():
        result = runner.invoke(
            app, ["sync", "--table", "main.nope", "--json", "--dry-run"], env=env
        )
    assert result.exit_code == 1
    assert _json(result.stdout)["problems"][0]["table"] == "main.nope"


# --- invalid catalog_dsn with --json -----------------------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_invalid_dsn_emits_error_envelope(seeded_catalog: CatalogHarness) -> None:
    env = {
        **cli_env_for(seeded_catalog),
        "DUCKLAKE_ICEBERG_CATALOG_DSN": "ducklake:sqlite:/does/not/exist.sqlite",
    }
    with mock_aws():
        result = runner.invoke(
            app, ["sync", "--table", "main.pedidos", "--json"], env=env
        )
    assert result.exit_code == 1
    payload = _json(result.stdout)  # stdout is still valid JSON
    assert payload["status"] == "error"


# --- unknown command ---------------------------------------------------------
def test_unknown_command() -> None:
    result = runner.invoke(app, ["no-such-command"])
    assert result.exit_code == 2
    assert (
        result.stdout.strip() == ""
    )  # usage errors write nothing to stdout (no JSON envelope)


# --- nonexistent AWS profile -------------------------------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_bad_aws_profile(seeded_catalog: CatalogHarness) -> None:
    env = {
        **cli_env_for(seeded_catalog),
        "DUCKLAKE_ICEBERG_DATA_PATH": "s3://bucket/ducklake/data/",
        "DUCKLAKE_ICEBERG_ICEBERG_METADATA_PREFIX": "s3://bucket/iceberg/",
        "DUCKLAKE_ICEBERG_AWS_PROFILE": "nonexistent_profile_ducklake_iceberg_xyz",
    }
    with mock_aws():
        result = runner.invoke(
            app, ["sync", "--table", "main.pedidos", "--json"], env=env
        )
    assert result.exit_code == 1
    payload = _json(result.stdout)
    assert payload["problems"][0]["code"] == "profile_not_found"
    # no AWS credential material leaks (the profile name is config, not a secret)
    assert "testing" not in result.stdout  # fake AWS_SECRET/ACCESS key
    assert "aws_secret_access_key" not in result.stdout.lower()


# --- --dry-run ---------------------------------------------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_dry_run(flushed_catalog: CatalogHarness) -> None:
    env = cli_env_for(flushed_catalog)
    with mock_aws():
        result = runner.invoke(
            app,
            ["sync", "--table", "main.pedidos", "--dry-run", "--json"],
            env=env,
        )
    assert result.exit_code == 0
    payload = _json(result.stdout)
    assert payload["dry_run"] is True
    steps = [s["step"] for s in payload["results"][0]["steps"]]
    # a dry run neither flushes nor registers; it still plans the export
    assert steps == ["doctor", "export"]


@pytest.mark.usefixtures("aws_credentials")
def test_dry_run_does_not_flush(seeded_catalog: CatalogHarness) -> None:
    """Inlined rows stay inlined: the dry run only plans."""
    env = cli_env_for(seeded_catalog)
    with mock_aws():
        result = runner.invoke(
            app,
            ["sync", "--table", "main.pedidos", "--dry-run", "--json"],
            env=env,
        )
    payload = _json(result.stdout)
    steps = [s["step"] for s in payload["results"][0]["steps"]]
    assert "checkpoint" not in steps
    assert "register" not in steps


# --- log level from the environment; clean stdout ----------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_env_log_level_stdout_clean(flushed_catalog: CatalogHarness) -> None:
    env = {**cli_env_for(flushed_catalog), "DUCKLAKE_ICEBERG_LOG_LEVEL": "DEBUG"}
    with mock_aws():
        result = runner.invoke(
            app, ["sync", "--table", "main.pedidos", "--dry-run", "--json"], env=env
        )
    assert result.exit_code == 0
    payload = _json(result.stdout)
    assert payload["command"] == "sync"
    # stdout is exactly ONE line (the JSON envelope); DEBUG logs go to stderr
    assert len(result.stdout.strip().splitlines()) == 1
    assert "ducklake_iceberg:" not in result.stdout  # log format of the package logger
