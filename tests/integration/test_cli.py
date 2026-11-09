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

from iceduck.cli import app
from tests.integration.conftest import CatalogHarness, cli_env_for

pytestmark = [pytest.mark.integration, pytest.mark.aws]

runner = CliRunner()


def _json(output: str) -> dict[str, Any]:
    payload: dict[str, Any] = json.loads(output)
    return payload


# --- help lists the 7 subcommands (backend independent) ----------------------
def test_help_lists_commands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for cmd in (
        "doctor",
        "checkpoint",
        "export",
        "register",
        "sync",
        "status",
        "validate",
    ):
        assert cmd in result.stdout


def test_doctor_help_lists_flags() -> None:
    result = runner.invoke(app, ["doctor", "--help"])
    assert result.exit_code == 0
    for flag in ("--table", "--all-tables", "--schema", "--dry-run", "--json"):
        assert flag in result.stdout


# --- --version ---------------------------------------------------------------
def test_version() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert result.stdout.strip().startswith("iceduck ")
    assert "(duckdb " in result.stdout
    assert ", pyiceberg " in result.stdout


# --- human-readable doctor ---------------------------------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_doctor_human(flushed_catalog: CatalogHarness) -> None:
    env = cli_env_for(flushed_catalog)
    with mock_aws():
        result = runner.invoke(app, ["doctor", "--table", "main.pedidos"], env=env)
    assert result.exit_code == 0
    assert "table_id" in result.stdout
    assert "snapshot_id" in result.stdout
    assert "schema_version" in result.stdout


# --- doctor --json (includes the config field = redacted()) ------------------
@pytest.mark.usefixtures("aws_credentials")
def test_doctor_json(flushed_catalog: CatalogHarness) -> None:
    env = cli_env_for(flushed_catalog)
    with mock_aws():
        result = runner.invoke(
            app, ["doctor", "--table", "main.pedidos", "--json"], env=env
        )
    assert result.exit_code == 0
    payload = _json(result.stdout)
    assert payload["version"] == 1
    assert payload["status"] == "ok"
    assert len(payload["results"]) == 1
    assert payload["results"][0]["aws"]["glue_reachable"] is True
    assert payload["results"][0]["aws"]["glue_database_exists"] is False
    # The preflight report includes the compatibility rules and the advisory.
    rules_report = payload["results"][0]["rules"]
    assert len(rules_report["rules"]) == 9
    assert rules_report["exit_code"] == 0
    # redacted() is the `config` payload in the envelope, without the host
    assert payload["config"] is not None
    assert payload["config"]["glue_database"] == "analytics"
    assert "host=***" in payload["config"]["catalog_dsn"] or (
        "host=" not in payload["config"]["catalog_dsn"]
    )


# --- global flag after the subcommand ----------------------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_flag_after_subcommand(flushed_catalog: CatalogHarness) -> None:
    env = cli_env_for(flushed_catalog)
    with mock_aws():
        result = runner.invoke(
            app,
            ["doctor", "--table", "main.pedidos", "--log-level", "DEBUG"],
            env=env,
        )
    assert result.exit_code == 0


# --- neither --table nor --all-tables ----------------------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_missing_target_flags(seeded_catalog: CatalogHarness) -> None:
    env = cli_env_for(seeded_catalog)
    with mock_aws():
        result = runner.invoke(app, ["doctor", "--json"], env=env)
    assert result.exit_code == 1
    payload = _json(result.stdout)
    assert "provide --table or --all-tables" in payload["problems"][0]["message"]


# --- --table and --all-tables together ---------------------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_mutually_exclusive(seeded_catalog: CatalogHarness) -> None:
    env = cli_env_for(seeded_catalog)
    with mock_aws():
        result = runner.invoke(
            app, ["doctor", "--table", "t", "--all-tables", "--json"], env=env
        )
    assert result.exit_code == 1
    assert "not both" in _json(result.stdout)["problems"][0]["message"]


# --- --schema without --all-tables -------------------------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_schema_without_all_tables(seeded_catalog: CatalogHarness) -> None:
    env = cli_env_for(seeded_catalog)
    with mock_aws():
        result = runner.invoke(
            app, ["doctor", "--schema", "vendas", "--table", "t", "--json"], env=env
        )
    assert result.exit_code == 1


# --- --all-tables with a table that fails to resolve -------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_all_tables_partial_failure(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.ok (id INTEGER)")
    catalog.write("CREATE TABLE lake.main.dup (id INTEGER)")
    # inject ambiguity only for 'dup': its resolve_table fails, 'ok' resolves
    catalog.meta_exec(
        "INSERT INTO ducklake_table "
        "(table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, "
        " table_name, path, path_is_relative) "
        "SELECT table_id + 1000, table_uuid, begin_snapshot, end_snapshot, "
        " schema_id, table_name, path, path_is_relative "
        "FROM ducklake_table WHERE table_name = 'dup'",
    )
    env = cli_env_for(catalog)
    with mock_aws():
        result = runner.invoke(app, ["doctor", "--all-tables", "--json"], env=env)
    assert result.exit_code == 1  # error dominates
    payload = _json(result.stdout)
    # the ambiguous resolution of 'dup' is recorded as a problem with the table
    assert payload["problems"]
    assert any(p.get("table") == "main.dup" for p in payload["problems"])
    # the ambiguity (forged duplicate PK) aborts the single read transaction;
    # doctor exits 1 and the problem names the offending table.


# --- --all-tables on a catalog without live tables ---------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_all_tables_empty(catalog: CatalogHarness) -> None:
    env = cli_env_for(catalog)  # only the empty schema
    with mock_aws():
        result = runner.invoke(app, ["doctor", "--all-tables", "--json"], env=env)
    assert result.exit_code == 4
    assert _json(result.stdout)["results"] == []


# --- validate exposes the reconciliation controls ----------------------------
def test_validate_help_lists_options() -> None:
    result = runner.invoke(app, ["validate", "--help"])
    assert result.exit_code == 0
    for flag in (
        "--deep",
        "--sample-rows",
        "--strict",
        "--timestamp-precision",
        "--only",
        "--check-s3",
        "--s3-sample",
        "--log-format",
    ):
        assert flag in result.stdout


def test_sync_help_lists_metrics_toggle() -> None:
    result = runner.invoke(app, ["sync", "--help"])
    assert result.exit_code == 0
    assert "--metrics" in result.stdout
    assert "--no-metrics" in result.stdout


# --- invalid catalog_dsn with --json -----------------------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_invalid_dsn_emits_error_envelope(seeded_catalog: CatalogHarness) -> None:
    env = {
        **cli_env_for(seeded_catalog),
        "ICEDUCK_CATALOG_DSN": "ducklake:sqlite:/does/not/exist.sqlite",
    }
    with mock_aws():
        result = runner.invoke(
            app, ["doctor", "--table", "main.pedidos", "--json"], env=env
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
        "ICEDUCK_DATA_PATH": "s3://bucket/ducklake/data/",
        "ICEDUCK_ICEBERG_METADATA_PREFIX": "s3://bucket/iceberg/",
        "ICEDUCK_AWS_PROFILE": "nonexistent_profile_iceduck_xyz",
    }
    with mock_aws():
        result = runner.invoke(
            app, ["doctor", "--table", "main.pedidos", "--json"], env=env
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
            ["doctor", "--table", "main.pedidos", "--dry-run", "--json"],
            env=env,
        )
    assert result.exit_code == 0
    assert _json(result.stdout)["dry_run"] is True


# --- data_path reconciliation mismatch -> exit 1 -----------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_data_path_mismatch_fails(seeded_catalog: CatalogHarness) -> None:
    env = {
        **cli_env_for(seeded_catalog),
        "ICEDUCK_DATA_PATH": "/path/that/does/not/match/data",
    }
    with mock_aws():
        result = runner.invoke(
            app, ["doctor", "--table", "main.pedidos", "--json"], env=env
        )
    assert result.exit_code == 1
    codes = [p["code"] for p in _json(result.stdout)["problems"]]
    assert "data_path_mismatch" in codes


# --- log level from the environment; clean stdout ----------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_env_log_level_stdout_clean(flushed_catalog: CatalogHarness) -> None:
    env = {**cli_env_for(flushed_catalog), "ICEDUCK_LOG_LEVEL": "DEBUG"}
    with mock_aws():
        result = runner.invoke(
            app, ["doctor", "--table", "main.pedidos", "--json"], env=env
        )
    assert result.exit_code == 0
    payload = _json(result.stdout)
    assert payload["command"] == "doctor"
    # stdout is exactly ONE line (the JSON envelope); DEBUG logs go to stderr
    assert len(result.stdout.strip().splitlines()) == 1
    assert "iceduck:" not in result.stdout  # log format of the 'iceduck' logger
