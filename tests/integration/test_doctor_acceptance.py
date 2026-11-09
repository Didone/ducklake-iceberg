"""Acceptance: the doctor stage of `sync` classifies a table before publishing.

`sync` runs the compatibility preflight first. These tests exercise
`ducklake-iceberg sync` end to end (CliRunner + SQLite catalog + moto) and cover
the verdicts: clean (0), inlined data (not a blocker: the flush resolves it) and
type blocker (2, with a `blocked` problem listing the findings).
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


def _steps(payload: dict[str, Any]) -> list[dict[str, Any]]:
    results = payload["results"]
    assert len(results) == 1
    steps: list[dict[str, Any]] = results[0]["steps"]
    return steps


# --------------------------------------------------------------------------
# clean (flushed) table -> exit 0, doctor step passes
# --------------------------------------------------------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_clean_table_passes_doctor(flushed_catalog: CatalogHarness) -> None:
    env = cli_env_for(flushed_catalog)
    with mock_aws():
        result = runner.invoke(
            app, ["sync", "--table", "main.pedidos", "--dry-run", "--json"], env=env
        )
    assert result.exit_code == 0, result.stdout
    payload = _json(result.stdout)
    assert payload["problems"] == []
    assert _steps(payload)[0] == {"step": "doctor", "exit_code": 0}


# --------------------------------------------------------------------------
# inlined data is not a doctor blocker: only the dry run's export refuses it
# --------------------------------------------------------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_inlined_data_passes_doctor_but_dry_run_cannot_export(
    seeded_catalog: CatalogHarness,
) -> None:
    env = cli_env_for(seeded_catalog)
    with mock_aws():
        result = runner.invoke(
            app, ["sync", "--table", "main.pedidos", "--dry-run", "--json"], env=env
        )
    payload = _json(result.stdout)
    steps = _steps(payload)
    # the flush would resolve it, so the doctor stage does not block...
    assert steps[0] == {"step": "doctor", "exit_code": 0}
    # ...but a dry run does not flush, so the export refuses to lose the rows
    assert [s["step"] for s in steps] == ["doctor", "export"]
    assert result.exit_code == 2
    (problem,) = payload["problems"]
    assert problem["code"] == "blocked"
    assert "inlined data" in problem["message"]


# --------------------------------------------------------------------------
# table with a blocked type -> exit 2, `blocked` problem names the finding
# --------------------------------------------------------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_unsupported_type_blocks_sync(catalog: CatalogHarness) -> None:
    catalog.write("CREATE SCHEMA IF NOT EXISTS lake.main")
    catalog.write("CREATE TABLE lake.main.bad (id INTEGER, qty UINTEGER)")
    env = cli_env_for(catalog)
    with mock_aws():
        result = runner.invoke(app, ["sync", "--table", "main.bad", "--json"], env=env)
    assert result.exit_code == 2
    payload = _json(result.stdout)
    assert payload["status"] == "blocked"
    blocked = [p for p in payload["problems"] if p["code"] == "blocked"]
    assert len(blocked) == 1
    assert blocked[0]["table"] == "main.bad"
    assert "unsupported-type" in blocked[0]["message"]
    assert "uint32" in blocked[0]["message"]
    # the pipeline stops at the doctor stage: nothing was flushed or exported
    steps = _steps(payload)
    assert [s["step"] for s in steps] == ["doctor"]
    assert steps[0]["exit_code"] == 2


# --------------------------------------------------------------------------
# human-readable output also reports the blocker
# --------------------------------------------------------------------------
@pytest.mark.usefixtures("aws_credentials")
def test_blocked_human_output(catalog: CatalogHarness) -> None:
    catalog.write("CREATE SCHEMA IF NOT EXISTS lake.main")
    catalog.write("CREATE TABLE lake.main.bad (id INTEGER, qty UINTEGER)")
    env = cli_env_for(catalog)
    with mock_aws():
        result = runner.invoke(app, ["sync", "--table", "main.bad"], env=env)
    assert result.exit_code == 2
    assert "unsupported-type" in result.stdout
