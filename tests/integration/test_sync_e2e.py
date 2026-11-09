"""``ducklake-iceberg sync`` end to end: real DuckLake -> Iceberg -> Glue (moto).

Exercises the command as the operator uses it (CLI, ``--json``, exit codes)
with the real PyIceberg ``register_table`` and boto3 ``UpdateTable`` against
the moto Glue. The metadata lives under a local prefix (PyArrow S3 is not
intercepted by moto), so the ``s3://`` check on registration is relaxed here.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import boto3
from moto import mock_aws
from pyiceberg.catalog.glue import GlueCatalog
from typer.testing import CliRunner

from ducklake_iceberg.cli import app

from .conftest import CatalogHarness

pytestmark = [pytest.mark.integration, pytest.mark.aws]

runner = CliRunner()
REGION = "us-east-1"


@pytest.fixture
def glue_env() -> Iterator[None]:
    with mock_aws(), mock.patch("ducklake_iceberg.glue._validate_metadata_location"):
        yield


@pytest.fixture
def config(catalog: CatalogHarness, tmp_path: Path) -> Path:
    catalog.write("CALL lake.set_option('data_inlining_row_limit', 0)")
    path = tmp_path / "ducklake-iceberg.yaml"
    path.write_text(
        f"catalog_dsn: '{catalog.dsn}'\n"
        f"data_path: '{catalog.data_path}'\n"
        f"iceberg_metadata_prefix: '{tmp_path / 'iceberg'}'\n"
        f"glue_database: analytics\n"
        f"aws_region: {REGION}\n"
    )
    return path


def _sync(config: Path, *extra: str) -> tuple[int, dict[str, Any]]:
    result = runner.invoke(
        app, ["sync", "--table", "main.t", "--config", str(config), "--json", *extra]
    )
    return result.exit_code, json.loads(result.stdout)


def _steps(doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {s["step"]: s for s in doc["results"][0]["steps"]}


def _glue_rows() -> int:
    table = GlueCatalog("t", **{"glue.region": REGION}).load_table(("analytics", "t"))
    return int(table.scan().to_arrow().num_rows)


def test_sync_registers_updates_and_is_idempotent(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v INTEGER)")
    catalog.write("INSERT INTO lake.main.t SELECT i, i FROM range(20) r(i)")

    code, doc = _sync(config)
    assert code == 0, doc["problems"]
    assert _steps(doc)["register"]["outcome"] == "registered"
    assert _glue_rows() == 20

    catalog.write("DELETE FROM lake.main.t WHERE id < 5")
    catalog.write("INSERT INTO lake.main.t VALUES (100, 100)")
    code, doc = _sync(config)
    assert code == 0, doc["problems"]
    steps = _steps(doc)
    assert steps["export"]["operation"] == "overwrite"
    assert steps["register"]["outcome"] == "updated"
    assert _glue_rows() == 16

    params = boto3.client("glue", region_name=REGION).get_table(
        DatabaseName="analytics", Name="t"
    )["Table"]["Parameters"]
    assert params["metadata_location"].endswith("v2.metadata.json")
    assert params["previous_metadata_location"].endswith("v1.metadata.json")

    code, doc = _sync(config)
    assert code == 4
    assert _steps(doc)["export"]["exit_code"] == 4


def test_sync_emits_versioned_metrics_and_json_logs(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1), (2)")
    result = runner.invoke(
        app,
        [
            "sync",
            "--table",
            "main.t",
            "--config",
            str(config),
            "--json",
            "--log-format",
            "json",
        ],
    )

    assert result.exit_code == 0
    envelope = json.loads(result.stdout)
    assert envelope["version"] == 1
    metrics = envelope["results"][0]["metrics"]
    assert metrics["run_id"]
    assert metrics["duration_ms"] >= sum(
        phase["duration_ms"] for phase in metrics["phases"]
    )
    assert {phase["phase"] for phase in metrics["phases"]} >= {
        "doctor",
        "delta",
        "glue",
    }
    events = [json.loads(line) for line in result.stderr.splitlines()]
    assert events[-1]["event"] == "command.end"
    sync_event = next(event for event in events if event["event"] == "sync.end")
    assert sync_event["table"] == "main.t"
    assert isinstance(sync_event["snapshot_id"], int)
    assert isinstance(sync_event["files_added"], int)
    assert isinstance(sync_event["bytes_referenced"], int)
    assert envelope["results"][0]["ducklake_snapshot_id"] == sync_event["snapshot_id"]

    no_metrics = runner.invoke(
        app,
        [
            "sync",
            "--table",
            "main.t",
            "--config",
            str(config),
            "--json",
            "--no-metrics",
        ],
    )
    assert no_metrics.exit_code == 4
    assert "metrics" not in json.loads(no_metrics.stdout)["results"][0]


def test_sync_materialize_deletes_removes_delete_files(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t SELECT * FROM range(50)")
    catalog.write("DELETE FROM lake.main.t WHERE id < 3")
    code, doc = _sync(config, "--materialize-deletes")
    assert code == 0, doc["problems"]
    assert _steps(doc)["materialize"]["result"]["files_processed"] >= 1
    table = GlueCatalog("t", **{"glue.region": REGION}).load_table(("analytics", "t"))
    snapshot = table.current_snapshot()
    assert snapshot is not None
    assert snapshot.summary is not None
    assert snapshot.summary.additional_properties["total-delete-files"] == "0"
    assert table.scan().to_arrow().num_rows == 47


def test_sync_retries_pending_register(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Export written and register failed: the next sync only publishes."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1)")
    export = runner.invoke(
        app, ["export", "--table", "main.t", "--config", str(config), "--json"]
    )
    assert export.exit_code == 0, export.stdout
    code, doc = _sync(config)
    assert code == 0, doc["problems"]
    steps = _steps(doc)
    assert steps["export"]["exit_code"] == 4
    assert steps["register"]["outcome"] == "registered"


def test_external_write_is_divergence(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Glue pointing to a metadata.json outside the ducklake-iceberg chain."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1)")
    assert _sync(config)[0] == 0
    glue = boto3.client("glue", region_name=REGION)
    table = glue.get_table(DatabaseName="analytics", Name="t")["Table"]
    params = dict(table["Parameters"])
    params["metadata_location"] = params["metadata_location"].replace(
        "v1.metadata.json", "v1-athena.metadata.json"
    )
    glue.update_table(
        DatabaseName="analytics",
        TableInput={
            "Name": "t",
            "TableType": table["TableType"],
            "Parameters": params,
            "StorageDescriptor": table["StorageDescriptor"],
        },
    )
    catalog.write("INSERT INTO lake.main.t VALUES (2)")
    code, doc = _sync(config)
    assert code == 3
    assert doc["problems"][0]["code"] == "catalog_diverged"


def test_sync_blocked_table_reports_exit_2(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id UBIGINT)")
    catalog.write("INSERT INTO lake.main.t VALUES (1)")
    code, doc = _sync(config)
    assert code == 2
    assert _steps(doc)["doctor"]["exit_code"] == 2
