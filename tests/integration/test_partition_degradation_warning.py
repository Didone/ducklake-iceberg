"""``sync`` surfaces the partition degradation.

A DuckLake spec with a calendar transform (``year``) has no Iceberg
equivalent: the table is published without the partition key (spec 0), exit 0,
and the DEGRADED warning with its remediation must show up in ``--json`` and
on stderr, not only in ``doctor``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from moto import mock_aws
from pyiceberg.table import StaticTable
from typer.testing import CliRunner

from ducklake_iceberg.cli import app

from .conftest import CatalogHarness

pytestmark = pytest.mark.integration

runner = CliRunner()
REGION = "us-east-1"


@pytest.fixture
def config(catalog: CatalogHarness, tmp_path: Path) -> Path:
    catalog.write("CALL lake.set_option('data_inlining_row_limit', 0)")
    catalog.write(
        "CREATE TABLE lake.main.t_part (id INTEGER, category VARCHAR, data DATE)"
    )
    path = tmp_path / "ducklake-iceberg.yaml"
    path.write_text(
        f"catalog_dsn: '{catalog.dsn}'\n"
        f"data_path: '{catalog.data_path}'\n"
        f"iceberg_metadata_prefix: '{tmp_path / 'iceberg'}'\n"
        f"glue_database: analytics\n"
        f"aws_region: {REGION}\n"
    )
    return path


def _insert(catalog: CatalogHarness) -> None:
    catalog.write(
        "INSERT INTO lake.main.t_part VALUES "
        "(1, 'a', DATE '2024-01-02'), (2, 'b', DATE '2025-03-04')"
    )


def _cli(config: Path, *args: str) -> tuple[int, dict[str, Any]]:
    with mock_aws(), mock.patch("ducklake_iceberg.glue._validate_metadata_location"):
        result = runner.invoke(app, [*args, "--config", str(config), "--json"])
    return result.exit_code, json.loads(result.stdout)


def _export_step(config: Path) -> dict[str, Any]:
    code, doc = _cli(config, "sync", "--table", "main.t_part")
    assert code == 0, doc["problems"]
    assert doc["problems"] == []
    steps = {s["step"]: s for s in doc["results"][0]["steps"]}
    return dict(steps["export"])


def _assert_year_warning(warnings: list[dict[str, Any]]) -> None:
    assert len(warnings) == 1
    w = warnings[0]
    assert w["severity"] == "degraded"
    assert "transform 'year' on column data has no Iceberg equivalent" in w["detail"]
    assert "epoch_year(data)" in w["remediation"]


def test_sync_json_reports_export_degraded_partition(
    catalog: CatalogHarness, config: Path
) -> None:
    catalog.write(
        "ALTER TABLE lake.main.t_part SET PARTITIONED BY (category, year(data))"
    )
    _insert(catalog)
    result = _export_step(config)
    _assert_year_warning(result["warnings"])
    # what gets published does not change: spec 0 (no partition key)
    location = result["metadata_location"]
    table = StaticTable.from_metadata(
        location if "://" in location else "file://" + location
    )
    assert table.spec().is_unpartitioned()
    assert table.scan().to_arrow().num_rows == 2


def test_sync_text_mode_warns_on_stderr(catalog: CatalogHarness, config: Path) -> None:
    catalog.write("ALTER TABLE lake.main.t_part SET PARTITIONED BY (year(data))")
    _insert(catalog)
    with mock_aws(), mock.patch("ducklake_iceberg.glue._validate_metadata_location"):
        result = runner.invoke(
            app, ["sync", "--table", "main.t_part", "--config", str(config)]
        )
    assert result.exit_code == 0
    assert "WARNING" in result.stderr
    assert "transform 'year' on column data" in result.stderr
    assert "epoch_year(data)" in result.stderr


def test_sync_json_reports_degraded_partition(
    catalog: CatalogHarness, config: Path
) -> None:
    catalog.write(
        "ALTER TABLE lake.main.t_part SET PARTITIONED BY (category, year(data))"
    )
    _insert(catalog)
    with mock_aws(), mock.patch("ducklake_iceberg.glue._validate_metadata_location"):
        code, doc = _cli(config, "sync", "--table", "main.t_part")
    assert code == 0
    assert doc["problems"] == []
    steps = {s["step"]: s for s in doc["results"][0]["steps"]}
    _assert_year_warning(steps["export"]["warnings"])
    assert steps["register"]["outcome"] == "registered"


def test_epoch_partition_has_no_warning(catalog: CatalogHarness, config: Path) -> None:
    catalog.write(
        "ALTER TABLE lake.main.t_part SET PARTITIONED BY (category, epoch_year(data))"
    )
    _insert(catalog)
    assert _export_step(config)["warnings"] == []


def test_historical_degraded_spec_without_files_has_no_warning(
    catalog: CatalogHarness, config: Path
) -> None:
    """Only a degraded spec with live files warns: a historical one does not."""
    catalog.write("ALTER TABLE lake.main.t_part SET PARTITIONED BY (year(data))")
    catalog.write("ALTER TABLE lake.main.t_part SET PARTITIONED BY (epoch_year(data))")
    _insert(catalog)
    assert _export_step(config)["warnings"] == []
