"""Performance: worker limits and the metrics of the heavy phases.

Covers the CLI scenarios of ``sync``: ``workers_capped`` and how the effective
limits reach the pools, and the ``footers`` phase metrics.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from moto import mock_aws
from typer.testing import CliRunner

from ducklake_iceberg import concurrency
from ducklake_iceberg.cli import app
from ducklake_iceberg.errors import ExitCode
from ducklake_iceberg.output import Envelope

from .conftest import CatalogHarness

pytestmark = [pytest.mark.integration]

runner = CliRunner()
REGION = "us-east-1"


@pytest.fixture
def glue_env() -> Iterator[None]:
    with mock_aws(), mock.patch("ducklake_iceberg.glue._validate_metadata_location"):
        yield


def _config(catalog: CatalogHarness, tmp_path: Path, **extra: str) -> Path:
    catalog.write("CALL lake.set_option('data_inlining_row_limit', 0)")
    path = tmp_path / "ducklake-iceberg.yaml"
    lines = [
        f"catalog_dsn: '{catalog.dsn}'",
        f"data_path: '{catalog.data_path}'",
        f"iceberg_metadata_prefix: '{tmp_path / 'iceberg'}'",
        "glue_database: analytics",
        f"aws_region: {REGION}",
        *(f"{key}: {value}" for key, value in extra.items()),
    ]
    path.write_text("\n".join(lines) + "\n")
    return path


def _sync(config: Path, *args: str) -> tuple[int, dict[str, Any]]:
    result = runner.invoke(app, ["sync", "--config", str(config), "--json", *args])
    return result.exit_code, json.loads(result.stdout)


def _phase(doc: dict[str, Any], name: str) -> dict[str, Any]:
    return next(p for p in doc["results"][0]["metrics"]["phases"] if p["phase"] == name)


def _fake_sync_table(
    probe: Callable[[Any], None],
) -> Callable[..., ExitCode]:
    def fake(
        settings: Any, qualified: str, *, envelope: Envelope, **_: Any
    ) -> ExitCode:
        probe(settings)
        envelope.results.append(
            {"table": qualified, "steps": [], "exit_code": 0, "duration_ms": 1}
        )
        return ExitCode.SUCCESS

    return fake


def test_sync_workers_capped_is_warning_and_limits_reach_pools(
    catalog: CatalogHarness, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Env > YAML, global ceiling, PyIceberg."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    probes: list[tuple[int, int, str | None]] = []

    def probe(settings: Any) -> None:
        limits = concurrency.active_limits()
        probes.append(
            (
                settings.max_footer_workers,
                limits.max_io_workers,
                os.environ.get(concurrency.PYICEBERG_MAX_WORKERS_ENV),
            )
        )

    monkeypatch.setattr("ducklake_iceberg.cli.sync_table", _fake_sync_table(probe))
    monkeypatch.setenv("DUCKLAKE_ICEBERG_MAX_TOTAL_WORKERS", "4")
    monkeypatch.setenv("DUCKLAKE_ICEBERG_MAX_FOOTER_WORKERS", "8")
    config = _config(catalog, tmp_path, max_io_workers="3")

    code, doc = _sync(config, "--table", "main.t")

    assert code == 0, doc["problems"]
    (problem,) = doc["problems"]
    assert problem["code"] == "workers_capped"
    assert problem["level"] == "warning"
    assert "max_footer_workers reduced from 8 to 4" in problem["message"]
    assert "max_io_workers" not in problem["message"]  # 3 is below the cap
    assert doc["config"]["max_footer_workers"] == 4
    assert set(probes) == {(4, 3, "4")}


def test_sync_within_the_cap_emits_no_warning(
    catalog: CatalogHarness, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    monkeypatch.setattr(
        "ducklake_iceberg.cli.sync_table", _fake_sync_table(lambda _s: None)
    )

    code, doc = _sync(_config(catalog, tmp_path), "--table", "main.t")

    assert code == 0, doc["problems"]
    assert doc["problems"] == []


# -- footers phase metrics -------------------------------------------------


@pytest.mark.aws
def test_footers_phase_reports_items_bytes_and_calls(
    catalog: CatalogHarness, tmp_path: Path, glue_env: None
) -> None:
    config = _config(catalog, tmp_path, stats_source="footer")
    catalog.write(
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t VALUES (1), (2)",
        "INSERT INTO lake.main.t VALUES (3)",
    )

    code, doc = _sync(config, "--table", "main.t")

    assert code == 0, doc["problems"]
    phase = _phase(doc, "footers")
    assert phase["items"] == phase["calls"] == 2
    assert phase["bytes"] > 0
