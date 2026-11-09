"""Integration tests for `export` against a real DuckLake catalog (SQLite).

Uses `flushed_catalog` (table `main.pedidos` already with a data file) and a
local metadata prefix, exercising the whole export pipeline with local FileIO
(no S3/moto). Final gate: `StaticTable` reads the result.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ducklake_iceberg.config import DuckLakeIcebergSettings
from ducklake_iceberg.errors import ExitCode
from ducklake_iceberg.export import plan_export, run_export

from .conftest import CatalogHarness


def _settings(harness: CatalogHarness, tmp_path: Path) -> DuckLakeIcebergSettings:
    prefix = tmp_path / "iceberg"
    prefix.mkdir(exist_ok=True)
    return harness.settings(iceberg_metadata_prefix=str(prefix))


def test_export_end_to_end(flushed_catalog: CatalogHarness, tmp_path: Path) -> None:
    settings = _settings(flushed_catalog, tmp_path)
    result = run_export(settings, "main", "pedidos")
    assert result.exit_code is ExitCode.SUCCESS
    assert result.pointer_path is not None
    assert result.manifest_list_path
    assert result.plan.total_records == 2

    # the metadata.json and the pointer exist
    meta = Path(result.plan.metadata_location.removeprefix("file://"))
    assert meta.exists()
    pointer = Path(result.pointer_path.removeprefix("file://"))
    assert pointer.name == "_ducklake_iceberg_last_export.json"
    assert pointer.exists()


def test_export_static_table_roundtrip(
    flushed_catalog: CatalogHarness, tmp_path: Path
) -> None:
    from pyiceberg.table import StaticTable

    settings = _settings(flushed_catalog, tmp_path)
    result = run_export(settings, "main", "pedidos")
    location = result.plan.metadata_location
    uri = location if location.startswith("file://") else "file://" + location
    table = StaticTable.from_metadata(uri)
    assert [f.name for f in table.schema().fields] == ["id", "uf"]
    arrow = table.scan().to_arrow()
    assert arrow.num_rows == 2
    assert sorted(arrow.column("id").to_pylist()) == [1, 2]


def test_export_dry_run_writes_nothing(
    flushed_catalog: CatalogHarness, tmp_path: Path
) -> None:
    settings = _settings(flushed_catalog, tmp_path)
    result = run_export(settings, "main", "pedidos", dry_run=True)
    assert result.exit_code is ExitCode.SUCCESS
    assert result.dry_run is True
    assert result.pointer_path is None
    # no object written under the metadata prefix
    prefix = Path(settings.iceberg_metadata_prefix.rstrip("/"))
    written = [p for p in prefix.rglob("*") if p.is_file()]
    assert written == [], f"dry-run wrote: {written}"


def test_plan_export_is_read_only(
    flushed_catalog: CatalogHarness, tmp_path: Path
) -> None:
    settings = _settings(flushed_catalog, tmp_path)
    plan = plan_export(settings, "main", "pedidos")
    assert plan.total_records == 2
    assert plan.metadata_version == 1
    prefix = Path(settings.iceberg_metadata_prefix.rstrip("/"))
    assert [p for p in prefix.rglob("*") if p.is_file()] == []


def test_export_inlined_pending_blocks(
    seeded_catalog: CatalogHarness, tmp_path: Path
) -> None:
    # seeded_catalog has inlined rows -> exit 2 before any enumeration
    from ducklake_iceberg.errors import CompatibilityError

    settings = _settings(seeded_catalog, tmp_path)
    with pytest.raises(CompatibilityError, match="inlined") as exc:
        run_export(settings, "main", "pedidos")
    assert exc.value.exit_code == 2


# -- CLI (CliRunner): command envelope and exit codes -----------------------


def test_cli_export_json(flushed_catalog: CatalogHarness, tmp_path: Path) -> None:
    import json

    from typer.testing import CliRunner

    from ducklake_iceberg.cli import app
    from tests.integration.conftest import cli_env_for

    prefix = tmp_path / "iceberg"
    prefix.mkdir(exist_ok=True)
    env = {
        **cli_env_for(flushed_catalog),
        "DUCKLAKE_ICEBERG_ICEBERG_METADATA_PREFIX": str(prefix),
    }
    runner = CliRunner()
    result = runner.invoke(
        app, ["export", "--table", "main.pedidos", "--json"], env=env
    )
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    assert payload["command"] == "export"
    assert payload["results"][0]["data_files"] == 1
    assert payload["results"][0]["total_records"] == 2


def test_cli_export_dry_run_writes_nothing(
    flushed_catalog: CatalogHarness, tmp_path: Path
) -> None:
    from typer.testing import CliRunner

    from ducklake_iceberg.cli import app
    from tests.integration.conftest import cli_env_for

    prefix = tmp_path / "iceberg"
    prefix.mkdir(exist_ok=True)
    env = {
        **cli_env_for(flushed_catalog),
        "DUCKLAKE_ICEBERG_ICEBERG_METADATA_PREFIX": str(prefix),
    }
    runner = CliRunner()
    result = runner.invoke(
        app, ["export", "--table", "main.pedidos", "--dry-run", "--json"], env=env
    )
    assert result.exit_code == 0, result.stdout
    assert [p for p in prefix.rglob("*") if p.is_file()] == []


def test_cli_export_requires_qualified_table(
    flushed_catalog: CatalogHarness, tmp_path: Path
) -> None:
    from typer.testing import CliRunner

    from ducklake_iceberg.cli import app
    from tests.integration.conftest import cli_env_for

    env = {
        **cli_env_for(flushed_catalog),
        "DUCKLAKE_ICEBERG_ICEBERG_METADATA_PREFIX": str(tmp_path / "iceberg"),
    }
    runner = CliRunner()
    result = runner.invoke(app, ["export", "--table", "pedidos", "--json"], env=env)
    assert result.exit_code == 1
