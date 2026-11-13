"""Recovery, identity and schema regressions from the repository review."""

from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest
from moto import mock_aws
from pyiceberg.table import StaticTable
from typer.testing import CliRunner

from ducklake_iceberg.cli import app
from ducklake_iceberg.config import DuckLakeIcebergSettings
from ducklake_iceberg.errors import ConfigError, DivergenceError, ExitCode
from ducklake_iceberg.export import run_export
from ducklake_iceberg.register import run_register
from ducklake_iceberg.writer.metadata import MetadataError

from .conftest import CatalogHarness

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("aws_credentials")]


@pytest.fixture
def settings(catalog: CatalogHarness, tmp_path: Path) -> DuckLakeIcebergSettings:
    catalog.write(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.t (id INTEGER, p INTEGER)",
        "INSERT INTO lake.main.t VALUES (1, 7), (2, 8)",
    )
    return catalog.settings(iceberg_metadata_prefix=str(tmp_path / "iceberg"))


def test_unchanged_export_rejects_another_source_uuid(
    catalog: CatalogHarness,
    settings: DuckLakeIcebergSettings,
) -> None:
    first = run_export(settings, "main", "t")
    before = Path(first.metadata_location).read_bytes()
    catalog.meta_exec(
        "UPDATE ducklake_table SET table_uuid = "
        "'12345678-1234-1234-1234-123456789abc' WHERE table_name = 't'"
    )
    with pytest.raises(DivergenceError):
        run_export(settings, "main", "t")
    assert Path(first.metadata_location).read_bytes() == before


def test_legacy_pointer_checks_source_before_noop(
    catalog: CatalogHarness,
    settings: DuckLakeIcebergSettings,
) -> None:
    first = run_export(settings, "main", "t")
    assert first.pointer_path is not None
    path = Path(first.pointer_path)
    pointer = json.loads(path.read_text())
    pointer.pop("table_uuid")
    pointer.pop("data_path")
    path.write_text(json.dumps(pointer))
    assert run_export(settings, "main", "t").exit_code is ExitCode.NOTHING_TO_DO
    catalog.meta_exec(
        "UPDATE ducklake_table SET table_uuid = "
        "'12345678-1234-1234-1234-123456789abc' WHERE table_name = 't'"
    )
    with pytest.raises(DivergenceError):
        run_export(settings, "main", "t")


@pytest.mark.parametrize("dry_run", [False, True])
def test_first_export_recovers_after_pointer_failure(
    settings: DuckLakeIcebergSettings,
    dry_run: bool,
) -> None:
    with (
        mock.patch(
            "ducklake_iceberg.export.write_pointer", side_effect=OSError("lost PUT")
        ),
        pytest.raises(OSError, match="lost PUT"),
    ):
        run_export(settings, "main", "t")
    prefix = Path(settings.iceberg_metadata_prefix)
    metadata_path = next(prefix.rglob("v1.metadata.json"))
    before = {p: p.read_bytes() for p in prefix.rglob("*") if p.is_file()}
    result = run_export(settings, "main", "t", dry_run=dry_run)
    if dry_run:
        assert {p: p.read_bytes() for p in prefix.rglob("*") if p.is_file()} == before
        return
    assert result.exit_code is ExitCode.SUCCESS
    assert result.metadata_location == str(metadata_path)
    assert result.to_json()["metadata_version"] == 1
    assert result.pointer_path is not None
    assert Path(result.pointer_path).is_file()
    assert all(p.read_bytes() == payload for p, payload in before.items())
    assert not list(prefix.rglob("v2.metadata.json"))
    assert (
        StaticTable.from_metadata(result.metadata_location).scan().to_arrow().num_rows
        == 2
    )
    assert run_export(settings, "main", "t").exit_code is ExitCode.NOTHING_TO_DO


@pytest.mark.parametrize("problem", ["uuid", "location", "provenance"])
def test_first_export_never_adopts_unrelated_metadata(
    settings: DuckLakeIcebergSettings,
    problem: str,
) -> None:
    first = run_export(settings, "main", "t")
    assert first.pointer_path is not None
    Path(first.pointer_path).unlink()
    path = Path(first.metadata_location)
    metadata = json.loads(path.read_text())
    if problem == "uuid":
        metadata["properties"]["ducklake.table-uuid"] = "another-source"
    elif problem == "location":
        metadata["location"] = "/another-export"
    else:
        metadata["properties"].pop("ducklake.table-uuid")
    path.write_text(json.dumps(metadata))
    before = path.read_bytes()
    with pytest.raises((DivergenceError, MetadataError)):
        run_export(settings, "main", "t")
    assert not Path(first.pointer_path).exists()
    assert path.read_bytes() == before


def test_recovered_successor_repairs_pointer_without_new_data(
    catalog: CatalogHarness,
    settings: DuckLakeIcebergSettings,
) -> None:
    first = run_export(settings, "main", "t")
    assert first.pointer_path is not None
    path = Path(first.pointer_path)
    old_pointer = path.read_bytes()
    catalog.write("INSERT INTO lake.main.t VALUES (3, 9)")
    second = run_export(settings, "main", "t")
    path.write_bytes(old_pointer)
    recovered = run_export(settings, "main", "t")
    assert recovered.exit_code is ExitCode.SUCCESS
    assert recovered.metadata_location == second.metadata_location
    assert json.loads(path.read_text())["metadata_version"] == 2
    assert run_export(settings, "main", "t").exit_code is ExitCode.NOTHING_TO_DO


@pytest.mark.parametrize("initial_export", [False, True])
def test_drop_former_partition_column_keeps_rows_readable(
    catalog: CatalogHarness,
    settings: DuckLakeIcebergSettings,
    initial_export: bool,
) -> None:
    catalog.write(
        "ALTER TABLE lake.main.t SET PARTITIONED BY (p)",
        "INSERT INTO lake.main.t VALUES (3, 7), (4, 8)",
    )
    if initial_export:
        run_export(settings, "main", "t")
    catalog.write(
        "ALTER TABLE lake.main.t RESET PARTITIONED BY",
        "ALTER TABLE lake.main.t DROP COLUMN p",
    )
    result = run_export(settings, "main", "t")
    table = StaticTable.from_metadata(result.metadata_location)
    assert table.schema().column_names == ["id"]
    assert sorted(r["id"] for r in table.scan().to_arrow().to_pylist()) == [1, 2, 3, 4]
    catalog.write("DELETE FROM lake.main.t WHERE id = 3")
    result = run_export(settings, "main", "t")
    table = StaticTable.from_metadata(result.metadata_location)
    assert sorted(r["id"] for r in table.scan().to_arrow().to_pylist()) == [1, 2, 4]


def test_metadata_prefix_checked_against_actual_source(
    settings: DuckLakeIcebergSettings,
    tmp_path: Path,
) -> None:
    prefix = Path(settings.data_path) / "iceberg"
    settings = DuckLakeIcebergSettings(
        **(
            settings.model_dump()
            | {
                "data_path": str(tmp_path / "unrelated"),
                "iceberg_metadata_prefix": str(prefix),
            }
        )
    )
    with pytest.raises(ConfigError, match="actual DuckLake source"):
        run_export(settings, "main", "t")
    assert not prefix.exists()


def test_retention_allows_retry_of_unpublished_exports(
    catalog: CatalogHarness,
    settings: DuckLakeIcebergSettings,
) -> None:
    import time

    settings = settings.model_copy(update={"snapshot_retention_hours": 1})
    start = time.time()
    with mock_aws(), mock.patch("ducklake_iceberg.glue._validate_metadata_location"):
        first = run_export(settings, "main", "t")
        run_register(
            settings, "main", "t", handoff=first.handoff, metadata=first.metadata
        )
        catalog.write("INSERT INTO lake.main.t VALUES (3, 9)")
        with mock.patch(
            "ducklake_iceberg.writer.metadata.time.time", return_value=start + 7200
        ):
            run_export(settings, "main", "t")  # publication fails before Glue
        catalog.write("INSERT INTO lake.main.t VALUES (4, 9)")
        with mock.patch(
            "ducklake_iceberg.writer.metadata.time.time", return_value=start + 14400
        ):
            third = run_export(settings, "main", "t")
        assert third.metadata is not None
        assert len(third.metadata.snapshots) == 1
        run_register(
            settings, "main", "t", handoff=third.handoff, metadata=third.metadata
        )
        from pyiceberg.catalog.glue import GlueCatalog

        table = GlueCatalog("test", **{"glue.region": settings.aws_region}).load_table(
            ("analytics", "t")
        )
        assert table.scan().to_arrow().num_rows == 4


def test_cli_preserves_quoted_identifiers_through_flush_and_export(
    catalog: CatalogHarness,
    settings: DuckLakeIcebergSettings,
    tmp_path: Path,
) -> None:
    import yaml

    catalog.write(
        "CALL lake.set_option('data_inlining_row_limit', 100)",
        'CREATE SCHEMA lake."s.dot"',
        'CREATE TABLE lake."s.dot"."a.b" (id INTEGER)',
        'INSERT INTO lake."s.dot"."a.b" VALUES (99)',
    )
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump(settings.model_dump()))
    runner = CliRunner()
    with mock_aws(), mock.patch("ducklake_iceberg.glue._validate_metadata_location"):
        for expected in [0, 4]:
            result = runner.invoke(
                app,
                ["sync", "--table", '"s.dot"."a.b"', "--config", str(config), "--json"],
            )
            assert result.exit_code == expected, result.stdout
            payload = json.loads(result.stdout)
            assert payload["problems"] == []
            metadata = payload["results"][0]["metadata_location"]
            assert StaticTable.from_metadata(
                metadata
            ).scan().to_arrow().to_pylist() == [{"id": 99}]


def test_first_export_recovery_requires_readable_manifests(
    settings: DuckLakeIcebergSettings,
) -> None:
    first = run_export(settings, "main", "t")
    assert first.pointer_path is not None
    Path(first.pointer_path).unlink()
    Path(first.manifest_paths[0]).unlink()
    with pytest.raises(FileNotFoundError):
        run_export(settings, "main", "t")
    assert not Path(first.pointer_path).exists()
