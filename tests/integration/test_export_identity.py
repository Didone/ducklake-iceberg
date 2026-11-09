"""``export`` identity guard: name collision vs. table recreation.

The Glue name (and the Iceberg location) drops the DuckLake schema:
``main.dup_name`` and ``s2.dup_name`` compete for the same mirror. The guard
blocks (exit 3, nothing written) and the message tells a name collision apart
from a recreated table.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from iceduck.config import IceduckSettings
from iceduck.errors import DivergenceError, ExitCode
from iceduck.export import run_export

from .conftest import CatalogHarness

pytestmark = pytest.mark.integration


def _settings(harness: CatalogHarness, tmp_path: Path) -> IceduckSettings:
    prefix = tmp_path / "iceberg"
    prefix.mkdir(exist_ok=True)
    return harness.settings(iceberg_metadata_prefix=str(prefix))


def _metadata_files(tmp_path: Path) -> list[Path]:
    return sorted((tmp_path / "iceberg").rglob("*.metadata.json"))


@pytest.fixture
def harness(catalog: CatalogHarness) -> CatalogHarness:
    catalog.write("CALL lake.set_option('data_inlining_row_limit', 0)")
    catalog.write("CREATE TABLE lake.main.dup_name (id INTEGER)")
    catalog.write("INSERT INTO lake.main.dup_name VALUES (1)")
    return catalog


def test_same_name_in_other_schema_is_a_name_collision(
    harness: CatalogHarness, tmp_path: Path
) -> None:
    harness.write("CREATE SCHEMA lake.s2")
    harness.write("CREATE TABLE lake.s2.dup_name (id INTEGER)")
    harness.write("INSERT INTO lake.s2.dup_name VALUES (2)")
    settings = _settings(harness, tmp_path)
    assert run_export(settings, "main", "dup_name").exit_code is ExitCode.SUCCESS
    before = _metadata_files(tmp_path)

    for _ in range(
        2
    ):  # the first attempt and the re-sync: always 3, never "nothing to do"
        with pytest.raises(DivergenceError) as exc:
            run_export(settings, "s2", "dup_name")
        assert exc.value.exit_code is ExitCode.DIVERGED
        assert (
            "main.dup_name and s2.dup_name map to the same Glue table "
            "analytics.dup_name" in exc.value.message
        )
        assert "does not include the DuckLake schema" in exc.value.message
        assert "was recreated" not in exc.value.message
        assert exc.value.hint is not None
        assert "glue_database" in exc.value.hint
    assert _metadata_files(tmp_path) == before  # nothing written


def test_recreated_table_keeps_recreated_message(
    harness: CatalogHarness, tmp_path: Path
) -> None:
    settings = _settings(harness, tmp_path)
    assert run_export(settings, "main", "dup_name").exit_code is ExitCode.SUCCESS
    harness.write("DROP TABLE lake.main.dup_name")
    harness.write("CREATE TABLE lake.main.dup_name (id INTEGER)")
    harness.write("INSERT INTO lake.main.dup_name VALUES (3)")
    before = _metadata_files(tmp_path)

    with pytest.raises(DivergenceError) as exc:
        run_export(settings, "main", "dup_name")
    assert exc.value.exit_code is ExitCode.DIVERGED
    assert "table main.dup_name was recreated" in exc.value.message
    assert "map to the same Glue table" not in exc.value.message
    assert _metadata_files(tmp_path) == before
