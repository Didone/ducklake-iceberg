"""``ADD COLUMN ... DEFAULT x`` and the data files written before it.

In DuckLake, rows of a file written before ``ADD COLUMN ... DEFAULT x`` read
``x`` (``initial_default``); Iceberg v2 has no ``initial-default``, so every
reader gets NULL. ``export`` blocks (exit 2) while a live data file lacks the
column, and publishes normally once the files have it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest import mock

import pytest
from pyiceberg.table import StaticTable

import iceduck.export
from iceduck.checkpoint.command import run_checkpoint
from iceduck.config import IceduckSettings
from iceduck.errors import CompatibilityError, ExitCode
from iceduck.export import run_export

from .conftest import CatalogHarness

pytestmark = pytest.mark.integration


@pytest.fixture
def harness(catalog: CatalogHarness) -> CatalogHarness:
    catalog.write("CALL lake.set_option('data_inlining_row_limit', 0)")
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1), (2)")  # file without c
    return catalog


def _settings(harness: CatalogHarness, tmp_path: Path) -> IceduckSettings:
    prefix = tmp_path / "iceberg"
    prefix.mkdir(exist_ok=True)
    return harness.settings(iceberg_metadata_prefix=str(prefix))


def _iceberg_rows(metadata_location: str) -> list[dict[str, Any]]:
    uri = (
        metadata_location
        if "://" in metadata_location
        else "file://" + metadata_location
    )
    rows = StaticTable.from_metadata(uri).scan().to_arrow().to_pylist()
    return sorted(rows, key=lambda r: r["id"])


def test_add_column_default_blocks_without_writing(
    harness: CatalogHarness, tmp_path: Path
) -> None:
    harness.write("ALTER TABLE lake.main.t ADD COLUMN c INTEGER DEFAULT 42")
    settings = _settings(harness, tmp_path)
    for dry_run in (True, False):
        with pytest.raises(CompatibilityError) as exc:
            run_export(settings, "main", "t", dry_run=dry_run)
        assert exc.value.exit_code is ExitCode.BLOCKED
        assert "'c' (DEFAULT 42, 1 file(s))" in exc.value.message
        assert exc.value.hint is not None
        assert 'SET "c" = "c"' in exc.value.hint
    assert not any((tmp_path / "iceberg").rglob("*.json"))


def test_add_column_default_blocks_already_published_files(
    harness: CatalogHarness, tmp_path: Path
) -> None:
    """Files published before the ADD would also read NULL in Iceberg."""
    settings = _settings(harness, tmp_path)
    assert run_export(settings, "main", "t").exit_code is ExitCode.SUCCESS
    harness.write("ALTER TABLE lake.main.t ADD COLUMN c INTEGER DEFAULT 42")
    harness.write("INSERT INTO lake.main.t VALUES (3, 3)")
    with pytest.raises(CompatibilityError):
        run_export(settings, "main", "t")


def test_drop_default_keeps_initial_default_blocking(
    harness: CatalogHarness, tmp_path: Path
) -> None:
    harness.write("ALTER TABLE lake.main.t ADD COLUMN c INTEGER DEFAULT 5")
    harness.write("ALTER TABLE lake.main.t ALTER COLUMN c DROP DEFAULT")
    with pytest.raises(CompatibilityError):
        run_export(_settings(harness, tmp_path), "main", "t")


def test_set_default_only_affects_new_rows(
    harness: CatalogHarness, tmp_path: Path
) -> None:
    """``SET DEFAULT`` only changes the write default: the value is in the file."""
    harness.write("ALTER TABLE lake.main.t ADD COLUMN c INTEGER")
    harness.write("ALTER TABLE lake.main.t ALTER COLUMN c SET DEFAULT 7")
    harness.write("INSERT INTO lake.main.t (id) VALUES (3)")
    result = run_export(_settings(harness, tmp_path), "main", "t")
    rows = _iceberg_rows(result.plan.metadata_location)
    assert [(r["id"], r["c"]) for r in rows] == [(1, None), (2, None), (3, 7)]


def test_rewritten_rows_publish_the_default(
    harness: CatalogHarness, tmp_path: Path
) -> None:
    """Hint remediation: ``UPDATE ... SET c = c`` writes 42 into the files."""
    harness.write("ALTER TABLE lake.main.t ADD COLUMN c INTEGER DEFAULT 42")
    harness.write("UPDATE lake.main.t SET c = c")
    result = run_export(_settings(harness, tmp_path), "main", "t")
    rows = _iceberg_rows(result.plan.metadata_location)
    assert [(r["id"], r["c"]) for r in rows] == [(1, 42), (2, 42)]


def test_compacted_files_with_default_are_published(
    harness: CatalogHarness, tmp_path: Path
) -> None:
    """The checkpoint merge materializes the default and keeps begin_snapshot:
    only the footer says the file already has the column."""
    harness.write("INSERT INTO lake.main.t VALUES (3)")
    harness.write("ALTER TABLE lake.main.t ADD COLUMN c INTEGER DEFAULT 42")
    harness.write("INSERT INTO lake.main.t (id) VALUES (4)")
    settings = _settings(harness, tmp_path)
    run_checkpoint(settings, table="main.t")
    result = run_export(settings, "main", "t")
    rows = _iceberg_rows(result.plan.metadata_location)
    assert [(r["id"], r["c"]) for r in rows] == [(1, 42), (2, 42), (3, 42), (4, 42)]


# -- no false positives (and no footer reads) --------------------------------


def _export_counting_footers(
    harness: CatalogHarness, tmp_path: Path
) -> tuple[list[dict[str, Any]], int]:
    from iceduck.writer.stats import file_field_ids as real

    with mock.patch.object(iceduck.export, "file_field_ids", wraps=real) as spy:
        result = run_export(_settings(harness, tmp_path), "main", "t")
    assert result.exit_code is ExitCode.SUCCESS
    return _iceberg_rows(result.plan.metadata_location), spy.call_count


def test_add_column_without_default_never_blocks(
    harness: CatalogHarness, tmp_path: Path
) -> None:
    """NULL ``initial_default``: both sides read NULL, nothing to check."""
    harness.write("ALTER TABLE lake.main.t ADD COLUMN c INTEGER")
    harness.write("INSERT INTO lake.main.t VALUES (3, 30)")
    rows, footers = _export_counting_footers(harness, tmp_path)
    assert [(r["id"], r["c"]) for r in rows] == [(1, None), (2, None), (3, 30)]
    assert footers == 0


def test_files_written_after_add_column_never_block(
    catalog: CatalogHarness, tmp_path: Path
) -> None:
    """A file written after the ADD has the column: it is not even a candidate."""
    catalog.write("CALL lake.set_option('data_inlining_row_limit', 0)")
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("ALTER TABLE lake.main.t ADD COLUMN c INTEGER DEFAULT 42")
    catalog.write("INSERT INTO lake.main.t (id) VALUES (1)")
    catalog.write("INSERT INTO lake.main.t VALUES (2, 7)")
    rows, footers = _export_counting_footers(catalog, tmp_path)
    assert [(r["id"], r["c"]) for r in rows] == [(1, 42), (2, 7)]
    assert footers == 0


def test_only_files_older_than_the_column_are_read(
    harness: CatalogHarness, tmp_path: Path
) -> None:
    """After the remediation, only a file older than the column would have its
    footer read; the UPDATE ended it, so the new files pass without a read."""
    harness.write("ALTER TABLE lake.main.t ADD COLUMN c INTEGER DEFAULT 42")
    harness.write("UPDATE lake.main.t SET c = c")
    harness.write("INSERT INTO lake.main.t VALUES (3, 3)")
    rows, footers = _export_counting_footers(harness, tmp_path)
    assert [(r["id"], r["c"]) for r in rows] == [(1, 42), (2, 42), (3, 3)]
    assert footers == 0
