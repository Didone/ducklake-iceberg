"""Export with inlined commits that arrive after the flush.

A concurrent writer (e.g. a consumer that commits every minute) leaves pending
inlined rows between the checkpoint flush and the export. The target becomes
the most recent snapshot without pending rows (``detect.clean_snapshot``):
nothing is lost, and the new rows go into the next export. The checkpoint
revalidation follows the same rule (test_checkpoint_revalidation.py); without
a flush, export still blocks (test_export.py).
"""

from __future__ import annotations

from pathlib import Path

from ducklake_iceberg.config import DuckLakeIcebergSettings
from ducklake_iceberg.errors import ExitCode
from ducklake_iceberg.export import run_export

from .conftest import CatalogHarness

LATE_ROWS = "INSERT INTO lake.main.pedidos VALUES (3, 'RJ'), (4, 'MG'), (5, 'BA')"


def _settings(harness: CatalogHarness, tmp_path: Path) -> DuckLakeIcebergSettings:
    prefix = tmp_path / "iceberg"
    prefix.mkdir(exist_ok=True)
    return harness.settings(iceberg_metadata_prefix=str(prefix))


def test_export_publishes_snapshot_before_late_inlined_rows(
    flushed_catalog: CatalogHarness, tmp_path: Path
) -> None:
    settings = _settings(flushed_catalog, tmp_path)
    flushed_catalog.write(LATE_ROWS)  # inlined, after the flush

    first = run_export(settings, "main", "pedidos")
    assert first.exit_code is ExitCode.SUCCESS
    assert first.plan.total_records == 2  # only what the flush wrote

    flushed_catalog.write("CALL ducklake_flush_inlined_data('lake')")
    second = run_export(settings, "main", "pedidos")
    assert second.exit_code is ExitCode.SUCCESS
    assert second.plan.total_records == 5
    assert second.plan.ducklake_snapshot_id > first.plan.ducklake_snapshot_id
