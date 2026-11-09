from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from iceduck.catalog.connection import open_catalog
from iceduck.maintenance.ducklake_cleanup import (
    apply_cleanup_plan,
    plan_scheduled_files,
)
from iceduck.s3 import LocalStore

from .conftest import CatalogHarness

pytestmark = pytest.mark.integration


def _schedule_files(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t SELECT * FROM range(1000)")
    catalog.write("CALL ducklake_flush_inlined_data('lake')")
    catalog.write("INSERT INTO lake.main.t SELECT * FROM range(1000, 2000)")
    catalog.write("CALL ducklake_flush_inlined_data('lake')")
    catalog.write("CALL ducklake_merge_adjacent_files('lake', 't', schema => 'main')")
    catalog.write(
        "UPDATE lake_meta.ducklake_files_scheduled_for_deletion "
        "SET schedule_start = now() - INTERVAL 2 DAY"
    )


def test_dry_run_keeps_old_files_and_catalog_rows(catalog: CatalogHarness) -> None:
    _schedule_files(catalog)
    settings = catalog.settings()

    with open_catalog(settings, read_only=True) as con:
        plan = plan_scheduled_files(
            con,
            older_than=datetime.now(UTC) - timedelta(days=1),
            max_deletes=1,
            data_path=catalog.data_path,
        )
        report = apply_cleanup_plan(
            con,
            LocalStore(),
            plan,
            apply=False,
            referenced_paths=set(),
        )
        row = con.raw.execute(
            "SELECT count(*) FROM lake_meta.ducklake_files_scheduled_for_deletion"
        ).fetchone()
        assert row is not None
        scheduled_count = row[0]

    assert len(plan.paths) == 1
    assert plan.truncated
    assert report.deleted == ()
    assert report.exit_code == 0
    assert scheduled_count == 2
    assert all(Path(path).exists() for path in plan.paths)


def test_apply_deletes_s3_equivalent_and_marks_catalog_rows(
    catalog: CatalogHarness,
) -> None:
    _schedule_files(catalog)
    settings = catalog.settings()

    with open_catalog(settings, read_only=False) as con:
        plan = plan_scheduled_files(
            con,
            older_than=datetime.now(UTC) - timedelta(days=1),
            max_deletes=10,
            data_path=catalog.data_path,
        )
        row = con.raw.execute(
            "SELECT count(*) FROM lake_meta.ducklake_snapshot"
        ).fetchone()
        assert row is not None
        snapshots_before = row[0]
        report = apply_cleanup_plan(
            con,
            LocalStore(),
            plan,
            apply=True,
            referenced_paths=set(),
        )
        row = con.raw.execute(
            "SELECT count(*) FROM lake_meta.ducklake_files_scheduled_for_deletion"
        ).fetchone()
        assert row is not None
        scheduled_count = row[0]
        row = con.raw.execute(
            "SELECT count(*) FROM lake_meta.ducklake_snapshot"
        ).fetchone()
        assert row is not None
        snapshots_after = row[0]

    assert len(report.deleted) == 2
    assert report.exit_code == 0
    assert scheduled_count == 0
    assert snapshots_after == snapshots_before
    assert not any(Path(path).exists() for path in report.deleted)


def test_apply_refuses_files_referenced_by_iceberg(catalog: CatalogHarness) -> None:
    _schedule_files(catalog)
    settings = catalog.settings()

    with open_catalog(settings, read_only=False) as con:
        plan = plan_scheduled_files(
            con,
            older_than=datetime.now(UTC) - timedelta(days=1),
            max_deletes=10,
            data_path=catalog.data_path,
        )
        report = apply_cleanup_plan(
            con,
            LocalStore(),
            plan,
            apply=True,
            referenced_paths=set(plan.paths),
        )
        row = con.raw.execute(
            "SELECT count(*) FROM lake_meta.ducklake_files_scheduled_for_deletion"
        ).fetchone()
        assert row is not None
        scheduled_count = row[0]

    assert report.exit_code == 2
    assert report.referenced == plan.paths
    assert scheduled_count == 2
    assert all(Path(path).exists() for path in plan.paths)
