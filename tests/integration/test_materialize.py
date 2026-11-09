"""``--materialize-deletes`` against a real DuckLake catalog.

The plan runs READ_ONLY; the rewrite (``ducklake_rewrite_data_files``) needs a
write connection. "Nothing to do" (no deletions, or no file over the
threshold) is ``NothingToDo`` -> exit 4.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from ducklake_iceberg.catalog.connection import open_catalog
from ducklake_iceberg.catalog.reader import TableRef, current_snapshot, resolve_table
from ducklake_iceberg.deletes import materialize as mt
from ducklake_iceberg.errors import ExitCode
from tests.integration.conftest import CatalogHarness

pytestmark = [pytest.mark.integration]


def _table_with_deletes(h: CatalogHarness, *, deletes: bool = True) -> None:
    h.write("CALL lake.set_option('data_inlining_row_limit', 0)")
    h.write("CREATE TABLE lake.main.t (id INTEGER)")
    h.write("INSERT INTO lake.main.t SELECT range FROM range(10)")
    h.write("INSERT INTO lake.main.t SELECT range FROM range(10, 20)")
    if deletes:
        h.write("DELETE FROM lake.main.t WHERE id IN (1, 2, 3)")


def _plan(h: CatalogHarness) -> mt.MaterializePlan:
    with open_catalog(h.settings(), read_only=True) as con, con.read_transaction():
        snap = current_snapshot(con)
        ident = resolve_table(con, TableRef("main", "t"), snap)
        return mt.plan_materialize(con, ident.table_id, snap.snapshot_id)


def _count(h: CatalogHarness) -> int:
    with open_catalog(h.settings()) as con:
        value = con.query_arrow("SELECT count(*) AS n FROM lake.main.t").to_pylist()
    return int(value[0]["n"])


def _current_snapshot_id(h: CatalogHarness) -> int:
    with open_catalog(h.settings()) as con:
        return current_snapshot(con).snapshot_id


# --- plan (READ_ONLY) --------------------------------------------------------
def test_plan_in_read_only(catalog: CatalogHarness) -> None:
    _table_with_deletes(catalog)
    plan = _plan(catalog)
    assert plan.delete_files == 1
    assert plan.deleted_rows == 3
    assert plan.data_files_affected == 1
    assert plan.bytes_to_rewrite > 0
    assert plan.max_deleted_fraction == pytest.approx(0.3)
    assert plan.has_puffin is False
    assert not plan.is_empty

    text = mt.format_plan(plan)
    assert "Delete files: 1" in text
    assert "30.00%" in text
    assert "puffin" not in text
    assert "puffin" in mt.format_plan(
        mt.MaterializePlan(1, 1, 1, 1, 1, 1, 0.1, has_puffin=True)
    )


def test_plan_without_deletes_is_nothing_to_do(catalog: CatalogHarness) -> None:
    _table_with_deletes(catalog, deletes=False)
    with pytest.raises(mt.NoDeletesError) as exc_info:
        _plan(catalog)
    assert exc_info.value.exit_code == ExitCode.NOTHING_TO_DO
    assert mt.MaterializePlan(1, 1, 0, 0, 0, 0, 0.0, has_puffin=False).is_empty


# --- execution ---------------------------------------------------------------
def test_materialize_rewrites_and_clears_deletes(catalog: CatalogHarness) -> None:
    _table_with_deletes(catalog)
    before = _current_snapshot_id(catalog)
    with open_catalog(catalog.settings(), read_only=False) as con:
        result = mt.materialize_deletes(
            con,
            catalog_name=con.alias,
            schema_name="main",
            table_name="t",
            snapshot_before=before,
            max_compacted_files=10,
        )
    assert result.files_processed >= 1
    assert result.files_created >= 1
    assert not result.no_op
    assert result.snapshot_before == before
    assert result.snapshot_after > before
    assert result.duration_ms >= 0
    assert f"snapshot {before} → {result.snapshot_after}" in mt.format_result(result)

    # deleted rows were removed at the source: no live delete remains
    assert _count(catalog) == 17
    with pytest.raises(mt.NoDeletesError):
        _plan(catalog)


def test_materialize_threshold_not_reached(catalog: CatalogHarness) -> None:
    _table_with_deletes(catalog)
    with (
        open_catalog(catalog.settings(), read_only=False) as con,
        pytest.raises(mt.NoFilesProcessedError) as exc_info,
    ):
        mt.materialize_deletes(
            con,
            catalog_name=con.alias,
            schema_name="main",
            table_name="t",
            snapshot_before=1,
            delete_threshold=0.9,
        )
    assert exc_info.value.exit_code == ExitCode.NOTHING_TO_DO
    assert "0.9" in exc_info.value.message


def test_materialize_in_read_only_connection(catalog: CatalogHarness) -> None:
    _table_with_deletes(catalog)
    with (
        open_catalog(catalog.settings(), read_only=True) as con,
        pytest.raises(mt.ReadOnlyDatabaseError),
    ):
        mt.materialize_deletes(
            con,
            catalog_name=con.alias,
            schema_name="main",
            table_name="t",
            snapshot_before=1,
        )


def test_materialize_unknown_table_fails(catalog: CatalogHarness) -> None:
    _table_with_deletes(catalog)
    with (
        open_catalog(catalog.settings(), read_only=False) as con,
        pytest.raises(mt.MaterializeError, match="Failed to materialize"),
    ):
        mt.materialize_deletes(
            con,
            catalog_name=con.alias,
            schema_name="main",
            table_name="does_not_exist",
            snapshot_before=1,
        )


class _ConflictingConnection:
    meta = "lake_meta"

    def query_arrow(self, sql: str, params: Any = None) -> pa.Table:
        raise RuntimeError("Transaction conflict: concurrent write")


def test_materialize_conflict() -> None:
    con: Any = _ConflictingConnection()
    with pytest.raises(mt.CompactionConflictError):
        mt.materialize_deletes(
            con,
            catalog_name="lake",
            schema_name="main",
            table_name="t",
            snapshot_before=1,
        )


# --- run_materialize (command step) ------------------------------------------
def test_run_materialize_dry_run_does_not_write(catalog: CatalogHarness) -> None:
    _table_with_deletes(catalog)
    before = _current_snapshot_id(catalog)
    detail = mt.run_materialize(catalog.settings(), "main", "t", dry_run=True)
    assert detail["dry_run"] is True
    plan = detail["plan"]
    assert isinstance(plan, dict)
    assert plan["delete_files"] == 1
    assert "result" not in detail
    assert _current_snapshot_id(catalog) == before


def test_run_materialize_executes(catalog: CatalogHarness) -> None:
    _table_with_deletes(catalog)
    detail = mt.run_materialize(catalog.settings(), "main", "t")
    result = detail["result"]
    assert isinstance(result, dict)
    assert result["files_processed"] >= 1
    assert result["table_name"] == "t"
    assert _count(catalog) == 17
    # second run: nothing to materialize (exit 4)
    with pytest.raises(mt.NoDeletesError) as exc_info:
        mt.run_materialize(catalog.settings(), "main", "t")
    assert exc_info.value.exit_code == ExitCode.NOTHING_TO_DO


def test_run_materialize_threshold_is_nothing_to_do(catalog: CatalogHarness) -> None:
    _table_with_deletes(catalog)
    with pytest.raises(mt.NoFilesProcessedError) as exc_info:
        mt.run_materialize(catalog.settings(), "main", "t", delete_threshold=0.95)
    assert exc_info.value.exit_code == ExitCode.NOTHING_TO_DO
