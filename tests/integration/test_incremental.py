"""End-to-end incremental export against a real DuckLake.

Each test writes to DuckLake, exports to a local prefix and reads the resulting
Iceberg table with PyIceberg (``StaticTable``), comparing row by row with
DuckDB itself. Acceptance criteria: write cycles followed by sync keep Iceberg
and DuckDB consistent, and DELETE and UPDATE return the same set of rows. Also
holds the mandatory delete regression test.

Inlining is turned off in the catalog (``data_inlining_row_limit = 0``) so that
every write produces files, as in production after the checkpoint.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyiceberg.manifest import ManifestContent
from pyiceberg.transforms import BucketTransform, DayTransform

from ducklake_iceberg import timing
from ducklake_iceberg.errors import CompatibilityError, DivergenceError, ExitCode
from ducklake_iceberg.export import run_export
from ducklake_iceberg.writer.commit import referenced_data_file

from .conftest import CatalogHarness, Lake

pytestmark = pytest.mark.integration


@pytest.fixture
def lake(catalog: CatalogHarness, tmp_path: Path) -> Lake:
    prefix = tmp_path / "iceberg"
    prefix.mkdir()
    return Lake(catalog, prefix)


# -- incremental cycles (acceptance) -----------------------------------------


def test_incremental_cycles_mirror_ducklake(lake: Lake) -> None:
    lake.write(
        "CREATE TABLE lake.main.t (id INTEGER, v INTEGER)",
        "ALTER TABLE lake.main.t SET PARTITIONED BY (bucket(4, id))",
        "INSERT INTO lake.main.t SELECT i, i FROM range(50) r(i)",
    )
    first = lake.export()
    assert first.exit_code is ExitCode.SUCCESS
    assert first.operation == "append"
    assert first.plan.metadata_version == 1
    lake.assert_mirrors(first)

    steps: list[tuple[str, str, str | None]] = [
        ("INSERT INTO lake.main.t SELECT i, i FROM range(50, 60) r(i)", "append", None),
        ("DELETE FROM lake.main.t WHERE id % 7 = 0", "delete", None),
        ("DELETE FROM lake.main.t WHERE id = 1", "overwrite", "cumulative"),
        ("UPDATE lake.main.t SET v = -v WHERE id BETWEEN 10 AND 20", "overwrite", None),
        (
            "MERGE INTO lake.main.t AS tgt USING (SELECT i AS id, i * 100 AS v "
            "FROM range(55, 70) r(i)) src ON tgt.id = src.id "
            "WHEN MATCHED THEN UPDATE SET v = src.v "
            "WHEN NOT MATCHED THEN INSERT VALUES (src.id, src.v)",
            "overwrite",
            None,
        ),
        ("CALL ducklake_merge_adjacent_files('lake', 't')", "replace", None),
        (
            "CALL ducklake_rewrite_data_files('lake', 't', delete_threshold => 0.0)",
            "replace",
            None,
        ),
        ("DELETE FROM lake.main.t", "delete", None),
    ]
    previous = first
    for version, (sql, operation, _note) in enumerate(steps, start=2):
        lake.write(sql)
        result = lake.export()
        assert result.exit_code is ExitCode.SUCCESS, sql
        assert result.operation == operation, sql
        assert result.plan.metadata_version == version
        assert result.plan.previous_metadata_location == (
            previous.plan.metadata_location
        )
        lake.assert_mirrors(result)

        table = lake.iceberg(result)
        snapshot = table.current_snapshot()
        assert snapshot is not None
        assert snapshot.sequence_number == version
        assert snapshot.parent_snapshot_id == previous.snapshot_id
        assert [e.metadata_file for e in table.metadata.metadata_log][-1] == (
            previous.plan.metadata_location
        )
        previous = result

    # no change: exit 4, nothing written
    again = lake.export()
    assert again.exit_code is ExitCode.NOTHING_TO_DO
    assert not Path(again.plan.metadata_location).exists()


def test_delete_regression_does_not_touch_later_rows(lake: Lake) -> None:
    """Insert, export, delete, export, reinsert, export."""
    lake.write(
        "CREATE TABLE lake.main.t (id INTEGER, v VARCHAR)",
        "INSERT INTO lake.main.t SELECT i, 'old' FROM range(10) r(i)",
    )
    lake.export()
    lake.write("DELETE FROM lake.main.t WHERE id < 5")
    after_delete = lake.export()
    assert len(lake.iceberg_rows(after_delete)) == 5
    # the same keys come back in another data file, after the delete
    lake.write("INSERT INTO lake.main.t SELECT i, 'new' FROM range(5) r(i)")
    final = lake.export()
    rows = lake.iceberg_rows(final)
    assert rows == lake.rows()
    assert sorted(r for r in rows if r[1] == "new") == [(i, "new") for i in range(5)]
    phases = timing.snapshot()["phases_ms"]
    assert {
        "deletes.read",
        "deletes.encode",
        "deletes.upload",
        "s3.manifests_put",
        "s3.metadata_get",
        "s3.metadata_put",
        "s3.pointer_get",
        "s3.pointer_put",
    } <= phases.keys()
    assert timing.counter("deletes.source_files") == 1


def test_other_table_change_is_nothing_to_do(lake: Lake) -> None:
    lake.write(
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t VALUES (1)",
    )
    lake.export()
    lake.write(
        "CREATE TABLE lake.main.other (id INTEGER)",
        "INSERT INTO lake.main.other VALUES (1)",
    )
    assert lake.export().exit_code is ExitCode.NOTHING_TO_DO


def test_dry_run_writes_nothing_and_reports_delta(lake: Lake) -> None:
    lake.write(
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t VALUES (1), (2)",
    )
    lake.export()
    lake.write("INSERT INTO lake.main.t VALUES (3)")
    before = sorted(p.name for p in lake.prefix.rglob("*"))
    dry = run_export(lake.settings, "main", "t", dry_run=True)
    assert dry.exit_code is ExitCode.SUCCESS
    assert dry.plan.added_data_files == 1
    assert dry.plan.metadata_version == 2
    assert sorted(p.name for p in lake.prefix.rglob("*")) == before


# -- manifest reuse ----------------------------------------------------------


def test_append_reuses_previous_manifests(lake: Lake) -> None:
    lake.write(
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t VALUES (1), (2)",
    )
    first = lake.export()
    lake.write("INSERT INTO lake.main.t VALUES (3)")
    second = lake.export()
    io = lake.iceberg(second).io
    first_manifests = {
        m.manifest_path
        for m in lake.iceberg(first).current_snapshot().manifests(io)  # type: ignore[union-attr]
    }
    second_manifests = {
        m.manifest_path
        for m in lake.iceberg(second).current_snapshot().manifests(io)  # type: ignore[union-attr]
    }
    assert first_manifests < second_manifests  # reused by reference
    assert len(second.manifest_paths) == 1  # only the append manifest is new


# -- deletes -----------------------------------------------------------------


def test_position_delete_files_reference_one_data_file(lake: Lake) -> None:
    lake.write(
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t SELECT * FROM range(100)",
        "DELETE FROM lake.main.t WHERE id IN (3, 5, 7)",
    )
    result = lake.export()
    table = lake.iceberg(result)
    snapshot = table.current_snapshot()
    assert snapshot is not None
    delete_manifests = [
        m for m in snapshot.manifests(table.io) if m.content == ManifestContent.DELETES
    ]
    assert len(delete_manifests) == 1
    entries = delete_manifests[0].fetch_manifest_entry(table.io)
    assert len(entries) == 1
    delete_file = entries[0].data_file
    assert delete_file.record_count == 3
    assert "/deletes/" in delete_file.file_path
    data_paths = {t.file.file_path for t in table.scan().plan_files()}
    assert referenced_data_file(delete_file) in data_paths
    summary = snapshot.summary
    assert summary is not None
    assert summary.additional_properties["total-position-deletes"] == "3"
    assert summary.additional_properties["total-delete-files"] == "1"
    lake.assert_mirrors(result)


def test_partial_delete_file_rows_from_all_snapshots(lake: Lake) -> None:
    """A cumulative delete file carries the per-row snapshot column."""
    lake.write(
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t SELECT * FROM range(20)",
    )
    for ids in ("1, 2", "3", "4"):
        lake.write(f"DELETE FROM lake.main.t WHERE id IN ({ids})")
    result = lake.export()
    assert result.plan.added_delete_files == 1
    lake.assert_mirrors(result)


def test_position_deletes_are_consolidated_per_partition_and_reused(lake: Lake) -> None:
    lake.write(
        "CREATE TABLE lake.main.t (id INTEGER, p INTEGER)",
        "ALTER TABLE lake.main.t SET PARTITIONED BY (p)",
        "INSERT INTO lake.main.t VALUES (1, 7), (2, 7)",
        "DELETE FROM lake.main.t WHERE id = 1",
        "INSERT INTO lake.main.t VALUES (3, 7), (4, 7), (6, 7)",
        "DELETE FROM lake.main.t WHERE id = 3",
    )
    result = lake.export()
    table = lake.iceberg(result)
    snapshot = table.current_snapshot()
    assert snapshot is not None
    delete_manifests = [
        m for m in snapshot.manifests(table.io) if m.content == ManifestContent.DELETES
    ]
    entries = [
        entry
        for manifest in delete_manifests
        for entry in manifest.fetch_manifest_entry(table.io)
    ]
    assert len(entries) == 1
    delete_file = entries[0].data_file
    assert delete_file.record_count == 2
    assert referenced_data_file(delete_file) is None
    data_paths = sorted(
        entry.data_file.file_path
        for manifest in snapshot.manifests(table.io)
        if manifest.content == ManifestContent.DATA
        for entry in manifest.fetch_manifest_entry(table.io)
    )
    assert delete_file.lower_bounds is not None
    assert delete_file.upper_bounds is not None
    assert delete_file.lower_bounds[2147483546] == min(data_paths).encode()
    assert delete_file.upper_bounds[2147483546] == max(data_paths).encode()
    lake.assert_mirrors(result)

    unchanged = lake.export()
    assert unchanged.exit_code is ExitCode.NOTHING_TO_DO
    pointer = json.loads(Path(str(result.pointer_path)).read_text())
    assert pointer["metadata_version"] == result.plan.metadata_version

    lake.write("INSERT INTO lake.main.t VALUES (9, 7)")
    appended = lake.export()
    assert appended.plan.added_delete_files == 0
    assert appended.plan.removed_delete_files == 0
    appended_table = lake.iceberg(appended)
    appended_snapshot = appended_table.current_snapshot()
    assert appended_snapshot is not None
    appended_delete_paths = [
        entry.data_file.file_path
        for manifest in appended_snapshot.manifests(appended_table.io)
        if manifest.content == ManifestContent.DELETES
        for entry in manifest.fetch_manifest_entry(appended_table.io)
    ]
    assert appended_delete_paths == [delete_file.file_path]
    lake.assert_mirrors(appended)

    lake.write("DELETE FROM lake.main.t WHERE id = 4")
    updated = lake.export()
    assert updated.plan.added_delete_files == 1
    assert updated.plan.removed_delete_files == 1
    lake.assert_mirrors(updated)


# -- schema and field IDs ----------------------------------------------------


def test_nested_struct_keeps_ducklake_field_ids(lake: Lake) -> None:
    lake.write(
        "CREATE TABLE lake.main.t (a INTEGER, s STRUCT(x INTEGER, y INTEGER), "
        "b INTEGER)",
        "INSERT INTO lake.main.t VALUES (1, {'x': 10, 'y': 20}, 100)",
    )
    result = lake.export()
    schema = lake.iceberg(result).schema()
    assert schema.find_field("b").field_id == 5  # DuckLake column_id
    assert schema.find_field("s.x").field_id == 3
    assert lake.iceberg_rows(result) == [(1, {"x": 10, "y": 20}, 100)]


def test_drop_rename_and_add_column(lake: Lake) -> None:
    lake.write(
        "CREATE TABLE lake.main.t (a INTEGER, b INTEGER, c INTEGER)",
        "INSERT INTO lake.main.t VALUES (1, 2, 3)",
    )
    lake.export()
    lake.write(
        "ALTER TABLE lake.main.t DROP COLUMN b",
        "ALTER TABLE lake.main.t RENAME COLUMN c TO c2",
    )
    renamed = lake.export()
    assert renamed.exit_code is ExitCode.SUCCESS
    assert renamed.operation is None  # schema only: no new snapshot
    lake.assert_mirrors(renamed)
    lake.write(
        "ALTER TABLE lake.main.t ADD COLUMN d VARCHAR",
        "INSERT INTO lake.main.t VALUES (4, 6, 'x')",
    )
    added = lake.export()
    lake.assert_mirrors(added)
    schema = lake.iceberg(added).schema()
    assert [f.name for f in schema.fields] == ["a", "c2", "d"]
    assert [f.field_id for f in schema.fields] == [1, 3, 4]
    assert len(lake.iceberg(added).metadata.schemas) == 3


def test_epoch_day_partition_spec_and_predicate(lake: Lake) -> None:
    lake.write(
        "CREATE TABLE lake.main.t (id INTEGER, d DATE)",
        "ALTER TABLE lake.main.t SET PARTITIONED BY (epoch_day(d))",
        "INSERT INTO lake.main.t SELECT i, DATE '2024-01-01' + (i % 3)::INTEGER "
        "FROM range(30) r(i)",
    )
    result = lake.export()
    table = lake.iceberg(result)
    spec = table.spec()
    assert spec.spec_id != 0  # the DuckLake partition id is preserved
    assert [type(f.transform) for f in spec.fields] == [DayTransform]
    assert table.scan(row_filter="d = '2024-01-02'").to_arrow().num_rows == 10
    lake.assert_mirrors(result)


def test_bucket_partition_predicate(lake: Lake) -> None:
    lake.write(
        "CREATE TABLE lake.main.t (id INTEGER)",
        "ALTER TABLE lake.main.t SET PARTITIONED BY (bucket(8, id))",
        "INSERT INTO lake.main.t SELECT * FROM range(64)",
    )
    result = lake.export()
    table = lake.iceberg(result)
    assert [type(f.transform) for f in table.spec().fields] == [BucketTransform]
    assert table.scan(row_filter="id = 17").to_arrow().num_rows == 1


# -- guards ------------------------------------------------------------------


def test_pending_inlined_rows_block_export_with_existing_files(lake: Lake) -> None:
    lake.write(
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t SELECT * FROM range(10)",
        "CALL lake.set_option('data_inlining_row_limit', 100)",
        "INSERT INTO lake.main.t VALUES (99)",  # stays inlined
    )
    with pytest.raises(CompatibilityError, match="inlined") as exc:
        lake.export()
    assert exc.value.exit_code is ExitCode.BLOCKED


def test_recreated_table_is_divergence(lake: Lake) -> None:
    lake.write(
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t VALUES (1)",
    )
    lake.export()
    lake.write(
        "DROP TABLE lake.main.t",
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t VALUES (2)",
    )
    with pytest.raises(DivergenceError, match="recreated"):
        lake.export()


def test_pointer_tracks_last_export(lake: Lake) -> None:
    lake.write(
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t VALUES (1)",
    )
    first = lake.export()
    assert first.pointer_path is not None
    pointer = json.loads(Path(first.pointer_path).read_text())
    assert pointer["metadata_version"] == 1
    lake.write("INSERT INTO lake.main.t VALUES (2)")
    second = lake.export()
    pointer = json.loads(Path(str(second.pointer_path)).read_text())
    assert pointer["metadata_location"] == second.plan.metadata_location
    props = lake.iceberg(second).metadata.properties
    assert props["ducklake.snapshot-id"] == str(second.plan.ducklake_snapshot_id)
    assert props["ducklake_iceberg.iceberg-snapshot-id"] == str(second.snapshot_id)
    assert props["ducklake_iceberg.sync-mode"] == "incremental"


def test_orphan_successor_metadata_is_adopted(lake: Lake) -> None:
    """An export that died between the metadata.json and the pointer does not
    block the next one."""
    lake.write(
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t VALUES (1)",
    )
    first = lake.export()
    assert first.pointer_path is not None
    pointer_v1 = Path(first.pointer_path).read_text()
    lake.write("INSERT INTO lake.main.t VALUES (2)")
    second = lake.export()
    Path(first.pointer_path).write_text(pointer_v1)  # simulates the crash
    lake.write("INSERT INTO lake.main.t VALUES (3)")
    third = lake.export()
    assert third.plan.metadata_version == 3
    assert third.plan.previous_metadata_location == second.plan.metadata_location
    assert third.plan.added_data_files == 1
    lake.assert_mirrors(third)
