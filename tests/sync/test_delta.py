"""Delta computation against a real DuckLake catalog (SQLite)."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest

from iceduck.catalog.connection import DuckLakeConnection, open_catalog
from iceduck.catalog.files import PathLayers, read_path_layers
from iceduck.sync.delta import (
    CatalogRewoundError,
    CompatibilityBlockerError,
    CrossCheckError,
    DeltaError,
    DeltaMode,
    RetentionGapError,
    TableDelta,
    canonical_path,
    compose_absolute_path,
    compute_delta,
)
from tests.integration.conftest import CatalogHarness

pytestmark = pytest.mark.integration


@dataclass
class _Ctx:
    con: DuckLakeConnection
    table_id: int
    layers: PathLayers
    current: int

    def delta(self, prev: int | None, **extra: Any) -> TableDelta:
        kwargs: dict[str, Any] = {
            "meta": self.con.meta,
            "lake": self.con.alias,
            "schema_name": "main",
            "table_name": "t",
            "table_id": self.table_id,
            "previous_snapshot_id": prev,
            "target_snapshot_id": self.current,
            "data_path": self.layers.data_path,
            "schema_path": self.layers.schema_path,
            "schema_path_is_relative": self.layers.schema_path_is_relative,
            "table_path": self.layers.table_path,
            "table_path_is_relative": self.layers.table_path_is_relative,
        }
        kwargs.update(extra)
        return compute_delta(self.con.raw, **kwargs)


def _open(lake: CatalogHarness) -> Iterator[_Ctx]:
    with open_catalog(lake.settings()) as con:
        meta = con.meta
        (row,) = con.raw.execute(
            f"SELECT table_id FROM {meta}.ducklake_table "
            "WHERE table_name = 't' AND end_snapshot IS NULL"
        ).fetchall()
        (snap,) = con.raw.execute(
            f"SELECT max(snapshot_id) FROM {meta}.ducklake_snapshot"
        ).fetchall()
        table_id = int(row[0])
        yield _Ctx(
            con=con,
            table_id=table_id,
            layers=read_path_layers(con.raw, meta, table_id, int(snap[0])),
            current=int(snap[0]),
        )


@pytest.fixture
def three_inserts(lake: CatalogHarness) -> CatalogHarness:
    """Snapshots: 1 create, 2/3/4 inserts (one file each)."""
    lake.write("CREATE TABLE lake.main.t (id INTEGER)")
    for lo in (0, 10, 20):
        lake.write(f"INSERT INTO lake.main.t SELECT * FROM range({lo}, {lo + 10})")
    return lake


@pytest.fixture
def ctx(three_inserts: CatalogHarness) -> Iterator[_Ctx]:
    yield from _open(three_inserts)


def test_full_delta_lists_live_files(ctx: _Ctx) -> None:
    d = ctx.delta(None)
    assert d.mode is DeltaMode.FULL
    assert len(d.added) == 3
    assert d.removed == ()
    assert d.transient == ()
    assert d.counters.added_records == 30
    assert d.counters.added_bytes == sum(f.file_size_bytes for f in d.added)
    assert all(f.path.startswith(ctx.layers.data_path) for f in d.added)
    assert all("/main/t/" in f.path for f in d.added)
    assert not d.is_empty


def test_incremental_delta(ctx: _Ctx) -> None:
    d = ctx.delta(3)
    assert d.mode is DeltaMode.INCREMENTAL
    assert [f.begin_snapshot for f in d.added] == [4]
    assert d.counters.added_files == 1
    assert d.counters.removed_files == 0


def test_noop_and_rewound(ctx: _Ctx) -> None:
    d = ctx.delta(ctx.current)
    assert d.mode is DeltaMode.NOOP
    assert d.is_empty
    with pytest.raises(CatalogRewoundError) as exc:
        ctx.delta(ctx.current + 1)
    assert exc.value.exit_code == 3


def test_iceberg_reconstruction_check(ctx: _Ctx) -> None:
    full = ctx.delta(None)
    previous = frozenset(f.path for f in full.added if f.begin_snapshot < 4)
    d = ctx.delta(3, iceberg_live_paths=previous)
    assert len(d.added) == 1
    with pytest.raises(CrossCheckError, match="reconstructed"):
        ctx.delta(3, iceberg_live_paths=frozenset())


def test_list_files_cross_check_detects_wrong_composition(ctx: _Ctx) -> None:
    with pytest.raises(CrossCheckError, match="ducklake_list_files"):
        ctx.delta(None, data_path="/outro/lugar/")


def test_removed_and_transient(three_inserts: CatalogHarness) -> None:
    three_inserts.write("CALL ducklake_merge_adjacent_files('lake')")  # S5
    three_inserts.write("DELETE FROM lake.main.t")  # S6: file ended
    for c in _open(three_inserts):
        removed = c.delta(5)
        assert len(removed.removed) == 1
        assert removed.added == ()
        assert removed.counters.removed_records == 30
        transient = c.delta(1)
        assert len(transient.transient) == 1
        assert transient.counters.transient_files == 1
        assert transient.transient[0].path.startswith(c.layers.data_path)
        assert transient.transient[0].path_is_relative


def test_parquet_delete_files_do_not_block(three_inserts: CatalogHarness) -> None:
    three_inserts.write("DELETE FROM lake.main.t WHERE id < 3")
    for c in _open(three_inserts):
        assert len(c.delta(None).added) == 3


def test_puffin_delete_files_block(three_inserts: CatalogHarness) -> None:
    three_inserts.write("DELETE FROM lake.main.t WHERE id < 3")
    three_inserts.meta_exec("UPDATE ducklake_delete_file SET format = 'puffin'")
    for c in _open(three_inserts):
        with pytest.raises(CompatibilityBlockerError, match="puffin") as exc:
            c.delta(None)
        assert exc.value.exit_code == 2


@pytest.mark.parametrize(
    ("sql", "match"),
    [
        ("UPDATE ducklake_data_file SET encryption_key = 'k'", "encryption_key"),
        ("UPDATE ducklake_data_file SET file_format = 'orc'", "orc"),
        ("UPDATE ducklake_data_file SET partial_max = 999", "partial_max"),
    ],
)
@pytest.mark.parametrize("prev", [None, 3])
def test_format_guards_full_and_incremental(
    three_inserts: CatalogHarness, sql: str, match: str, prev: int | None
) -> None:
    three_inserts.meta_exec(sql + " WHERE begin_snapshot = 4")
    for c in _open(three_inserts):
        with pytest.raises(CompatibilityBlockerError, match=match):
            c.delta(prev)


def test_format_guard_ignores_files_not_added(three_inserts: CatalogHarness) -> None:
    three_inserts.meta_exec(
        "UPDATE ducklake_data_file SET encryption_key = 'k' WHERE begin_snapshot = 2"
    )
    for c in _open(three_inserts):
        assert len(c.delta(3).added) == 1  # the encrypted file was already published


def test_retention_gap(three_inserts: CatalogHarness) -> None:
    three_inserts.meta_exec("DELETE FROM ducklake_snapshot WHERE snapshot_id < 2")
    for c in _open(three_inserts):
        with pytest.raises(RetentionGapError, match="expired"):
            c.delta(1)


def test_scheduled_for_deletion(three_inserts: CatalogHarness) -> None:
    three_inserts.meta_exec(
        "INSERT INTO ducklake_files_scheduled_for_deletion "
        "SELECT data_file_id, path, path_is_relative, '2026-01-01' "
        "FROM ducklake_data_file WHERE begin_snapshot = 2"
    )
    for c in _open(three_inserts):
        with pytest.raises(RetentionGapError, match="scheduled_for_deletion"):
            c.delta(3)


def test_zero_size_file_is_error(three_inserts: CatalogHarness) -> None:
    three_inserts.meta_exec(
        "UPDATE ducklake_data_file SET file_size_bytes = 0 WHERE begin_snapshot = 4"
    )
    for c in _open(three_inserts):
        with pytest.raises(DeltaError, match="file_size_bytes") as exc:
            c.delta(3)
        assert exc.value.exit_code == 1


def test_compose_absolute_path_layers() -> None:
    assert compose_absolute_path(
        "s3://b/d/", None, True, None, True, "/x/f", False
    ) == ("/x/f")
    assert (
        compose_absolute_path("s3://b/d/", "s/", True, "t/", True, "f.parquet", True)
        == "s3://b/d/s/t/f.parquet"
    )
    assert (
        compose_absolute_path(
            "s3://b/d/", "s3://o/s/", False, "t/", True, "f.parquet", True
        )
        == "s3://o/s/t/f.parquet"
    )
    assert (
        compose_absolute_path(
            "s3://b/d/", "s/", True, "s3://o/t/", False, "f.parquet", True
        )
        == "s3://o/t/f.parquet"
    )


def test_canonical_path_matches_catalog_files() -> None:
    assert canonical_path("S3://b//a/./c.parquet") == "s3://b/a/c.parquet"
    assert canonical_path("s3://b/a/../c") == "s3://b/a/../c"  # does not resolve ..
