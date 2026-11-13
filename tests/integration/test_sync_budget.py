"""Budget of expensive operations in a ``sync`` (target: < 1 s per table).

Each DuckDB connection used to cost ~0.7 s (extensions + SSO credential +
ATTACH) and each new ``boto3.Session`` cost an SSO call. These tests pin the
ceiling: one READ_ONLY connection per sync with no pending inlined data, no
write connection when there is no work, a flush (and nothing else) when
inlined data is pending, and a single Session.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from moto import mock_aws

from ducklake_iceberg import timing
from ducklake_iceberg.catalog.connection import DuckLakeConnection
from ducklake_iceberg.checkpoint.model import TableRef

from .conftest import CatalogFactory, CatalogHarness, run_sync, sync_config

pytestmark = [pytest.mark.integration, pytest.mark.aws]


@pytest.fixture
def glue_env() -> Iterator[None]:
    with mock_aws(), mock.patch("ducklake_iceberg.glue._validate_metadata_location"):
        yield


@pytest.fixture
def opened(monkeypatch: pytest.MonkeyPatch) -> list[bool]:
    """``read_only`` flag of each DuckDB connection opened."""
    modes: list[bool] = []
    real_open = DuckLakeConnection.open

    def spy(self: DuckLakeConnection) -> DuckLakeConnection:
        modes.append(self.read_only)
        return real_open(self)

    monkeypatch.setattr(DuckLakeConnection, "open", spy)
    return modes


@pytest.fixture
def maintenance_calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Tables flushed (``ducklake_flush_inlined_data``) by the checkpoint stage."""
    from ducklake_iceberg.checkpoint import flush

    calls: list[str] = []
    real_call = flush._flush_call

    def spy(con: DuckLakeConnection, table: TableRef) -> int:
        calls.append(table.qualified)
        return real_call(con, table)

    monkeypatch.setattr(flush, "_flush_call", spy)
    return calls


def _steps(doc: dict[str, Any]) -> list[str]:
    return [s["step"] for s in doc["results"][0]["steps"]]


def test_sync_without_inlined_uses_one_read_only_connection(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
    opened: list[bool],
    maintenance_calls: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = make_catalog(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t SELECT * FROM range(10)",
    )
    config = sync_config(catalog, tmp_path)
    _forbid(monkeypatch, "ducklake_iceberg.checkpoint.flush.read_fingerprint")

    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    assert opened == [True]
    assert maintenance_calls == []
    # snapshot unchanged by the checkpoint: the doctor is not repeated
    assert _steps(doc) == ["doctor", "checkpoint", "export", "register"]

    opened.clear()
    code, doc = run_sync(config)
    assert code == 4
    assert opened == [True]
    assert timing.counter("duckdb.connect") == 1
    assert timing.counter("doctor.query_cache_hits") == 1


def test_sync_resolves_aws_session_once(
    make_catalog: CatalogFactory, tmp_path: Path, glue_env: None
) -> None:
    catalog = make_catalog(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t SELECT * FROM range(10)",
    )
    config = sync_config(catalog, tmp_path)

    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    # export (FileIO) and register (FileIO, GlueCatalog, GetTable/UpdateTable)
    # share the process-wide Session and FileIO
    assert timing.counter("boto3.session") == 1
    assert timing.counter("fileio") == 1


def test_sync_with_inlined_only_flushes(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
    opened: list[bool],
    maintenance_calls: list[str],
) -> None:
    catalog = make_catalog(
        "CALL lake.set_option('data_inlining_row_limit', 1000)",
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t SELECT * FROM range(10)",
    )
    config = sync_config(catalog, tmp_path)

    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    assert maintenance_calls == ["main.t"]
    assert opened == [True]  # RW is a temporary attachment on the same root
    assert timing.counter("ducklake.attach.successes") == 3
    assert timing.counter("ducklake.reattach.rw") == 1
    assert timing.counter("ducklake.reattach.ro") == 1
    # the flush changed the snapshot: post-checkpoint revalidation
    assert _steps(doc) == ["doctor", "checkpoint", "doctor", "export", "register"]
    assert timing.counter("inlining.cache_hits") >= 2
    phases = timing.snapshot()["phases_ms"]
    assert phases["checkpoint.flush_call"] >= 0
    assert phases["catalog.attach"] >= 0
    assert timing.counter("catalog.queries") > 0


def _forbid(monkeypatch: pytest.MonkeyPatch, *targets: str) -> None:
    def boom(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("read that sync should not perform")

    for target in targets:
        monkeypatch.setattr(target, boom)


def test_unchanged_sync_reads_only_pointer_and_glue(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = make_catalog(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t SELECT * FROM range(10)",
    )
    config = sync_config(catalog, tmp_path)
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]

    published = doc["results"][0]["metadata_location"]
    # no table token in ducklake_snapshot_changes: no metadata.json, no
    # manifests, and no re-read in register
    catalog.write("CREATE TABLE lake.main.outra (x INTEGER)")
    _forbid(
        monkeypatch,
        "ducklake_iceberg.export.read_metadata_json",
        "ducklake_iceberg.export.load_base_snapshot",
        "ducklake_iceberg.register._read_metadata_properties",
        "ducklake_iceberg.glue._read_metadata",
    )
    code, doc = run_sync(config)
    assert code == 4, doc["problems"]
    steps = {s["step"]: s for s in doc["results"][0]["steps"]}
    assert steps["export"]["exit_code"] == 4
    assert steps["export"]["metadata_location"] == published
    assert steps["export"]["metadata_version"] == 1
    assert Path(published).is_file()
    assert steps["register"]["exit_code"] == 4


def test_changed_sync_register_does_not_reread(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = make_catalog(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t SELECT * FROM range(10)",
    )
    config = sync_config(catalog, tmp_path)
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]

    catalog.write("INSERT INTO lake.main.t SELECT * FROM range(10, 20)")
    _forbid(
        monkeypatch,
        "ducklake_iceberg.register.read_pointer",
        "ducklake_iceberg.register._read_metadata_properties",
        "ducklake_iceberg.glue._read_metadata",
        "ducklake_iceberg.glue._metadata_exists",
    )
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    steps = {s["step"]: s for s in doc["results"][0]["steps"]}
    assert steps["register"]["outcome"] == "updated"
    assert steps["register"]["metadata_location"].endswith("v2.metadata.json")


def test_unchanged_after_upgrade_takes_full_path(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Different tool version in the pointer: the shortcut does not apply."""
    catalog = make_catalog(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t SELECT * FROM range(10)",
    )
    config = sync_config(catalog, tmp_path)
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]

    from ducklake_iceberg import export
    from ducklake_iceberg.writer.commit import load_base_snapshot as real

    calls: list[object] = []

    def spy(*a: Any, **k: Any) -> Any:
        calls.append(a)
        return real(*a, **k)

    monkeypatch.setattr(export, "load_base_snapshot", spy)
    monkeypatch.setattr(export, "__version__", "9.9.9")
    code, doc = run_sync(config)
    assert code == 4, doc["problems"]
    assert calls  # full path: manifests re-read and compared


def test_unchanged_shortcut_refuses_window_with_expired_snapshots(
    catalog: CatalogHarness,
    tmp_path: Path,
    glue_env: None,
) -> None:
    """A snapshot missing from the window may carry the table token: full path.

    DuckLake 1.1 keeps ``ducklake_snapshot_changes`` on expire; the state is
    forged (snapshot and change record deleted) to cover a version that
    clears the record as well.
    """
    if catalog.backend != "sqlite":
        pytest.skip("state is forged directly in SQLite")
    config = sync_config(catalog, tmp_path)
    catalog.write(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.t (id INTEGER)",
        "INSERT INTO lake.main.t SELECT * FROM range(10)",
    )
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]

    catalog.write(
        "INSERT INTO lake.main.t SELECT * FROM range(10, 15)",
        "CREATE TABLE lake.main.outra (x INTEGER)",
    )
    for table in ("ducklake_snapshot_changes", "ducklake_snapshot"):
        catalog.meta_exec(
            f"DELETE FROM {table} WHERE snapshot_id = (SELECT snapshot_id FROM "
            "ducklake_snapshot_changes WHERE changes_made = 'inserted_into_table:1' "
            "ORDER BY snapshot_id DESC LIMIT 1)"
        )

    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    steps = {s["step"]: s for s in doc["results"][0]["steps"]}
    assert steps["export"]["added_data_files"] == 1
    assert steps["register"]["outcome"] == "updated"
