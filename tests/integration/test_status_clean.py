"""``ducklake-iceberg status`` and ``ducklake-iceberg clean`` end to end.

Real DuckLake (SQLite) -> ``ducklake-iceberg sync`` -> moto Glue, with the metadata
under a **local** prefix (PyArrow S3 is not intercepted by moto); that is why
the ``s3://`` check on registration is relaxed here, as in ``test_sync_e2e``.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import boto3
import pyarrow as pa
from moto import mock_aws
from pyiceberg.catalog.glue import GlueCatalog
from typer.testing import CliRunner

from ducklake_iceberg.cli import app

from .conftest import CatalogHarness

pytestmark = [pytest.mark.integration, pytest.mark.aws]

runner = CliRunner()
REGION = "us-east-1"
OLD = 3 * 86400  # older than the default grace period (24 h)


@pytest.fixture
def glue_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("DUCKLAKE_ICEBERG_STATE_DIR", str(tmp_path / "state"))
    with mock_aws(), mock.patch("ducklake_iceberg.glue._validate_metadata_location"):
        yield


@pytest.fixture
def config(catalog: CatalogHarness, tmp_path: Path) -> Path:
    catalog.write("CALL lake.set_option('data_inlining_row_limit', 0)")
    path = tmp_path / "ducklake-iceberg.yaml"
    path.write_text(
        f"catalog_dsn: '{catalog.dsn}'\n"
        f"data_path: '{catalog.data_path}'\n"
        f"iceberg_metadata_prefix: '{tmp_path / 'iceberg'}'\n"
        f"glue_database: analytics\n"
        f"aws_region: {REGION}\n"
    )
    return path


@pytest.fixture
def location(tmp_path: Path) -> Path:
    return tmp_path / "iceberg" / "analytics" / "t"


def _invoke(config: Path, *args: str) -> tuple[int, dict[str, Any]]:
    result = runner.invoke(app, [*args, "--config", str(config), "--json"])
    return result.exit_code, json.loads(result.stdout)


def _sync(config: Path) -> None:
    code, doc = _invoke(config, "sync", "--table", "main.t")
    assert code == 0, doc["problems"]


def _status(config: Path, *extra: str) -> tuple[int, dict[str, Any]]:
    code, doc = _invoke(config, "status", *extra)
    assert len(doc["results"]) <= 1  # a single document
    return code, doc


def _one(doc: dict[str, Any]) -> dict[str, Any]:
    tables = doc["results"][0]["tables"]
    assert len(tables) == 1
    table: dict[str, Any] = tables[0]
    return table


def _clean(config: Path, *extra: str) -> tuple[int, dict[str, Any]]:
    return _invoke(config, "clean", "--table", "main.t", *extra)


def _age(path: Path, seconds: int = OLD) -> None:
    ts = time.time() - seconds
    os.utime(path, (ts, ts))


def _glue_table() -> Any:
    return GlueCatalog("t", **{"glue.region": REGION}).load_table(("analytics", "t"))


def _point_glue_to(metadata_location: str) -> None:
    glue = boto3.client("glue", region_name=REGION)
    table = glue.get_table(DatabaseName="analytics", Name="t")["Table"]
    params = dict(table["Parameters"])
    params["metadata_location"] = metadata_location
    glue.update_table(
        DatabaseName="analytics",
        TableInput={
            "Name": "t",
            "TableType": table["TableType"],
            "Parameters": params,
            "StorageDescriptor": table["StorageDescriptor"],
        },
    )


# -- status ---------------------------------------------------------------------


def test_status_lifecycle(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v INTEGER)")
    catalog.write("CREATE TABLE lake.main.other (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t SELECT i, i FROM range(20) r(i)")

    code, doc = _status(config, "--table", "main.t")
    assert code == 3
    assert _one(doc)["status"] == "never_synced"

    _sync(config)
    code, doc = _status(config, "--table", "main.t")
    table = _one(doc)
    assert code == 4, table
    assert table["status"] == "in_sync"
    assert table["pending"]["snapshots"] == 0
    assert (
        table["state"]["iceberg_snapshot_id"] == table["iceberg"]["current_snapshot_id"]
    )
    assert table["ducklake"]["table_uuid"] is not None
    assert table["iceberg"]["total_records"] == 20
    summary = doc["results"][0]["summary"]
    assert summary == {**summary, "tables": 1, "in_sync": 1, "exit_code": 4}

    # another table advances D without changing this one: still in sync (like export)
    catalog.write("INSERT INTO lake.main.other VALUES (1)")
    code, doc = _status(config, "--table", "main.t")
    table = _one(doc)
    assert code == 4
    assert table["pending"]["snapshots"] == 1
    assert any(f["severity"] == "info" for f in table["findings"])

    catalog.write("INSERT INTO lake.main.t VALUES (100, 100)")
    catalog.write("DELETE FROM lake.main.t WHERE id < 3")
    code, doc = _status(config, "--table", "main.t")
    table = _one(doc)
    assert code == 3
    assert table["status"] == "behind"
    pending = table["pending"]
    assert pending["added_files"] == 1
    assert pending["added_records"] == 1
    assert pending["added_delete_files"] == 1
    assert pending["removed_files"] == 0
    assert table["findings"][0]["remediation"] == "ducklake-iceberg sync --table main.t"

    code, doc = _status(config, "--table", "main.t", "--no-delta")
    assert code == 3
    assert _one(doc)["pending"] is None
    assert _one(doc)["status"] == "behind"

    code, doc = _status(config, "--table", "main.t", "--exit-zero")
    assert code == 0
    assert doc["results"][0]["summary"]["exit_code"] == 3

    _sync(config)
    code, doc = _status(config, "--table", "main.t")
    assert code == 4
    assert _one(doc)["pending"]["added_delete_files"] == 0


def test_validate_reconciles_published_table(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1, 10), (2, 20), (3, 30)")
    _sync(config)

    code, doc = _invoke(config, "validate", "--table", "main.t", "--deep")

    assert code == 0, doc["problems"]
    result = doc["results"][0]
    assert result["ducklake_snapshot_id"] > 0
    assert result["iceberg_snapshot_id"] > 0
    assert [check["id"] for check in result["checks"]] == [
        "row_count",
        "column_aggregates",
        "partition_predicate",
        "bounds_predicate",
        "row_sample",
    ]
    assert all(check["status"] in {"pass", "skipped"} for check in result["checks"])
    row_count = result["checks"][0]["details"]
    assert row_count["ducklake_rows"] == 3
    assert row_count["iceberg_deep_rows"] == 3


def test_validate_partition_and_bounds_predicates(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, d DATE)")
    catalog.write("ALTER TABLE lake.main.t SET PARTITIONED BY (epoch_day(d))")
    catalog.write(
        "INSERT INTO lake.main.t SELECT i, DATE '2024-01-01' + "
        "(i % 3)::INTEGER FROM range(30) r(i)"
    )
    _sync(config)

    code, doc = _invoke(config, "validate", "--table", "main.t")

    assert code == 0, doc["problems"]
    checks = {item["id"]: item for item in doc["results"][0]["checks"]}
    assert checks["partition_predicate"]["status"] == "pass"
    assert {
        item["kind"] for item in checks["partition_predicate"]["details"]["predicates"]
    } == {
        "frequent",
        "rare",
        "absent",
    }
    assert checks["bounds_predicate"]["status"] == "pass"


def test_validate_retention_simulates_only_with_declared_windows(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1), (2)")
    retention_config = config.with_name("retention.yaml")
    retention_config.write_text(
        config.read_text(encoding="utf-8") + "\nretention:\n"
        "  expire_window: 30d\n"
        "  cleanup_window: 7d\n"
        "  sync_interval: 1h\n"
        "  sync_p95: 4m\n"
        "  safety_factor: 2.0\n",
        encoding="utf-8",
    )
    _sync(retention_config)

    code, doc = _invoke(
        retention_config,
        "validate",
        "--table",
        "main.t",
        "--only",
        "retention",
        "--check-s3",
        "full",
    )

    assert code == 0, doc["problems"]
    result = doc["results"][0]
    assert result["policy"]["source"] == "config"
    assert result["policy"]["confidence"] == "declared"
    assert result["risk"]["severity"] == "ok"
    assert result["referenced_files"] > 0
    assert result["doomed_snapshots"] == []
    assert result["s3_checked"] > 0
    assert result["s3_missing"] == []


def test_validate_flags_external_iceberg_write(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1, 10)")
    _sync(config)
    table = _glue_table()
    table.append(
        pa.table(
            {
                "id": pa.array([2], type=pa.int32()),
                "v": pa.array([20], type=pa.int32()),
            }
        )
    )

    code, doc = _invoke(config, "validate", "--table", "main.t")

    assert code == 3
    result = doc["results"][0]
    assert result["external_write"] is True
    assert result["checks"][0]["details"]["external_write"] is True


def test_status_schema_drift_and_rich_output(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1, 1)")
    _sync(config)
    catalog.write("ALTER TABLE lake.main.t RENAME COLUMN v TO w")
    code, doc = _status(config, "--table", "main.t")
    table = _one(doc)
    assert code == 3
    assert table["status"] == "schema_drift"
    assert table["pending"]["added_files"] == 0

    result = runner.invoke(
        app, ["status", "--table", "main.t", "--config", str(config)]
    )
    assert result.exit_code == 3
    assert "SCHEMA_DRIFT" in result.stdout
    assert "Run: ducklake-iceberg sync --table main.t" in result.stdout


def test_status_external_write_and_diverged_both(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1)")
    _sync(config)
    # INSERT "via Athena": direct PyIceberg commit on the mirror table
    _glue_table().append(pa.table({"id": pa.array([42], pa.int32())}))

    code, doc = _status(config, "--table", "main.t")
    table = _one(doc)
    assert code == 3
    assert table["status"] == "external_write"
    path = table["findings"][0]["detail"]["path"]
    assert [p["operation"] for p in path] == ["append"]
    assert path[0]["snapshot_id"] == table["iceberg"]["current_snapshot_id"]
    assert table["pending"] is None

    catalog.write("INSERT INTO lake.main.t VALUES (2)")
    code, doc = _status(config, "--table", "main.t")
    table = _one(doc)
    assert code == 3
    assert table["status"] == "diverged_both"
    codes = [f["code"] for f in table["findings"]]
    assert codes[:3] == ["diverged_both", "external_write", "behind"]

    # clean is suspended
    code, doc = _clean(config, "--apply")
    assert code == 3
    assert doc["problems"][0]["code"] == "catalog_diverged"


def test_status_all_tables_with_unreadable_foreign_and_recreated(
    catalog: CatalogHarness, config: Path, glue_env: None, location: Path
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1)")
    _sync(config)
    catalog.write("CREATE TABLE lake.main.foreign_t (id INTEGER)")
    catalog.write("CREATE TABLE lake.main.recreated (id INTEGER)")
    glue = boto3.client("glue", region_name=REGION)
    glue.create_table(
        DatabaseName="analytics",
        TableInput={"Name": "foreign_t", "Parameters": {"table_type": "HIVE"}},
    )

    # table recreated at the source: same name, different table_uuid
    code, doc = _invoke(config, "sync", "--table", "main.recreated")
    assert code == 0, doc["problems"]
    catalog.write("DROP TABLE lake.main.recreated")
    catalog.write("CREATE TABLE lake.main.recreated (id INTEGER)")

    _point_glue_to(str(location / "metadata" / "v404.metadata.json"))
    code, doc = _status(config, "--all-tables")
    by_table = {
        t["ducklake"]["table"]: t["status"] for t in doc["results"][0]["tables"]
    }
    assert by_table == {
        "foreign_t": "foreign",
        "recreated": "identity_mismatch",
        "t": "unreadable",
    }
    assert code == 1
    summary = doc["results"][0]["summary"]
    assert summary["errors"] == 1
    assert summary["exit_code"] == 1

    result = runner.invoke(app, ["status", "--all-tables", "--config", str(config)])
    assert result.exit_code == 1
    assert "Total: 3 tables" in result.stdout


def test_status_table_missing_everywhere(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    code, doc = _status(config, "--table", "main.nope")
    assert code == 1
    assert doc["problems"][0]["code"] == "table_not_found"
    assert doc["results"][0]["tables"] == []


def test_status_glue_table_without_ducklake_table(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1)")
    _sync(config)
    catalog.write("DROP TABLE lake.main.t")
    code, doc = _status(config, "--table", "main.t")
    table = _one(doc)
    assert code == 3
    assert table["status"] == "identity_mismatch"
    assert table["findings"][0]["detail"] == {"ducklake_table": None}


# -- clean ----------------------------------------------------------------------


def _orphans(location: Path) -> list[Path]:
    """Artifacts of a lost commit: metadata.json and manifest list."""
    meta = location / "metadata"
    orphan_meta = meta / "v9.metadata.json"
    orphan_meta.write_text(json.dumps({"format-version": 2}))
    orphan_list = meta / "snap-1-1-00000000-0000-0000-0000-000000000000.avro"
    orphan_list.write_bytes(b"Obj\x01")
    orphan_manifest = meta / "00000000-0000-0000-0000-000000000000-m0.avro"
    orphan_manifest.write_bytes(b"Obj\x01")
    for p in (orphan_meta, orphan_list, orphan_manifest):
        _age(p)
    return [orphan_meta, orphan_list, orphan_manifest]


def test_clean_dry_run_then_apply(
    catalog: CatalogHarness, config: Path, glue_env: None, location: Path
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t SELECT * FROM range(10)")
    _sync(config)
    catalog.write("DELETE FROM lake.main.t WHERE id < 2")
    _sync(config)
    before = sorted(p for p in location.rglob("*") if p.is_file())
    assert any(p.parent.name == "deletes" for p in before)

    code, doc = _clean(config)
    assert code == 4, doc  # healthy table: nothing to delete
    assert doc["results"][0]["deletable"] == 0

    recent = location / "metadata" / "v8.metadata.json"
    recent.write_text("{}")  # within the grace period
    future = location / "metadata" / "v7.metadata.json"
    future.write_text("{}")
    ts = time.time() + OLD
    os.utime(future, (ts, ts))
    orphans = _orphans(location)

    with mock.patch("ducklake_iceberg.s3.LocalStore.delete_objects") as delete:
        code, doc = _clean(config)
        delete.assert_not_called()  # dry run never deletes
    assert code == 0
    result = doc["results"][0]
    assert result["dry_run"] is True
    assert result["deletable"] == 3
    reasons = {Path(c["key"]).name: c["reason"] for c in result["candidates"]}
    assert reasons["v8.metadata.json"].startswith("within the grace period")
    assert "future" in reasons["v7.metadata.json"]
    assert reasons["_ducklake_iceberg_last_export.json"].startswith("unknown type")

    code, doc = _clean(config, "--apply", "--max-deletes", "2")
    assert code == 0, doc["problems"]
    assert doc["results"][0]["truncated"] is True
    assert len(doc["results"][0]["deleted"]) == 2
    code, doc = _clean(config, "--apply")
    assert code == 0
    assert not any(p.exists() for p in orphans)
    assert recent.exists()
    assert future.exists()
    after = sorted(p for p in location.rglob("*") if p.is_file())
    assert after == sorted([*before, recent, future])
    assert _glue_table().scan().to_arrow().num_rows == 8

    result = runner.invoke(app, ["clean", "--table", "main.t", "--config", str(config)])
    assert result.exit_code == 4
    assert "no eligible orphans" in result.stdout


def test_clean_ducklake_files_is_global_and_dry_run_by_default(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    boto3.client("glue", region_name=REGION).create_database(
        DatabaseInput={"Name": "analytics"}
    )

    code, doc = _invoke(config, "clean", "--ducklake-files")

    assert code == 4, doc["problems"]
    assert doc["dry_run"] is True
    assert doc["results"][0]["scope"] == "ducklake"
    assert doc["results"][0]["candidates"] == []


def test_clean_protects_pointer_target(
    catalog: CatalogHarness, config: Path, glue_env: None, location: Path
) -> None:
    """Export written and register pending: v2 is the base of the next export."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1)")
    _sync(config)
    catalog.write("INSERT INTO lake.main.t VALUES (2)")
    code, doc = _invoke(config, "export", "--table", "main.t")
    assert code == 0, doc["problems"]
    for p in location.rglob("*"):
        if p.is_file():
            _age(p)
    code, doc = _clean(config, "--apply")
    assert code == 4, doc
    assert (location / "metadata" / "v2.metadata.json").exists()
    _sync(config)  # the pending register is still possible
    assert _glue_table().scan().to_arrow().num_rows == 2


@pytest.mark.parametrize(
    ("grace", "message"),
    [("abc", "invalid duration format"), ("30m", "is below the floor")],
)
def test_clean_invalid_grace(
    catalog: CatalogHarness, config: Path, glue_env: None, grace: str, message: str
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    code, doc = _clean(config, "--grace", grace)
    assert code == 1
    assert message in doc["problems"][0]["message"]


def test_clean_aborts_on_parquet_missing_and_unreadable(
    catalog: CatalogHarness, config: Path, glue_env: None, location: Path
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1)")
    _sync(config)
    orphans = _orphans(location)

    # Data Parquet under the metadata prefix
    planted = location / "data" / "x.parquet"
    planted.parent.mkdir()
    planted.write_bytes(b"PAR1")
    code, doc = _clean(config, "--apply")
    assert code == 2
    assert ".parquet" in doc["problems"][0]["message"]
    planted.unlink()

    # unreadable reachable manifest list: abort, never treat it as an orphan
    manifest_list = next(
        p for p in (location / "metadata").glob("snap-*.avro") if p not in orphans
    )
    content = manifest_list.read_bytes()
    manifest_list.write_bytes(b"garbage")
    code, doc = _clean(config, "--apply")
    assert code == 1
    assert "unreadable" in doc["problems"][0]["message"]
    manifest_list.write_bytes(content)

    # reachable manifest deleted by hand: abort without deleting anything
    manifest = next(
        p for p in (location / "metadata").glob("*-m0.avro") if p not in orphans
    )
    manifest.unlink()
    code, doc = _clean(config, "--apply")
    assert code == 1
    assert "do not exist" in doc["problems"][0]["message"]
    assert all(p.exists() for p in orphans)


def test_clean_lock_held(catalog: CatalogHarness, config: Path, glue_env: None) -> None:
    from ducklake_iceberg.sync.lock import _lock_key, table_lock

    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1)")
    _sync(config)
    key = _lock_key(profile=None, glue_database="analytics", glue_table="t")
    with table_lock(key, metadata={"table": "main.t"}):
        code, doc = _clean(config, "--apply")
        assert code == 1
        assert doc["results"] == []
        code, _doc = _clean(config)  # a dry run does not take the lock
        assert code == 4


def test_clean_table_never_published(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    code, doc = _clean(config)
    assert code == 1
    assert "missing from Glue" in doc["problems"][0]["message"]
