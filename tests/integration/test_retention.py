"""``ducklake-iceberg validate --only retention`` end to end.

Real DuckLake (SQLite on the fast path, Postgres with ``-m postgres``) ->
``ducklake-iceberg sync`` -> moto Glue, with the Iceberg metadata under a local prefix
(PyArrow S3 is not intercepted by moto). States DuckLake does not produce on
its own within seconds (old snapshot, old schedule) are forged with
``meta_exec`` directly on the metadata database.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import boto3
from moto import mock_aws
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField
from typer.testing import CliRunner

from ducklake_iceberg import aws
from ducklake_iceberg.catalog.connection import open_catalog
from ducklake_iceberg.cli import app
from ducklake_iceberg.retention import (
    IcebergFiles,
    RetentionPolicy,
    RetentionSource,
    simulate_maintenance,
)

from .conftest import CatalogHarness

pytestmark = [pytest.mark.integration, pytest.mark.aws]

runner = CliRunner()
REGION = "us-east-1"
DECLARED = (
    "\nsnapshot_retention_hours: 24\n"
    "retention:\n"
    "  expire_window: 30d\n"
    "  cleanup_window: 7d\n"
    "  sync_interval: 1h\n"
    "  sync_p95: 4m\n"
)


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


def _with(config: Path, block: str) -> Path:
    path = config.with_name("retention.yaml")
    path.write_text(config.read_text(encoding="utf-8") + block, encoding="utf-8")
    return path


def _invoke(config: Path, *args: str) -> tuple[int, dict[str, Any]]:
    result = runner.invoke(app, [*args, "--config", str(config), "--json"])
    return result.exit_code, json.loads(result.stdout)


def _sync(config: Path) -> None:
    code, doc = _invoke(config, "sync", "--table", "main.t")
    assert code == 0, doc["problems"]


def _retention(config: Path, *extra: str) -> tuple[int, dict[str, Any]]:
    code, doc = _invoke(
        config, "validate", "--table", "main.t", "--only", "retention", *extra
    )
    assert not doc["problems"] or code == 1, doc["problems"]
    return code, doc


def _result(doc: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = doc["results"][0]
    return result


def _published_snapshot() -> int:
    table = GlueCatalog("t", **{"glue.region": REGION}).load_table(("analytics", "t"))
    return int(table.properties["ducklake.snapshot-id"])


def _parquet(catalog: CatalogHarness) -> list[Path]:
    return sorted((Path(catalog.data_path) / "main" / "t").glob("*.parquet"))


def _ts(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S+00")


def _table_with_two_files(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1), (2)")
    catalog.write("INSERT INTO lake.main.t VALUES (3)")


# -- happy path and catalog without expiration --------------------------------------


def test_declared_policy_ok_prints_readable_margin(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Margin in readable units, plus the recommendation."""
    _table_with_two_files(catalog)
    declared = _with(config, DECLARED)
    _sync(declared)

    code, doc = _retention(declared)

    assert code == 0
    result = _result(doc)
    assert result["risk"]["severity"] == "ok"
    assert result["messages"][0]["message"].startswith(
        "retention ok: margin 36d22h (R+C=37d"
    )
    assert result["recommendation"].startswith("expire_snapshots older_than >= ")
    assert "(current 30d; sync every 1h, p95 4min" in result["recommendation"]
    # read-only simulation; no new DuckLake snapshot
    assert result["iceberg"]["retained_snapshots"] == 1

    text = runner.invoke(
        app,
        [
            "validate",
            "--table",
            "main.t",
            "--only",
            "retention",
            "--config",
            str(declared),
        ],
    )
    assert text.exit_code == 0, text.stdout
    assert "retention ok: margin 36d22h (R+C=37d" in text.stdout
    assert "recommendation: expire_snapshots" in text.stdout


def test_fresh_catalog_unknown_policy_warns(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """No expiration and no config: unknown, warning, exit 0 (3 with --strict)."""
    _table_with_two_files(catalog)
    _sync(config)

    code, doc = _retention(config)

    assert code == 0
    result = _result(doc)
    assert result["policy"]["source"] == "inferred"
    assert result["policy"]["confidence"] == "unknown"
    assert result["policy"]["evidence"]["expiration_observed"] is False
    assert result["risk"]["severity"] == "warning"
    assert "unknown retention policy" in result["warnings"]
    assert result["doomed_snapshots"] == []  # R unknown: expire is not simulated

    strict, _ = _retention(config, "--strict")
    assert strict == 3


def test_catalog_options_are_scanned_and_used(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Catalog ``expire_older_than``/``delete_older_than`` options are used."""
    _table_with_two_files(catalog)
    catalog.write("CALL lake.set_option('expire_older_than', '30 days')")
    catalog.write("CALL lake.set_option('delete_older_than', '1 week')")
    _sync(config)

    code, doc = _retention(config)

    assert code == 0
    policy = _result(doc)["policy"]
    assert policy["source"] == "catalog"
    assert policy["confidence"] == "declared"
    assert policy["expire_window_seconds"] == 30 * 86400
    assert policy["cleanup_window_seconds"] == 7 * 86400
    names = {option["option_name"] for option in policy["catalog_options"]}
    assert {"expire_older_than", "delete_older_than"} <= names


# -- impossible policy --------------------------------------------------------------


def test_impossible_policy_exits_3(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    _table_with_two_files(catalog)
    impossible = _with(
        config, "\nretention:\n  expire_window: 1h\n  sync_interval: 24h\n"
    )
    _sync(impossible)

    code, doc = _retention(impossible)

    assert code == 3
    result = _result(doc)
    assert result["risk"]["severity"] == "critical"
    assert any(
        m["message"].startswith("impossible policy: R+C=1h <= I+D=1d")
        for m in result["messages"]
    )


# -- published snapshot expired or about to expire ----------------------------------


def test_published_snapshot_expired_exits_3(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """A real ``ducklake_expire_snapshots`` removes S_pub."""
    _table_with_two_files(catalog)
    declared = _with(config, DECLARED)
    _sync(declared)
    published = _published_snapshot()
    catalog.write("INSERT INTO lake.main.t VALUES (4)")
    catalog.write(f"CALL ducklake_expire_snapshots('lake', versions => [{published}])")

    code, doc = _retention(declared)

    assert code == 3
    result = _result(doc)
    assert result["published_snapshot_present"] is False
    assert result["risk"]["severity"] == "critical"
    assert result["messages"][0]["message"] == (
        f"published snapshot {published} no longer exists in DuckLake"
    )


def test_published_snapshot_in_expire_dry_run_exits_3(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """The expiration dry run (R=30d) includes S_pub, forged to be 40 days old."""
    _table_with_two_files(catalog)
    declared = _with(config, DECLARED)
    _sync(declared)
    published = _published_snapshot()
    catalog.write("INSERT INTO lake.main.t VALUES (4)")
    old = _ts(datetime.now(UTC) - timedelta(days=40))
    catalog.meta_exec(
        f"UPDATE ducklake_snapshot SET snapshot_time = '{old}' "
        f"WHERE snapshot_id <= {published}"
    )

    code, doc = _retention(declared)

    assert code == 3, doc["problems"]
    result = _result(doc)
    assert published in result["doomed_snapshots"]
    assert result["published_snapshot_present"] is True
    assert result["risk"]["published_age_seconds"] >= 40 * 86400 - 60
    messages = [m["message"] for m in result["messages"]]
    assert f"published snapshot {published} expires in the next maintenance" in messages


# -- removal queue, relative versus absolute paths ----------------------------------


def test_referenced_file_scheduled_for_deletion_exits_3(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Compaction schedules files that the current Iceberg snapshot references.

    The queue stores paths relative to ``data_path``; the manifest stores
    absolute ones. The intersection only works after normalization.
    """
    _table_with_two_files(catalog)
    declared = _with(config, DECLARED)
    _sync(declared)
    referenced = {str(path) for path in _parquet(catalog)}
    catalog.write("CALL ducklake_merge_adjacent_files('lake')")

    code, doc = _retention(declared)

    assert code == 3
    result = _result(doc)
    doomed = result["doomed_files"]
    assert {item["path"] for item in doomed} == referenced
    assert {item["reason"] for item in doomed} == {"scheduled"}
    assert {item["referenced_by"] for item in doomed} == {"current"}
    assert all(item["scheduled_at"] for item in doomed)
    assert "2 referenced files scheduled for removal" in [
        m["message"] for m in result["messages"]
    ]


def test_history_files_scheduled_warn_then_block_when_eligible(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Retained Iceberg snapshots reference compacted files.

    Scheduled recently (within C) is a warning; eligible in the next cleanup
    (scheduled more than C ago) breaks time travel and gives exit 3.
    """
    _table_with_two_files(catalog)
    declared = _with(config, DECLARED)
    _sync(declared)
    old_files = {str(path) for path in _parquet(catalog)}
    catalog.write("CALL ducklake_merge_adjacent_files('lake')")
    _sync(declared)

    code, doc = _retention(declared)

    assert code == 0
    result = _result(doc)
    assert result["iceberg"]["retained_snapshots"] == 2
    assert result["iceberg"]["history_only_files"] == 2
    history = [f for f in result["doomed_files"] if f["referenced_by"] == "history"]
    assert {f["path"] for f in history} == old_files
    assert {f["reason"] for f in history} == {"scheduled"}
    assert any("retained Iceberg snapshots" in w for w in result["warnings"])
    strict, _ = _retention(declared, "--strict")
    assert strict == 3

    old = _ts(datetime.now(UTC) - timedelta(days=8))
    catalog.meta_exec(
        f"UPDATE ducklake_files_scheduled_for_deletion SET schedule_start = '{old}'"
    )
    code, doc = _retention(declared)

    assert code == 3
    result = _result(doc)
    history = [f for f in result["doomed_files"] if f["referenced_by"] == "history"]
    assert {f["reason"] for f in history} == {"cleanup"}
    assert result["risk"]["severity"] == "critical"


# -- referenced file missing --------------------------------------------------------


def test_missing_referenced_file_with_full_check_exits_3(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    _table_with_two_files(catalog)
    declared = _with(config, DECLARED)
    _sync(declared)
    victim = _parquet(catalog)[0]
    victim.unlink()

    code, doc = _retention(declared, "--check-s3", "full")

    assert code == 3
    result = _result(doc)
    assert result["s3_missing"] == [str(victim)]
    assert result["s3_checked"] == 2
    assert {f["reason"] for f in result["doomed_files"]} == {"missing_s3"}
    assert f"{victim}: missing" in [m["message"] for m in result["messages"]]


# -- history with expiration --------------------------------------------------------


def test_expired_history_is_observed(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Expired snapshot_ids 0..4 -> confidence observed."""
    _table_with_two_files(catalog)
    for value in range(4, 8):
        catalog.write(f"INSERT INTO lake.main.t VALUES ({value})")
    _sync(config)
    assert _published_snapshot() > 4
    catalog.write("CALL ducklake_expire_snapshots('lake', versions => [0, 1, 2, 3, 4])")

    _, doc = _retention(config)

    policy = _result(doc)["policy"]
    assert policy["source"] == "inferred"
    assert policy["confidence"] == "observed"
    evidence = policy["evidence"]
    assert evidence["oldest_retained_snapshot"] == 5
    assert evidence["expiration_observed"] is True
    # the whole fixture runs in seconds: the observed window is of that order
    assert 0 <= evidence["observed_retention_seconds"] < 600
    assert policy["expire_window_seconds"] == evidence["observed_retention_seconds"]


def test_declared_longer_than_observed_warns_and_uses_declared(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Config declares 7d, history shows 3d."""
    _table_with_two_files(catalog)
    declared = _with(
        config, "\nretention:\n  expire_window: 7d\n  cleanup_window: 1d\n"
    )
    _sync(declared)
    catalog.write("CALL ducklake_expire_snapshots('lake', versions => [0])")
    three_days = _ts(datetime.now(UTC) - timedelta(days=3))
    catalog.meta_exec(
        f"UPDATE ducklake_snapshot SET snapshot_time = '{three_days}' "
        "WHERE snapshot_id = 1"
    )

    code, doc = _retention(declared)

    assert code == 0
    result = _result(doc)
    assert result["policy"]["expire_window_seconds"] == 7 * 86400
    assert result["policy"]["evidence"]["observed_retention_seconds"] >= 3 * 86400
    assert any(
        w.startswith(
            "declared retention 7d differs from the one observed in the history 3d"
        )
        for w in result["warnings"]
    )
    strict, _ = _retention(declared, "--strict")
    assert strict == 3


# -- orphan outside data_path -------------------------------------------------------


def test_orphan_outside_data_path_is_not_reported(
    catalog: CatalogHarness, tmp_path: Path
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    inside = Path(catalog.data_path) / "main" / "orphan.parquet"
    inside.parent.mkdir(parents=True, exist_ok=True)
    inside.write_bytes(b"x")
    outside = tmp_path / "iceberg" / "analytics" / "t" / "deletes" / "d.parquet"
    outside.parent.mkdir(parents=True)
    outside.write_bytes(b"x")
    policy = RetentionPolicy(
        timedelta(days=7), timedelta(), RetentionSource.CONFIG, "declared"
    )

    with open_catalog(catalog.settings(), read_only=True, s3=False) as con:
        _, removable = simulate_maintenance(con, policy=policy)

    assert str(inside) in removable
    assert str(outside) not in removable


# -- Iceberg table without a snapshot -----------------------------------------------


def test_iceberg_table_without_snapshot_exits_4(
    catalog: CatalogHarness, config: Path, glue_env: None, tmp_path: Path
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    boto3.client("glue", region_name=REGION).create_database(
        DatabaseInput={"Name": "analytics"}
    )
    GlueCatalog("t", **{"glue.region": REGION}).create_table(
        ("analytics", "t"),
        Schema(NestedField(1, "id", IntegerType(), required=False)),
        location=str(tmp_path / "iceberg" / "analytics" / "t"),
    )

    code, doc = _retention(config)

    assert code == 4
    result = _result(doc)
    assert result["policy"] is None
    assert result["warning"] == "nothing to check: table has no published snapshot"

    text = runner.invoke(
        app,
        [
            "validate",
            "--table",
            "main.t",
            "--only",
            "retention",
            "--config",
            str(config),
        ],
    )
    assert text.exit_code == 4
    assert "nothing to check: table has no published snapshot" in text.stdout


def test_empty_table_has_nothing_to_protect(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Iceberg snapshot without files (empty table) -> exit 4, no error."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    _invoke(config, "sync", "--table", "main.t")

    code, doc = _retention(config)

    assert code == 4, doc["problems"]
    result = _result(doc)
    assert result["referenced_files"] == 0
    assert any("no referenced files" in w for w in result["warnings"])


# -- 403 on head_object -------------------------------------------------------------


def test_access_denied_on_head_object_exits_1(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    _table_with_two_files(catalog)
    declared = _with(config, DECLARED)
    _sync(declared)
    s3 = aws.client("s3", REGION, None)
    s3.create_bucket(Bucket="lake")
    s3.put_object(Bucket="lake", Key="data/f.parquet", Body=b"x")

    def deny(**_: Any) -> tuple[Any, dict[str, Any]]:
        return mock.Mock(status_code=403, headers={}), {
            "Error": {"Code": "403", "Message": "Forbidden"},
            "ResponseMetadata": {"HTTPStatusCode": 403},
        }

    s3.meta.events.register("before-call.s3.HeadObject", deny)
    files = IcebergFiles(
        current={"s3://lake/data/f.parquet"},
        retained={"s3://lake/data/f.parquet"},
        retained_snapshots=1,
        oldest_history_ms=None,
        expired_evidence=False,
    )
    with mock.patch("ducklake_iceberg.retention.iceberg_files", return_value=files):
        code, doc = _retention(declared, "--check-s3", "full")

    assert code == 1
    problem = doc["problems"][0]
    assert problem["message"] == "AccessDenied while checking s3://lake/data/f.parquet"
    assert "IAM" in (problem["hint"] or "")
