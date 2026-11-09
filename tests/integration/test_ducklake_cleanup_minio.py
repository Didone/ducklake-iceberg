"""Physical DuckLake cleanup against a real S3 (MinIO).

DuckLake with an ``s3://`` ``DATA_PATH`` on an ephemeral MinIO (testcontainers),
moto Glue and Iceberg metadata under a local prefix. moto lets S3 pass through
(``passthrough``) and ducklake-iceberg's boto3 reaches MinIO via
``AWS_ENDPOINT_URL_S3``;
ducklake-iceberg's DuckDB does not touch S3 in this flow (the plan comes from the
official dry-run and the export statistics come from the catalog).

It also pins the upstream bug behind the workaround: with the ``aws``
extension loaded, the native ``ducklake_cleanup_old_files`` aborts in
``S3FileSystem::RemoveFiles`` (httpfs 77392a7207). When the fixed build
(duckdb/duckdb-httpfs#507) is published, the canary test fails and says so;
since it pins extension versions, it also carries the ``pins`` marker.

``minio`` marker: requires Docker; without Docker the tests are skipped. The
CI fast path excludes them (``not minio``); they run in the job with Docker.
"""

from __future__ import annotations

import gc
import json
import os
import re
import uuid
import warnings
from collections.abc import Iterator
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from unittest import mock

import duckdb
import pytest
from typer.testing import CliRunner

from ducklake_iceberg.cli import app

pytestmark = [pytest.mark.integration, pytest.mark.aws, pytest.mark.minio]

runner = CliRunner()
REGION = "us-east-1"
KEY_ID = "minioadmin"
SECRET = "minioadmin"
#: MinIO stopped publishing images on Docker Hub; the pgsty fork keeps the same
#: CLI (``minio server``). Override with DUCKLAKE_ICEBERG_MINIO_IMAGE.
DEFAULT_IMAGE = "pgsty/minio:RELEASE.2026-08-04T00-00-00Z"


@dataclass(frozen=True, slots=True)
class MinIO:
    endpoint: str  # host:port, without scheme

    @property
    def url(self) -> str:
        return f"http://{self.endpoint}"


def _docker_available() -> bool:
    # without Docker the client leaves the socket open; the ResourceWarning is
    # emitted on GC, which is forced here so it does not become an error
    # (filterwarnings=error) at teardown
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ResourceWarning)
        try:
            from testcontainers.core.docker_client import DockerClient

            client = DockerClient().client
            try:
                client.ping()
            finally:
                client.close()
        except Exception:
            return False
        finally:
            gc.collect()
    return True


@pytest.fixture(scope="module")
def minio() -> Iterator[MinIO]:
    if not _docker_available():
        pytest.skip("Docker unavailable: MinIO tests skipped")
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.wait_strategies import LogMessageWaitStrategy

    container = (
        DockerContainer(os.environ.get("DUCKLAKE_ICEBERG_MINIO_IMAGE", DEFAULT_IMAGE))
        .with_env("MINIO_ROOT_USER", KEY_ID)
        .with_env("MINIO_ROOT_PASSWORD", SECRET)
        .with_command("server /data")
        .with_exposed_ports(9000)
        .waiting_for(LogMessageWaitStrategy(re.compile("API:")))
    )
    with container:
        host = container.get_container_host_ip()
        yield MinIO(f"{host}:{container.get_exposed_port(9000)}")


@dataclass(frozen=True, slots=True)
class Lake:
    minio: MinIO
    bucket: str
    dsn: str
    data_path: str  # s3://bucket/lake/
    config: Path

    def connect(self, *, aws: bool = False) -> duckdb.DuckDBPyConnection:
        """Write connection with a MinIO secret (explicit endpoint)."""
        con = duckdb.connect(
            ":memory:",
            config={
                "autoinstall_known_extensions": False,
                "autoload_known_extensions": False,
            },
        )
        for ext in ("ducklake", "sqlite", "httpfs", *(("aws",) if aws else ())):
            con.execute(f"INSTALL {ext}")
            con.execute(f"LOAD {ext}")
        con.execute(
            "CREATE SECRET minio (TYPE s3, PROVIDER config, KEY_ID $k, SECRET $s, "
            "REGION $r, ENDPOINT $e, URL_STYLE 'path', USE_SSL false)",
            {"k": KEY_ID, "s": SECRET, "r": REGION, "e": self.minio.endpoint},
        )
        con.execute(
            f"ATTACH '{self.dsn}' AS lake "
            f"(METADATA_CATALOG 'lake_meta', DATA_PATH '{self.data_path}')"
        )
        return con

    def write(self, *statements: str) -> None:
        con = self.connect()
        try:
            for sql in statements:
                con.execute(sql)
        finally:
            con.close()

    def scheduled(self) -> int:
        con = self.connect()
        try:
            row = con.execute(
                "SELECT count(*) FROM lake_meta.ducklake_files_scheduled_for_deletion"
            ).fetchone()
        finally:
            con.close()
        assert row is not None
        return int(row[0])

    def objects(self) -> set[str]:
        import boto3

        client = boto3.client(
            "s3",
            endpoint_url=self.minio.url,
            aws_access_key_id=KEY_ID,
            aws_secret_access_key=SECRET,
            region_name=REGION,
        )
        pages = client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket,
            Prefix=self.data_path.removeprefix(f"s3://{self.bucket}/"),
        )
        return {
            f"s3://{self.bucket}/{obj['Key']}"
            for page in pages
            for obj in page.get("Contents", [])
        }


@pytest.fixture
def lake(
    minio: MinIO, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Lake]:
    from moto import mock_aws
    from moto.core.config import DefaultConfig

    bucket = f"lake-{uuid.uuid4().hex[:12]}"
    # AWS_SECURITY_TOKEN is the legacy name of the session token, which botocore
    # still reads; tests/test_glue.py sets it at import time, and MinIO rejects
    # the fake token with InvalidTokenId.
    for name in (
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", KEY_ID)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", SECRET)
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    # ducklake-iceberg's boto3 (S3Store) goes to MinIO; Glue stays on moto
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", minio.url)
    monkeypatch.setenv("DUCKLAKE_ICEBERG_STATE_DIR", str(tmp_path / "state"))

    config = tmp_path / "ducklake-iceberg.yaml"
    dsn = f"ducklake:sqlite:{tmp_path / 'catalog.sqlite'}"
    data_path = f"s3://{bucket}/lake/"
    config.write_text(
        f"catalog_dsn: '{dsn}'\n"
        f"data_path: '{data_path}'\n"
        f"iceberg_metadata_prefix: '{tmp_path / 'iceberg'}'\n"
        "glue_database: analytics\n"
        f"aws_region: {REGION}\n",
        encoding="utf-8",
    )
    # real MinIO credentials (not moto's fake ones) and unmocked S3
    moto_config: DefaultConfig = {
        "core": {
            "mock_credentials": False,
            "passthrough": {"urls": [re.escape(minio.url) + ".*"], "services": []},
        }
    }
    with (
        mock_aws(config=moto_config),
        mock.patch("ducklake_iceberg.glue._validate_metadata_location"),
    ):
        import boto3

        boto3.client(
            "s3",
            endpoint_url=minio.url,
            aws_access_key_id=KEY_ID,
            aws_secret_access_key=SECRET,
            region_name=REGION,
        ).create_bucket(Bucket=bucket)
        boto3.client("glue", region_name=REGION).create_database(
            DatabaseInput={"Name": "analytics"}
        )
        yield Lake(minio, bucket, dsn, data_path, config)


def _invoke(lake: Lake, *args: str) -> tuple[int, dict[str, Any]]:
    result = runner.invoke(app, [*args, "--config", str(lake.config), "--json"])
    return result.exit_code, json.loads(result.stdout)


def _two_files(lake: Lake) -> None:
    lake.write(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.t (id INTEGER)",
    )
    lake.write("INSERT INTO lake.main.t SELECT * FROM range(1000)")
    lake.write("INSERT INTO lake.main.t SELECT * FROM range(1000, 2000)")


def _compact_and_age(lake: Lake) -> None:
    """Merge schedules the 2 files; backdate the schedule to outlive --grace."""
    lake.write(
        "CALL ducklake_merge_adjacent_files('lake', 't', schema => 'main')",
        "UPDATE lake_meta.ducklake_files_scheduled_for_deletion "
        "SET schedule_start = now() - INTERVAL 2 DAY",
    )


def test_clean_ducklake_files_deletes_from_minio_and_catalog_queue(
    lake: Lake,
) -> None:
    _two_files(lake)
    _compact_and_age(lake)
    objects_before = lake.objects()
    assert len(objects_before) == 3
    assert lake.scheduled() == 2

    code, doc = _invoke(lake, "clean", "--ducklake-files")

    assert code == 0, doc["problems"]
    result = doc["results"][0]
    assert result["dry_run"] is True
    assert len(result["candidates"]) == 2
    assert set(result["candidates"]) < objects_before
    assert result["deleted"] == []
    assert lake.objects() == objects_before
    assert lake.scheduled() == 2

    code, doc = _invoke(lake, "clean", "--ducklake-files", "--apply")

    assert code == 0, (doc["problems"], doc["results"][0].get("failed"))
    result = doc["results"][0]
    assert result["dry_run"] is False
    assert sorted(result["deleted"]) == sorted(result["candidates"])
    assert result["failed"] == []
    assert lake.objects() == objects_before - set(result["deleted"])
    assert len(lake.objects()) == 1
    assert lake.scheduled() == 0
    con = lake.connect()
    try:
        row = con.execute("SELECT count(*), sum(id) FROM lake.main.t").fetchone()
    finally:
        con.close()
    assert row == (2000, sum(range(2000)))

    code, doc = _invoke(lake, "clean", "--ducklake-files", "--apply")
    assert code == 4, doc["problems"]


def test_clean_ducklake_files_protects_files_of_retained_iceberg_snapshots(
    lake: Lake,
) -> None:
    _two_files(lake)
    code, doc = _invoke(lake, "sync", "--table", "main.t")
    assert code == 0, doc["problems"]
    _compact_and_age(lake)
    objects_before = lake.objects()

    for extra in ((), ("--apply",)):
        code, doc = _invoke(lake, "clean", "--ducklake-files", *extra)

        assert code == 2, doc["problems"]
        result = doc["results"][0]
        assert sorted(result["referenced"]) == sorted(result["candidates"])
        assert len(result["candidates"]) == 2
        assert result["deleted"] == []
        assert doc["problems"][0]["code"] == "cleanup_blocked"
        assert lake.objects() == objects_before
        assert lake.scheduled() == 2


@pytest.mark.pins
def test_native_cleanup_hits_httpfs_negative_secret_score(lake: Lake) -> None:
    """Canary for the upstream bug: fails once the fixed httpfs is released."""
    _two_files(lake)
    _compact_and_age(lake)

    con = lake.connect(aws=True)
    try:
        with pytest.raises(duckdb.Error, match=r"value -1000000000000 outside"):
            con.execute("CALL ducklake_cleanup_old_files('lake', cleanup_all => true)")
    finally:
        con.close()
    assert len(lake.objects()) == 3
    assert lake.scheduled() == 2

    # without the aws_default storage in the lookup, the same CALL works. A
    # different lake: the INTERNAL Error invalidates the previous database
    # without releasing the SQLite lock.
    lake = replace(
        lake,
        dsn=lake.dsn.removesuffix(".sqlite") + "-ok.sqlite",
        data_path=f"s3://{lake.bucket}/lake-ok/",
    )
    _two_files(lake)
    _compact_and_age(lake)
    con = lake.connect(aws=True)
    try:
        con.execute("SET aws_default_credentials = false")
        removed = con.execute(
            "CALL ducklake_cleanup_old_files('lake', cleanup_all => true)"
        ).fetchall()
    finally:
        con.close()
    assert len(removed) == 2
    assert len(lake.objects()) == 1
    assert lake.scheduled() == 0
