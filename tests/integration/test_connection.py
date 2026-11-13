"""Tests for the DuckDB connection layer.

The SAME suite runs against SQLite (fast) and Postgres (`-m postgres`), via the
parametrized `catalog` fixture. A few tests are inherently SQLite-specific
(file creation) and use `_make_sqlite`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest

from ducklake_iceberg.catalog.connection import (
    DuckLakeConnection,
    _backend_of,
    _escape,
    _quote,
    open_catalog,
)
from ducklake_iceberg.errors import AwsError, CatalogError
from tests.integration.conftest import (
    CatalogHarness,
    _make_sqlite,
    _postgres_admin,
    postgres_catalog,
)

pytestmark = pytest.mark.integration


# --- opening and diagnostics -------------------------------------------------
def test_open_exposes_both_databases(catalog: CatalogHarness) -> None:
    with open_catalog(catalog.settings()) as con:
        env = con.environment()
        assert "lake" in env.attached
        assert "lake_meta" in env.attached
        assert env.read_only is True
        assert env.extensions["ducklake"] is True
        # the current backend extension is loaded (registered name)
        registered = {"sqlite": "sqlite_scanner", "postgres": "postgres_scanner"}
        assert env.extensions[registered[catalog.backend]] is True
        assert env.duckdb_version.startswith("v2.")


# --- query_arrow returns a pyarrow.Table without warnings --------------------
def test_query_arrow_returns_table(catalog: CatalogHarness) -> None:
    with open_catalog(catalog.settings()) as con:
        table = con.query_arrow("SELECT count(*) AS n FROM lake_meta.ducklake_snapshot")
        assert isinstance(table, pa.Table)
        assert table.column_names == ["n"]
        assert table.to_pydict()["n"][0] >= 1


# --- positional and named parameters -----------------------------------------
def test_query_arrow_positional_params(catalog: CatalogHarness) -> None:
    with open_catalog(catalog.settings()) as con:
        table = con.query_arrow("SELECT ? + 1 AS v", [41])
        assert table.to_pydict()["v"] == [42]


def test_query_arrow_named_params(catalog: CatalogHarness) -> None:
    with open_catalog(catalog.settings()) as con:
        table = con.query_arrow("SELECT $x AS v", {"x": 7})
        assert table.to_pydict()["v"] == [7]


# --- a missing catalog is not created in READ_ONLY mode (SQLite) ------------
def test_missing_catalog_not_created(tmp_path: Path) -> None:
    missing = tmp_path / "missing.sqlite"
    harness = _make_sqlite(tmp_path)
    settings = harness.settings(catalog_dsn=f"ducklake:sqlite:{missing}")
    with pytest.raises(CatalogError) as exc:
        open_catalog(settings)
    assert "catalog unreachable" in exc.value.message
    assert not missing.exists()


# --- open_catalog emits READ_ONLY by default ---------------------------------
def test_default_open_is_read_only(catalog: CatalogHarness) -> None:
    con = DuckLakeConnection(catalog.settings())
    con.open()
    try:
        assert con._last_attach_sql is not None
        assert "READ_ONLY" in con._last_attach_sql
        assert "METADATA_CATALOG" in con._last_attach_sql
    finally:
        con.close()


def test_write_mode_omits_read_only(catalog: CatalogHarness) -> None:
    con = DuckLakeConnection(catalog.settings(), read_only=False)
    con.open()
    try:
        assert con._last_attach_sql is not None
        assert "READ_ONLY" not in con._last_attach_sql
        assert con.read_only is False
    finally:
        con.close()


# --- write on READ_ONLY ------------------------------------------------------
def test_write_on_read_only_fails(catalog: CatalogHarness) -> None:
    with open_catalog(catalog.settings()) as con:
        with pytest.raises(CatalogError) as exc:
            con.execute("CREATE TABLE lake.main.x (a INTEGER)")
        assert "READ_ONLY" in exc.value.message
        table = con.query_arrow("SELECT 1 AS ok")
        assert table.to_pydict()["ok"] == [1]


# --- read_transaction is not reentrant ---------------------------------------
def test_read_transaction_not_reentrant(catalog: CatalogHarness) -> None:
    def _nested(con: DuckLakeConnection) -> None:
        with con.read_transaction(), con.read_transaction():
            pass

    with open_catalog(catalog.settings()) as con:
        with pytest.raises(CatalogError) as exc:
            _nested(con)
        assert "reentrant" in exc.value.message


# --- an exception inside read_transaction rolls back -------------------------
def test_read_transaction_rolls_back(catalog: CatalogHarness) -> None:
    def _fail(con: DuckLakeConnection) -> None:
        with con.read_transaction():
            con.query_arrow("SELECT 1")
            raise ValueError("boom")

    with open_catalog(catalog.settings()) as con:
        with pytest.raises(ValueError, match="boom"):
            _fail(con)
        assert con._in_transaction is False
        assert con.query_arrow("SELECT 2 AS v").to_pydict()["v"] == [2]


def test_read_transaction_commits(catalog: CatalogHarness) -> None:
    with open_catalog(catalog.settings()) as con:
        with con.read_transaction() as c:
            table = c.query_arrow("SELECT count(*) AS n FROM lake_meta.ducklake_table")
        assert table.to_pydict()["n"][0] >= 0
        assert con._in_transaction is False


# --- a nonexistent AWS profile fails on open ---------------------------------
def test_missing_aws_profile_fails(catalog: CatalogHarness) -> None:
    settings = catalog.settings(
        data_path="s3://bucket/ducklake/data/",
        iceberg_metadata_prefix="s3://bucket/iceberg/",
        aws_profile="nonexistent_profile_ducklake_iceberg_xyz",
    )
    with pytest.raises(AwsError) as exc:
        open_catalog(settings)
    assert "AWS profile" in exc.value.message


# --- use after close ---------------------------------------------------------
def test_use_after_close(catalog: CatalogHarness) -> None:
    con = DuckLakeConnection(catalog.settings())
    con.open()
    con.close()
    with pytest.raises(CatalogError) as exc:
        con.query_arrow("SELECT 1")
    assert "closed" in exc.value.message


def test_close_is_idempotent(catalog: CatalogHarness) -> None:
    con = DuckLakeConnection(catalog.settings())
    con.open()
    con.close()
    con.close()  # must not raise


# --- diagnostics: S3 secret appears when there is an s3:// location ----------
def test_secret_created_for_s3(catalog: CatalogHarness) -> None:
    # Only skip when boto3 itself cannot resolve the environment profile; with
    # a valid credential (including an SSO profile) the secret must be
    # created - by boto3, without the AWS SDK C++ SSO provider.
    import os

    import boto3
    from botocore.exceptions import BotoCoreError

    profile = os.environ.get("AWS_PROFILE")
    try:
        credentials = (
            boto3.Session(profile_name=profile).get_credentials() if profile else None
        )
        if credentials is not None:
            credentials.get_frozen_credentials()  # forces the SSO token refresh
    except BotoCoreError:  # expired SSO token, unknown profile, ...
        credentials = None
    if credentials is None:
        pytest.skip("no AWS profile with credentials available to validate the secret")
    settings = catalog.settings(
        data_path="s3://bucket/ducklake/data/",
        iceberg_metadata_prefix="s3://bucket/iceberg/",
        aws_profile=profile,
    )
    with open_catalog(settings) as con:
        assert "ducklake_iceberg_s3" in con.environment().secrets


def test_secret_chain_includes_sso(
    catalog: CatalogHarness, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Without credentials in boto3, it falls back to the C++ chain, and
    ``CHAIN 'config'`` alone would reject SSO."""
    empty = tmp_path / "aws-empty"
    empty.write_text("")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(empty))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(empty))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    for name in (
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    settings = catalog.settings(
        data_path="s3://bucket/ducklake/data/",
        iceberg_metadata_prefix="s3://bucket/iceberg/",
    )
    con = DuckLakeConnection(settings)
    emitted: list[str] = []
    monkeypatch.setattr(con, "execute", lambda sql, params=None: emitted.append(sql))
    con._create_s3_secret()
    assert len(emitted) == 1
    assert "PROVIDER credential_chain" in emitted[0]
    assert "CHAIN 'config;sso'" in emitted[0]
    assert "PROFILE" not in emitted[0]


def test_no_secret_without_s3(catalog: CatalogHarness) -> None:
    with open_catalog(catalog.settings()) as con:
        assert "ducklake_iceberg_s3" not in con.environment().secrets


def test_installed_extensions_do_not_run_install(
    catalog: CatalogHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    con = DuckLakeConnection(catalog.settings(), s3=False)
    execute = con.execute

    def forbid_install(sql: str, params: object = None) -> None:
        assert not sql.startswith("INSTALL ")
        assert params is None
        execute(sql)

    monkeypatch.setattr(con, "execute", forbid_install)
    with con:
        assert con.environment().extensions["ducklake"]


# --- pure helpers (backend independent) --------------------------------------
@pytest.mark.parametrize(
    ("dsn", "expected"),
    [
        ("ducklake:postgres:dbname=lake host=pg", "postgres"),
        ("ducklake:sqlite:/tmp/c.sqlite", "sqlite"),
        ("ducklake:mysql:db=lake", "mysql"),
        ("ducklake:duckdb:/tmp/c.ducklake", "duckdb"),
        ("ducklake:/tmp/other", "duckdb"),
    ],
)
def test_backend_of(dsn: str, expected: str) -> None:
    assert _backend_of(dsn) == expected


def test_quote_and_escape() -> None:
    assert _quote("lake'; DROP") == "'lake''; DROP'"
    assert _escape("a'b") == "a''b"


def test_required_extensions_per_backend(tmp_path: Path) -> None:
    sqlite_con = DuckLakeConnection(_make_sqlite(tmp_path).settings())
    assert "sqlite" in sqlite_con._required_extensions()
    assert "aws" in sqlite_con._required_extensions()

    pg_settings = _make_sqlite(tmp_path).settings(
        catalog_dsn="ducklake:postgres:dbname=lake host=pg"
    )
    pg_con = DuckLakeConnection(pg_settings)
    assert "postgres" in pg_con._required_extensions()


@pytest.mark.parametrize("threads", [None, 1, 2])
def test_duckdb_thread_setting_preserves_native_default_and_explicit_limits(
    catalog: CatalogHarness, threads: int | None
) -> None:
    if threads is None:
        with duckdb.connect(":memory:") as native:
            expected = native.execute("SELECT current_setting('threads')").fetchone()
    else:
        expected = (threads,)
    settings = catalog.settings(duckdb_threads=threads)
    with open_catalog(settings, s3=False) as con:
        actual = con.raw.execute("SELECT current_setting('threads')").fetchone()
        assert actual is not None
        assert actual == expected


def test_batched_setup_keeps_statement_snapshots(catalog: CatalogHarness) -> None:
    with open_catalog(catalog.settings(), s3=False) as con:
        before = con.query_arrow(
            "SELECT max(snapshot_id) n FROM lake_meta.ducklake_snapshot"
        ).to_pylist()[0]["n"]
    catalog.write(
        "CREATE TABLE lake.main.batched(id BIGINT)",
        "INSERT INTO lake.main.batched VALUES (1)",
        "INSERT INTO lake.main.batched VALUES (2)",
    )
    with open_catalog(catalog.settings(), s3=False) as con:
        after = con.query_arrow(
            "SELECT max(snapshot_id) n FROM lake_meta.ducklake_snapshot"
        ).to_pylist()[0]["n"]
        assert after == before + 3
        assert con.query_arrow(
            "SELECT * FROM lake.main.batched ORDER BY id"
        ).to_pylist() == [
            {"id": 1},
            {"id": 2},
        ]


@pytest.mark.postgres
def test_shared_service_keeps_catalogs_isolated(
    postgres_service: Any, tmp_path: Path
) -> None:
    with postgres_catalog(postgres_service, tmp_path / "first") as first:
        first.write("CREATE TABLE lake.main.same_name(id BIGINT)")
        first.write("INSERT INTO lake.main.same_name VALUES (42)")
        first.write("CREATE SCHEMA lake.first_only")
        with open_catalog(first.settings(), s3=False) as con:
            first_uuid = con.query_arrow(
                "SELECT table_uuid FROM lake_meta.ducklake_table"
            ).to_pylist()[0]["table_uuid"]
    with postgres_catalog(postgres_service, tmp_path / "second") as second:
        second.write("CREATE SCHEMA IF NOT EXISTS lake.vendas")
        with open_catalog(second.settings(), s3=False) as con:
            assert (
                con.query_arrow("SELECT * FROM lake_meta.ducklake_table").num_rows == 0
            )
            assert "first_only" not in {
                row["schema_name"]
                for row in con.query_arrow(
                    "SELECT schema_name FROM lake_meta.ducklake_schema"
                ).to_pylist()
            }
        second.write("CREATE TABLE lake.main.same_name(id VARCHAR)")
        second.write("INSERT INTO lake.main.same_name VALUES ('fresh')")
        with open_catalog(second.settings(), s3=False) as con:
            assert con.query_arrow("SELECT * FROM lake.main.same_name").to_pylist() == [
                {"id": "fresh"}
            ]
            assert (
                con.query_arrow(
                    "SELECT table_uuid FROM lake_meta.ducklake_table"
                ).to_pylist()[0]["table_uuid"]
                != first_uuid
            )
    assert first.dsn != second.dsn
    assert first.data_path != second.data_path


@pytest.mark.postgres
def test_failed_catalog_setup_drops_database_and_restores_environment(
    postgres_service: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    monkeypatch.setenv("PGDATABASE", "caller_database")
    created: list[str] = []

    def fail_setup() -> None:
        with postgres_catalog(postgres_service, tmp_path) as harness:
            created.append(os.environ["PGDATABASE"])
            harness.write("CREATE TABLE lake.main.partial(id BIGINT)")
            raise RuntimeError("injected setup failure")

    with pytest.raises(RuntimeError, match="injected setup failure"):
        fail_setup()
    assert os.environ["PGDATABASE"] == "caller_database"
    databases = _postgres_admin(
        postgres_service, "SELECT datname FROM pg_database"
    ).splitlines()
    assert created[0].encode() not in databases


@pytest.mark.postgres
def test_catalog_cleanup_reports_active_work(
    postgres_service: Any, tmp_path: Path
) -> None:
    import os
    import time
    from threading import Thread

    workers: list[Thread] = []
    results: list[int] = []

    def leave_query_running() -> None:
        with postgres_catalog(postgres_service, tmp_path):
            database = os.environ["PGDATABASE"]

            def query() -> None:
                result = postgres_service.exec(
                    [
                        "psql",
                        "-U",
                        postgres_service.username,
                        "-d",
                        database,
                        "-c",
                        "SELECT pg_sleep(10)",
                    ]
                )
                results.append(result[0])

            worker = Thread(target=query)
            workers.append(worker)
            worker.start()
            for _ in range(100):
                active = _postgres_admin(
                    postgres_service,
                    "SELECT datname, state FROM pg_stat_activity WHERE state='active'",
                )
                if (database + "|active").encode() in active.splitlines():
                    break
                time.sleep(0.05)
            else:
                pytest.fail("owned query did not start")

    try:
        with pytest.raises(RuntimeError, match="active PostgreSQL work"):
            leave_query_running()
    finally:
        for worker in workers:
            worker.join(timeout=10)
    assert all(not worker.is_alive() for worker in workers)
    assert results
    assert results[0] != 0  # Cleanup terminated the owned active query.
