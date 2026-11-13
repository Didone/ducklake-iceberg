"""Integration fixtures: a real DuckLake catalog, parametrized by backend.

The SAME suite must pass against both a SQLite and a Postgres catalog, with no
code changes. This is achieved with a single harness (`CatalogHarness`) exposed
by the `catalog` fixture, parametrized over both backends. The fast path uses
SQLite (no container); the slow one uses Postgres via `testcontainers` and is
selected by the `postgres` marker.

Postgres credentials never go into the DSN: they are passed through libpq
environment variables (`PGHOST`/`PGPORT`/`PGUSER`/`PGPASSWORD`/`PGDATABASE`),
which the DuckDB `postgres` extension consumes.
"""

from __future__ import annotations

import uuid
import warnings
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import pytest

from ducklake_iceberg.config import DuckLakeIcebergSettings

if TYPE_CHECKING:
    from pyiceberg.table import StaticTable

    from ducklake_iceberg.export import ExportResult


def _load_extensions(con: duckdb.DuckDBPyConnection, *extensions: str) -> None:
    """Load installed prerequisites; install only on a missing/unreadable load."""
    for extension in extensions:
        try:
            con.execute(f"LOAD {extension}")
        except duckdb.IOException:
            con.execute(f"INSTALL {extension}")
            con.execute(f"LOAD {extension}")


@dataclass(slots=True)
class CatalogHarness:
    """Uniform interface to a DuckLake catalog, independent of the backend."""

    backend: str
    dsn: str
    data_path: str
    #: path of the SQLite file, to forge states via direct SQL (sqlite only)
    sqlite_file: Path | None = field(default=None)

    def write(self, *statements: str) -> None:
        """Own one setup connection; each statement retains its own commit."""
        con = duckdb.connect(":memory:", config={"threads": 2})
        try:
            _load_extensions(con, "ducklake", self.backend)
            con.execute(
                f"ATTACH '{self.dsn}' AS lake "
                f"(METADATA_CATALOG 'lake_meta', DATA_PATH '{self.data_path}')"
            )
            for sql in statements:
                con.execute(sql)
        finally:
            con.close()

    def settings(self, **overrides: object) -> DuckLakeIcebergSettings:
        base: dict[str, object] = {
            "catalog_dsn": self.dsn,
            "data_path": self.data_path,
            "iceberg_metadata_prefix": "/local/iceberg",
            "glue_database": "analytics",
            "aws_region": "sa-east-1",
            "duckdb_threads": 2,
        }
        base.update(overrides)
        return DuckLakeIcebergSettings(**base)  # type: ignore[arg-type]

    def meta_exec(self, sql: str) -> None:
        """Run SQL directly against the metadata database, to forge states.

        On SQLite it uses the file; on Postgres it uses the DuckDB connection
        itself with the `postgres_execute` escape hatch, since there is no
        dedicated driver.
        """
        if self.backend == "sqlite":
            import sqlite3

            assert self.sqlite_file is not None
            con = sqlite3.connect(self.sqlite_file)
            try:
                con.execute(sql)
                con.commit()
            finally:
                con.close()
            return
        _postgres_meta_exec(sql)


class Lake:
    """Read/export helpers for the file-backed catalog prepared by lake."""

    def __init__(self, catalog: CatalogHarness, prefix: Path) -> None:
        self.catalog = catalog
        self.prefix = prefix
        self.settings: DuckLakeIcebergSettings = catalog.settings(
            iceberg_metadata_prefix=str(prefix)
        )

    def write(self, *statements: str) -> None:
        self.catalog.write(*statements)

    def rows(self, table: str = "main.t") -> list[tuple[Any, ...]]:
        con = duckdb.connect(":memory:", config={"threads": 2})
        try:
            for ext in ("ducklake", self.catalog.backend):
                con.execute(f"LOAD {ext}")
            con.execute(
                f"ATTACH '{self.catalog.dsn}' AS lake "
                f"(METADATA_CATALOG 'lake_meta', READ_ONLY)"
            )
            return sorted(con.execute(f"SELECT * FROM lake.{table}").fetchall())
        finally:
            con.close()

    def export(self, table: str = "t") -> ExportResult:
        from ducklake_iceberg.export import run_export

        return run_export(self.settings, "main", table)

    def iceberg(self, result: ExportResult) -> StaticTable:
        from pyiceberg.table import StaticTable

        return StaticTable.from_metadata("file://" + result.plan.metadata_location)

    def iceberg_rows(self, result: ExportResult) -> list[tuple[Any, ...]]:
        arrow = self.iceberg(result).scan().to_arrow()
        return sorted(tuple(r.values()) for r in arrow.to_pylist())

    def assert_mirrors(self, result: ExportResult, table: str = "main.t") -> None:
        assert self.iceberg_rows(result) == self.rows(table)


def sync_config(catalog: CatalogHarness, tmp_path: Path) -> Path:
    """Write CLI settings; source preparation belongs to the caller."""
    path = tmp_path / "ducklake-iceberg.yaml"
    path.write_text(
        f"catalog_dsn: '{catalog.dsn}'\n"
        f"data_path: '{catalog.data_path}'\n"
        f"iceberg_metadata_prefix: '{tmp_path / 'iceberg'}'\n"
        f"glue_database: analytics\n"
        f"aws_region: us-east-1\n"
        f"duckdb_threads: 2\n"
    )
    return path


def run_sync(config: Path, *extra: str) -> tuple[int, dict[str, Any]]:
    import json

    from typer.testing import CliRunner

    from ducklake_iceberg import timing
    from ducklake_iceberg.cli import app

    timing.reset()
    result = CliRunner().invoke(
        app, ["sync", "--table", "main.t", "--config", str(config), "--json", *extra]
    )
    return result.exit_code, json.loads(result.stdout)


def _postgres_meta_exec(sql: str) -> None:
    import os

    con = duckdb.connect(":memory:", config={"threads": 2})
    try:
        con.execute("INSTALL postgres")
        con.execute("LOAD postgres")
        # Credentials via libpq (PGHOST/PGUSER/PGPASSWORD/...); ATTACH TYPE
        # postgres connects to the same server as the metadata catalog.
        dbname = os.environ["PGDATABASE"]
        host = os.environ["PGHOST"]
        port = os.environ["PGPORT"]
        con.execute(
            f"ATTACH 'dbname={dbname} host={host} port={port}' AS pg (TYPE postgres)"
        )
        con.execute(f"CALL postgres_execute('pg', {_sql_literal(sql)})")
    finally:
        con.close()


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _make_sqlite(tmp_path: Path) -> CatalogHarness:
    catalog = tmp_path / "catalog.sqlite"
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    return CatalogHarness(
        backend="sqlite",
        dsn=f"ducklake:sqlite:{catalog}",
        data_path=f"{data_dir}/",
        sqlite_file=catalog,
    )


_CATALOG_BACKENDS = [
    "sqlite",
    pytest.param("postgres", marks=pytest.mark.postgres),
]


@contextmanager
def _catalog_case(
    request: pytest.FixtureRequest, tmp_path: Path, *statements: str
) -> Iterator[CatalogHarness]:
    """Own a fresh catalog; close explicit bootstrap SQL before the test."""
    manager: AbstractContextManager[CatalogHarness]
    if request.param == "sqlite":
        manager = nullcontext(_make_sqlite(tmp_path))
    else:
        # Share the service, never a database, attachment or data namespace.
        manager = postgres_catalog(
            request.getfixturevalue("postgres_service"), tmp_path
        )
    with manager as harness:
        harness.write("CREATE SCHEMA IF NOT EXISTS lake.vendas", *statements)
        yield harness


@pytest.fixture(params=_CATALOG_BACKENDS)
def catalog(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[CatalogHarness]:
    """Empty catalog with a vendas schema (first snapshot), on either backend."""
    with _catalog_case(request, tmp_path) as harness:
        yield harness


def _wait_until_postgres_accepts(dbname: str, host: str, port: str) -> None:
    """Block until the server answers queries steadily.

    The postgres image starts a temporary server for the init scripts, stops
    it and starts the real one: a container reported as started may still
    refuse connections (or answer the TLS probe with garbage) for a moment.
    Three consecutive successful queries mean the final server is up.
    """
    import time

    con = duckdb.connect(":memory:", config={"threads": 2})
    try:
        con.execute("INSTALL postgres")
        con.execute("LOAD postgres")
        streak, deadline = 0, time.monotonic() + 30
        while streak < 3:
            try:
                con.execute(
                    f"ATTACH 'dbname={dbname} host={host} port={port}' "
                    "AS probe (TYPE postgres, READ_ONLY)"
                )
                try:
                    con.execute("SELECT * FROM postgres_query('probe', 'SELECT 1')")
                finally:
                    con.execute("DETACH probe")
                streak += 1
            except duckdb.Error:
                streak = 0
                if time.monotonic() > deadline:
                    raise
            time.sleep(0.3)
    finally:
        con.close()


@pytest.fixture(scope="session")
def postgres_service() -> Iterator[Any]:
    """One owned server per pytest process; catalogs remain function-scoped."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        from testcontainers.postgres import PostgresContainer

    for attempt in range(3):
        container = PostgresContainer(
            "postgres:16-alpine",
            username="test",
            password=uuid.uuid4().hex,
            dbname="test",
        )
        try:
            container.start()
            with pytest.MonkeyPatch.context() as patch:
                for key, value in _postgres_environment(
                    container, container.dbname
                ).items():
                    patch.setenv(key, value)
                _wait_until_postgres_accepts(
                    container.dbname,
                    container.get_container_host_ip(),
                    str(container.get_exposed_port(5432)),
                )
        except BaseException as exc:
            container.stop()
            if not isinstance(exc, duckdb.Error) or attempt == 2:
                raise
        else:
            break
    try:
        yield container
    finally:
        container.stop()


def _postgres_environment(container: Any, database: str) -> dict[str, str]:
    return {
        "PGHOST": str(container.get_container_host_ip()),
        "PGPORT": str(container.get_exposed_port(5432)),
        "PGUSER": str(container.username),
        "PGPASSWORD": str(container.password),
        "PGDATABASE": database,
    }


def _postgres_admin(container: Any, sql: str) -> bytes:
    # Service lifecycle only. DuckLake metadata/data access still uses DuckDB.
    code, output = container.exec(
        [
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-tA",
            "-U",
            container.username,
            "-d",
            container.dbname,
            "-c",
            sql,
        ]
    )
    if code:
        raise RuntimeError("owned PostgreSQL test database administration failed")
    return bytes(output)


@contextmanager
def postgres_catalog(container: Any, tmp_path: Path) -> Iterator[CatalogHarness]:
    """Own a fresh database, environment and file namespace for one test."""
    database = "dli_" + uuid.uuid4().hex
    _postgres_admin(container, f'CREATE DATABASE "{database}"')
    try:
        with pytest.MonkeyPatch.context() as patch:
            env = _postgres_environment(container, database)
            for key, value in env.items():
                patch.setenv(key, value)
            data_dir = tmp_path / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            yield CatalogHarness(
                backend="postgres",
                dsn=(
                    f"ducklake:postgres:dbname={database} "
                    f"host={env['PGHOST']} port={env['PGPORT']}"
                ),
                data_path=f"{data_dir}/",
            )
    finally:
        states = (
            _postgres_admin(
                container,
                "SELECT state, count(*) FROM pg_stat_activity "
                f"WHERE datname = '{database}' "
                "AND backend_type = 'client backend' GROUP BY state",
            )
            .decode()
            .splitlines()
        )
        # Native error paths may retain an idle backend after DuckDB closes.
        # Terminate only this test's idle clients; active work still fails.
        suffix = " WITH (FORCE)" if states else ""
        _postgres_admin(container, f'DROP DATABASE "{database}"{suffix}')
        if any(line.split("|", 1)[0] != "idle" for line in states):
            raise RuntimeError("test ended with active PostgreSQL work")


_SEED_PEDIDOS = (
    "CREATE TABLE lake.main.pedidos (id INTEGER, uf VARCHAR)",
    "INSERT INTO lake.main.pedidos VALUES (1, 'MG'), (2, 'SP')",
)


@pytest.fixture(params=_CATALOG_BACKENDS)
def seeded_catalog(
    request: pytest.FixtureRequest, tmp_path: Path
) -> Iterator[CatalogHarness]:
    """Harness with a `main.pedidos` table holding 2 inlined rows."""
    with _catalog_case(request, tmp_path, *_SEED_PEDIDOS) as harness:
        yield harness


@pytest.fixture(params=_CATALOG_BACKENDS)
def flushed_catalog(
    request: pytest.FixtureRequest, tmp_path: Path
) -> Iterator[CatalogHarness]:
    """`seeded_catalog` with the flush applied: the preflight is clean (exit 0).

    The inlined data becomes a data file, so the `inlined-data` rule does not
    block and `doctor` exits 0 - a state suited to exercising the CLI plumbing
    (envelope, --json, flags) without the compatibility blocker.
    """
    with _catalog_case(
        request,
        tmp_path,
        *_SEED_PEDIDOS,
        "CALL ducklake_flush_inlined_data('lake', schema_name => 'main', "
        "table_name => 'pedidos')",
    ) as harness:
        yield harness


@pytest.fixture(params=_CATALOG_BACKENDS)
def lake(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Lake]:
    """File-backed source and Iceberg target, isolated for one case."""
    with _catalog_case(
        request, tmp_path, "CALL lake.set_option('data_inlining_row_limit', 0)"
    ) as harness:
        prefix = tmp_path / "iceberg"
        prefix.mkdir()
        yield Lake(harness, prefix)


@pytest.fixture
def aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fake credentials for moto, isolating the real environment."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "sa-east-1")
    monkeypatch.delenv("AWS_PROFILE", raising=False)


def cli_env_for(harness: CatalogHarness) -> dict[str, str]:
    """DUCKLAKE_ICEBERG_* variables for the CLI; data_path equals the catalog's."""
    return {
        "DUCKLAKE_ICEBERG_CATALOG_DSN": harness.dsn,
        "DUCKLAKE_ICEBERG_DATA_PATH": harness.data_path,
        "DUCKLAKE_ICEBERG_ICEBERG_METADATA_PREFIX": "/local/iceberg",
        "DUCKLAKE_ICEBERG_GLUE_DATABASE": "analytics",
        "DUCKLAKE_ICEBERG_DUCKDB_THREADS": "2",
        "DUCKLAKE_ICEBERG_AWS_REGION": "sa-east-1",
    }
