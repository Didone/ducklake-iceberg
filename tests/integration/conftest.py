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

import warnings
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import pytest

from iceduck.config import IceduckSettings

SettingsFactory = Callable[..., IceduckSettings]
Writer = Callable[[str], None]


@dataclass(slots=True)
class CatalogHarness:
    """Uniform interface to a DuckLake catalog, independent of the backend."""

    backend: str
    dsn: str
    data_path: str
    #: path of the SQLite file, to forge states via direct SQL (sqlite only)
    sqlite_file: Path | None = field(default=None)

    def write(self, sql: str) -> None:
        """Open a fresh write connection and run `sql` (creates a snapshot)."""
        con = duckdb.connect(":memory:")
        try:
            for ext in ("ducklake", self.backend):
                con.execute(f"INSTALL {ext}")
                con.execute(f"LOAD {ext}")
            con.execute(
                f"ATTACH '{self.dsn}' AS lake "
                f"(METADATA_CATALOG 'lake_meta', DATA_PATH '{self.data_path}')"
            )
            con.execute(sql)
        finally:
            con.close()

    def settings(self, **overrides: object) -> IceduckSettings:
        base: dict[str, object] = {
            "catalog_dsn": self.dsn,
            "data_path": self.data_path,
            "iceberg_metadata_prefix": "/local/iceberg",
            "glue_database": "analytics",
            "aws_region": "sa-east-1",
        }
        base.update(overrides)
        return IceduckSettings(**base)  # type: ignore[arg-type]

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


def _postgres_meta_exec(sql: str) -> None:
    import os

    con = duckdb.connect(":memory:")
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


@pytest.fixture(
    params=[
        "sqlite",
        pytest.param("postgres", marks=pytest.mark.postgres),
    ]
)
def catalog(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[CatalogHarness]:
    """Empty catalog harness, parametrized by backend (sqlite/postgres).

    Seeds a `vendas` schema (first snapshot) and yields the harness.
    """
    backend = request.param
    if backend == "sqlite":
        harness = _make_sqlite(tmp_path)
        harness.write("CREATE SCHEMA IF NOT EXISTS lake.vendas")
        yield harness
        return

    # backend == "postgres": ephemeral container via testcontainers
    harness, stop = _start_postgres(tmp_path)
    try:
        harness.write("CREATE SCHEMA IF NOT EXISTS lake.vendas")
        yield harness
    finally:
        stop()


def _start_postgres(tmp_path: Path) -> tuple[CatalogHarness, Callable[[], None]]:
    import os

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        from testcontainers.postgres import PostgresContainer

    container = PostgresContainer("postgres:16-alpine")
    container.start()

    host = container.get_container_host_ip()
    port = container.get_exposed_port(5432)
    user = container.username
    password = container.password
    dbname = container.dbname

    # Credentials via libpq (never in the DSN): the DuckDB postgres extension
    # reads them.
    os.environ.update(
        PGHOST=host,
        PGPORT=str(port),
        PGUSER=user,
        PGPASSWORD=password,
        PGDATABASE=dbname,
    )
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    harness = CatalogHarness(
        backend="postgres",
        dsn=f"ducklake:postgres:dbname={dbname} host={host} port={port}",
        data_path=f"{data_dir}/",
    )

    def stop() -> None:
        for key in ("PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE"):
            os.environ.pop(key, None)
        container.stop()

    return harness, stop


@pytest.fixture
def seeded_catalog(catalog: CatalogHarness) -> CatalogHarness:
    """Harness with a `main.pedidos` table holding 2 inlined rows."""
    catalog.write("CREATE TABLE lake.main.pedidos (id INTEGER, uf VARCHAR)")
    catalog.write("INSERT INTO lake.main.pedidos VALUES (1, 'MG'), (2, 'SP')")
    return catalog


@pytest.fixture
def flushed_catalog(seeded_catalog: CatalogHarness) -> CatalogHarness:
    """`seeded_catalog` with the flush applied: the preflight is clean (exit 0).

    The inlined data becomes a data file, so the `inlined-data` rule does not
    block and `doctor` exits 0 - a state suited to exercising the CLI plumbing
    (envelope, --json, flags) without the compatibility blocker.
    """
    seeded_catalog.write(
        "CALL ducklake_flush_inlined_data('lake', schema_name => 'main', "
        "table_name => 'pedidos')"
    )
    return seeded_catalog


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
    """ICEDUCK_* variables for the CLI, with data_path equal to the catalog's."""
    return {
        "ICEDUCK_CATALOG_DSN": harness.dsn,
        "ICEDUCK_DATA_PATH": harness.data_path,
        "ICEDUCK_ICEBERG_METADATA_PREFIX": "/local/iceberg",
        "ICEDUCK_GLUE_DATABASE": "analytics",
        "ICEDUCK_AWS_REGION": "sa-east-1",
    }
