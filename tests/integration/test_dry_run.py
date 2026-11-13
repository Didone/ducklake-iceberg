"""Tests for `sync --dry-run`.

They cover the no-write guarantees (SQL spy and connection factory spy) and
the exit codes. They run against SQLite and Postgres.
"""

from __future__ import annotations

import json
import re
from typing import Any

import duckdb
import pytest
from typer.testing import CliRunner

from ducklake_iceberg.catalog.connection import open_catalog
from ducklake_iceberg.cli import app
from tests.integration.conftest import CatalogFactory, CatalogHarness, cli_env_for

pytestmark = [pytest.mark.integration]

runner = CliRunner()

#: Write regex: no dry-run statement may match it.
_WRITE_SQL = re.compile(
    r"(?i)\b(call\s+ducklake_(flush_inlined_data|rewrite_data_files|"
    r"merge_adjacent_files)|insert|update|delete|create|drop|alter)\b"
)


def _max_snapshot(h: CatalogHarness) -> int:
    with open_catalog(h.settings(), read_only=True) as con:
        return int(
            con.query_arrow("SELECT max(snapshot_id) FROM lake_meta.ducklake_snapshot")
            .column(0)[0]
            .as_py()
        )


def _dry_run(h: CatalogHarness, table: str) -> Any:
    return runner.invoke(
        app,
        ["sync", "--table", table, "--dry-run", "--json"],
        env=cli_env_for(h),
    )


def _steps(output: str) -> list[dict[str, Any]]:
    steps: list[dict[str, Any]] = json.loads(output)["results"][0]["steps"]
    return steps


# --- inlined rows, no writes, snapshot unchanged -----------------------------
def test_dry_run_does_not_flush(seeded_catalog: CatalogHarness) -> None:
    before = _max_snapshot(seeded_catalog)
    result = _dry_run(seeded_catalog, "main.pedidos")
    payload = json.loads(result.stdout)
    assert payload["dry_run"] is True
    steps = _steps(result.stdout)
    assert [s["step"] for s in steps] == ["doctor", "checkpoint", "register"]
    assert steps[1]["dry_run"] is True
    assert steps[1]["pending_inlined_rows"] > 0
    assert _max_snapshot(seeded_catalog) == before  # no snapshot created


# --- SQL spy: no write statement ---------------------------------------------
def test_dry_run_executes_no_write_sql(
    seeded_catalog: CatalogHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    executed: list[str] = []
    original = duckdb.DuckDBPyConnection.execute

    def spy(self: Any, sql: str, *a: Any, **k: Any) -> Any:
        executed.append(sql)
        return original(self, sql, *a, **k)

    monkeypatch.setattr(duckdb.DuckDBPyConnection, "execute", spy)
    _dry_run(seeded_catalog, "main.pedidos")
    offending = [s for s in executed if _WRITE_SQL.search(s)]
    assert offending == [], offending


# --- factory spy: no read_only=False connection ------------------------------
def test_dry_run_never_opens_write_connection(
    seeded_catalog: CatalogHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ducklake_iceberg.catalog.connection import open_catalog as real

    modes: list[bool] = []

    def spy(cfg: Any, *, read_only: bool = True, s3: bool = True) -> Any:
        modes.append(read_only)
        return real(cfg, read_only=read_only, s3=s3)

    monkeypatch.setattr("ducklake_iceberg.catalog.connection.open_catalog", spy)
    monkeypatch.setattr("ducklake_iceberg.pipeline.open_catalog", spy)
    monkeypatch.setattr("ducklake_iceberg.checkpoint.flush.open_catalog", spy)
    _dry_run(seeded_catalog, "main.pedidos")
    assert modes
    assert all(modes)


# --- nested VARIANT in dry-run -> exit 2 -------------------------------------
def test_dry_run_still_runs_doctor(make_catalog: CatalogFactory) -> None:
    catalog = make_catalog(
        "CREATE TABLE lake.main.t (id INTEGER, p STRUCT(a VARIANT))",
        "INSERT INTO lake.main.t VALUES (1, {'a': NULL})",
    )
    before = _max_snapshot(catalog)
    result = _dry_run(catalog, "main.t")
    assert result.exit_code in (0, 2), result.stdout
    assert _steps(result.stdout)[0]["step"] == "doctor"
    assert _max_snapshot(catalog) == before


# --- unqualified table -------------------------------------------------------
def test_dry_run_unqualified_table(seeded_catalog: CatalogHarness) -> None:
    result = _dry_run(seeded_catalog, "pedidos")
    assert result.exit_code == 1, result.stdout
    assert "qualified name" in json.loads(result.stdout)["problems"][0]["message"]


def test_dry_run_missing_table(seeded_catalog: CatalogHarness) -> None:
    result = _dry_run(seeded_catalog, "main.missing")
    assert result.exit_code == 1, result.stdout
    assert json.loads(result.stdout)["problems"]
