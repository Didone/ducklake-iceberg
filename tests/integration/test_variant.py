"""Integration tests for the nested VARIANT block.

The SAME suite runs against SQLite and Postgres. A checkpoint of a table with
a nested VARIANT must exit with code 2 without ever opening the write
connection.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from typer.testing import CliRunner

from ducklake_iceberg.catalog.connection import open_catalog
from ducklake_iceberg.catalog.reader import (
    current_snapshot,
    parse_table_ref,
    resolve_table,
)
from ducklake_iceberg.checkpoint.command import run_checkpoint
from ducklake_iceberg.checkpoint.model import NestedVariantError, TableRef
from ducklake_iceberg.checkpoint.variant import (
    find_nested_variants,
    find_variant_columns,
)
from ducklake_iceberg.cli import app
from ducklake_iceberg.errors import ExitCode
from tests.integration.conftest import CatalogHarness, cli_env_for

pytestmark = [pytest.mark.integration]

runner = CliRunner()


def _variants(h: CatalogHarness, table: str) -> tuple[Any, ...]:
    with open_catalog(h.settings(), read_only=True) as con, con.read_transaction():
        snap = current_snapshot(con)
        ident = resolve_table(con, parse_table_ref(table), snap)
        ref = TableRef(ident.ref.schema_name, ident.ref.table_name, ident.table_id)
        return find_variant_columns(con.raw, con.meta, ref, snap.snapshot_id)


def _error(h: CatalogHarness, table: str) -> Any:
    report = run_checkpoint(h.settings(), table=table)
    return report, report.results[0].error


# --- STRUCT(VARIANT) with inlined rows -> exit 2, no write ------------------
def test_nested_variant_in_struct_blocks(
    catalog: CatalogHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog.write(
        "CREATE TABLE lake.main.t (id INTEGER, payload STRUCT(attrs VARIANT))"
    )
    catalog.write("INSERT INTO lake.main.t VALUES (1, {'attrs': NULL})")

    opened_write: list[bool] = []
    from ducklake_iceberg.catalog.connection import open_catalog as real

    def spy(cfg: Any, *, read_only: bool = True, s3: bool = True) -> Any:
        opened_write.append(read_only)
        return real(cfg, read_only=read_only, s3=s3)

    monkeypatch.setattr("ducklake_iceberg.checkpoint.command.open_catalog", spy)
    report, error = _error(catalog, "main.t")
    assert report.exit_code == ExitCode.BLOCKED
    assert isinstance(error, NestedVariantError)
    assert error.detected == "pre_check"
    assert [c.path for c in error.columns] == ["payload.attrs"]
    # the write connection (read_only=False) is never opened
    assert False not in opened_write


# --- --dry-run also blocks --------------------------------------------------
def test_nested_variant_blocks_dry_run(catalog: CatalogHarness) -> None:
    catalog.write(
        "CREATE TABLE lake.main.t (id INTEGER, payload STRUCT(attrs VARIANT))"
    )
    catalog.write("INSERT INTO lake.main.t VALUES (1, {'attrs': NULL})")
    result = runner.invoke(
        app,
        ["checkpoint", "--table", "main.t", "--dry-run", "--json"],
        env=cli_env_for(catalog),
    )
    assert result.exit_code == 2, result.stdout
    payload = json.loads(result.stdout)
    assert payload["dry_run"] is True
    assert payload["results"][0]["wrote"] is False
    assert payload["results"][0]["tables"][0]["blocked_by"] == ["E-VARIANT-NESTED"]
    assert payload["results"][0]["tables"][0]["would_flush"] is False


# --- LIST(STRUCT(VARIANT)) -> path tags[].v, depth 2 ------------------------
def test_nested_variant_in_list_of_struct(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, tags STRUCT(v VARIANT)[])")
    cols = find_nested_variants(_variants(catalog, "main.t"))
    assert len(cols) == 1
    assert cols[0].path == "tags[].v"
    assert cols[0].depth == 2
    assert cols[0].parent_type == "struct"


# --- MAP(VARCHAR, VARIANT) -> blocks, child column_id -----------------------
def test_nested_variant_in_map(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, m MAP(VARCHAR, VARIANT))")
    _report, error = _error(catalog, "main.t")
    assert isinstance(error, NestedVariantError)
    assert error.columns[0].path == "m.value"
    assert error.columns[0].column_id > 0


# --- top-level VARIANT only -> checkpoint proceeds ---------------------------
def test_top_level_variant_does_not_block(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, top VARIANT)")
    catalog.write("INSERT INTO lake.main.t VALUES (1, NULL)")
    report = run_checkpoint(catalog.settings(), table="main.t")
    result = report.results[0]
    assert result.error is None
    assert result.rows_flushed == 1
    assert report.exit_code == ExitCode.SUCCESS
    # but there is a top-level variant (the export will be blocked by the doctor)
    assert len(_variants(catalog, "main.t")) == 1


# --- nested variant only in an old schema version ---------------------------
def test_nested_variant_only_in_old_schema_version(catalog: CatalogHarness) -> None:
    catalog.write(
        "CREATE TABLE lake.main.t (id INTEGER, payload STRUCT(attrs VARIANT))"
    )
    catalog.write("ALTER TABLE lake.main.t DROP COLUMN payload")
    catalog.write("INSERT INTO lake.main.t VALUES (1)")
    # the current snapshot no longer has a nested variant
    assert find_nested_variants(_variants(catalog, "main.t")) == ()
    report = run_checkpoint(catalog.settings(), table="main.t")
    assert report.results[0].error is None


# --- three nested variant columns in one message, sorted by path ------------
def test_three_nested_variants_sorted_by_path(catalog: CatalogHarness) -> None:
    catalog.write(
        "CREATE TABLE lake.main.t ("
        "  id INTEGER,"
        "  a STRUCT(x VARIANT),"
        "  b STRUCT(y VARIANT),"
        "  c MAP(VARCHAR, VARIANT))"
    )
    _report, error = _error(catalog, "main.t")
    assert isinstance(error, NestedVariantError)
    paths = [c.path for c in error.columns]
    assert paths == sorted(paths)
    assert set(paths) == {"a.x", "b.y", "c.value"}


# --- table without nested columns pays no cost and is not blocked -----------
def test_plain_table_has_no_variants(seeded_catalog: CatalogHarness) -> None:
    assert _variants(seeded_catalog, "main.pedidos") == ()


# --- --all-tables: good table is processed, table with variant is blocked ---
def test_all_tables_isolates_nested_variant(catalog: CatalogHarness) -> None:
    catalog.write("CREATE TABLE lake.main.ok (id INTEGER)")
    catalog.write("INSERT INTO lake.main.ok VALUES (1), (2)")
    catalog.write("CREATE TABLE lake.main.bad (id INTEGER, p STRUCT(a VARIANT))")
    catalog.write("INSERT INTO lake.main.bad VALUES (1, {'a': NULL})")
    result = runner.invoke(
        app, ["checkpoint", "--all-tables", "--json"], env=cli_env_for(catalog)
    )
    assert result.exit_code == 2, result.stdout
    tables = {
        t["table_name"]: t for t in json.loads(result.stdout)["results"][0]["tables"]
    }
    assert tables["bad"]["error"]["code"] == "E-VARIANT-NESTED"
    assert tables["ok"]["error"] is None
    assert tables["ok"]["rows_flushed"] == 2  # the good table was processed


# --- rich message names the path and column_id ------------------------------
def test_rich_message_names_column(catalog: CatalogHarness) -> None:
    catalog.write(
        "CREATE TABLE lake.main.t (id INTEGER, payload STRUCT(attrs VARIANT))"
    )
    catalog.write("INSERT INTO lake.main.t VALUES (1, {'attrs': NULL})")
    env = {**cli_env_for(catalog), "COLUMNS": "200"}
    result = runner.invoke(app, ["checkpoint", "--table", "main.t"], env=env)
    assert result.exit_code == 2
    assert "nested VARIANT" in result.stdout
    assert "payload.attrs" in result.stdout
    assert "column_id" in result.stdout
    assert "nothing was written to the catalog" in result.stdout
