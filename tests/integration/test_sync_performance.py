"""Performance: column statistics, concurrent ``--all-tables`` and limits.

Covers the scenarios that need a real DuckLake catalog (single-scan
collection, deterministic sampling) and the CLI ones (``sync --all-tables``,
``--fail-fast``, ``--glue-stats``, ``workers_capped``, metrics). Glue is moto,
which does not implement ``UpdateColumnStatisticsForTable``: the call is
replaced on the process client by a recorder.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from botocore.exceptions import ClientError
from moto import mock_aws
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
)
from typer.testing import CliRunner

from ducklake_iceberg import aws, concurrency, glue_stats
from ducklake_iceberg.catalog.connection import open_catalog
from ducklake_iceberg.catalog.reader import current_snapshot
from ducklake_iceberg.cli import app
from ducklake_iceberg.concurrency import Interrupted, Outcome
from ducklake_iceberg.errors import ExitCode
from ducklake_iceberg.glue_stats import (
    collect_column_statistics,
    plan_column_statistics,
)
from ducklake_iceberg.output import Envelope, Problem

from .conftest import CatalogHarness

pytestmark = [pytest.mark.integration]

runner = CliRunner()
REGION = "us-east-1"

_SCHEMA = Schema(
    NestedField(1, "id", IntegerType(), required=False),
    NestedField(2, "nome", StringType(), required=False),
    NestedField(3, "ativo", BooleanType(), required=False),
    NestedField(4, "em", TimestampType(), required=False),
)


def _seed(catalog: CatalogHarness, table: str = "t") -> None:
    catalog.write(
        f"CREATE TABLE lake.main.{table} "
        "(id INTEGER, nome VARCHAR, ativo BOOLEAN, em TIMESTAMP)"
    )
    catalog.write(
        f"INSERT INTO lake.main.{table} VALUES "
        "(1, '日本', true, TIMESTAMP '2024-01-01'), "
        "(2, 'abc', false, NULL), "
        "(NULL, NULL, true, NULL), "
        "(4, 'ü', NULL, NULL)"
    )


# -- collection ------------------------------------------------------------


def _collect(
    catalog: CatalogHarness, *, snapshot: int | None = None, **kwargs: Any
) -> list[glue_stats.ColumnStat]:
    with open_catalog(catalog.settings(), read_only=True, s3=False) as con:
        version = (
            snapshot if snapshot is not None else current_snapshot(con).snapshot_id
        )
        return collect_column_statistics(
            con,
            table_ref="lake.main.t",
            snapshot_version=version,
            plans=plan_column_statistics(_SCHEMA, ndv="approx"),
            **kwargs,
        )


def test_collect_exact_nulls_and_byte_lengths(catalog: CatalogHarness) -> None:
    """Exact null counts and ``MaximumLength`` in bytes."""
    _seed(catalog)

    stats = {stat.column_name: stat for stat in _collect(catalog)}

    assert set(stats) == {"id", "nome", "ativo"}  # timestamp skipped
    assert stats["id"].number_of_nulls == 1
    assert stats["nome"].number_of_nulls == 1
    assert stats["ativo"].number_of_nulls == 1
    # strlen('日本') = 6 bytes (length = 2 characters)
    assert stats["nome"].maximum_length == len("日本".encode())
    expected_avg = sum(len(v.encode()) for v in ("日本", "abc", "ü")) / 3
    assert stats["nome"].average_length == pytest.approx(expected_avg)
    assert (stats["ativo"].number_of_trues, stats["ativo"].number_of_falses) == (2, 1)
    assert (stats["id"].minimum, stats["id"].maximum) == (1, 4)
    assert stats["id"].rows_scanned == 4


def test_collect_reads_the_given_snapshot(catalog: CatalogHarness) -> None:
    """Reads ``AT (VERSION => S_pub)``, not the current snapshot."""
    _seed(catalog)
    with open_catalog(catalog.settings(), read_only=True, s3=False) as con:
        published = current_snapshot(con).snapshot_id
    catalog.write("INSERT INTO lake.main.t VALUES (99, NULL, NULL, NULL)")

    stats = {s.column_name: s for s in _collect(catalog, snapshot=published)}

    assert stats["id"].maximum == 4
    assert stats["id"].rows_scanned == 4


def test_collect_sample_is_deterministic(catalog: CatalogHarness) -> None:
    """``bernoulli, 42`` returns the same sample both times."""
    catalog.write(
        "CREATE TABLE lake.main.t "
        "(id INTEGER, nome VARCHAR, ativo BOOLEAN, em TIMESTAMP)"
    )
    catalog.write(
        "INSERT INTO lake.main.t SELECT i, 'n' || (i % 50), i % 2 = 0, NULL "
        "FROM range(4000) r(i)"
    )

    first = _collect(catalog, sample_percent=5)
    second = _collect(catalog, sample_percent=5)

    assert first == second
    assert 0 < first[0].rows_scanned < 4000


# -- CLI: harness ----------------------------------------------------------


@pytest.fixture
def glue_env() -> Iterator[None]:
    with mock_aws(), mock.patch("ducklake_iceberg.glue._validate_metadata_location"):
        yield


def _config(catalog: CatalogHarness, tmp_path: Path, **extra: str) -> Path:
    catalog.write("CALL lake.set_option('data_inlining_row_limit', 0)")
    path = tmp_path / "ducklake-iceberg.yaml"
    lines = [
        f"catalog_dsn: '{catalog.dsn}'",
        f"data_path: '{catalog.data_path}'",
        f"iceberg_metadata_prefix: '{tmp_path / 'iceberg'}'",
        "glue_database: analytics",
        f"aws_region: {REGION}",
        *(f"{key}: {value}" for key, value in extra.items()),
    ]
    path.write_text("\n".join(lines) + "\n")
    return path


def _sync(config: Path, *args: str) -> tuple[int, dict[str, Any]]:
    result = runner.invoke(app, ["sync", "--config", str(config), "--json", *args])
    return result.exit_code, json.loads(result.stdout)


def _step(doc: dict[str, Any], name: str, index: int = 0) -> dict[str, Any]:
    return next(s for s in doc["results"][index]["steps"] if s["step"] == name)


def _phase(doc: dict[str, Any], name: str) -> dict[str, Any]:
    return next(p for p in doc["results"][0]["metrics"]["phases"] if p["phase"] == name)


@pytest.fixture
def stats_calls(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Record ``UpdateColumnStatisticsForTable`` calls (missing in moto)."""
    calls: list[dict[str, Any]] = []
    real_client = aws.client

    def client(service: str, region: str, profile: str | None) -> Any:
        found = real_client(service, region, profile)
        if service == "glue":

            def update(**kwargs: Any) -> dict[str, Any]:
                calls.append(kwargs)
                return {"Errors": []}

            found.update_column_statistics_for_table = update
        return found

    monkeypatch.setattr(aws, "client", client)
    return calls


# -- CLI: --glue-stats ------------------------------------------------------


@pytest.mark.aws
def test_sync_glue_stats_publishes_with_scan_start_time(
    catalog: CatalogHarness,
    tmp_path: Path,
    glue_env: None,
    stats_calls: list[dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Batch publishing; types without statistics are skipped with a reason;
    ``AnalyzedTime`` = scan start; metrics carry items/bytes/calls."""
    config = _config(catalog, tmp_path)
    _seed(catalog)
    scan_entered: list[datetime] = []
    real_collect = glue_stats.collect_column_statistics

    def spy(*args: Any, **kwargs: Any) -> list[glue_stats.ColumnStat]:
        scan_entered.append(datetime.now(UTC))
        return real_collect(*args, **kwargs)

    monkeypatch.setattr(glue_stats, "collect_column_statistics", spy)

    code, doc = _sync(config, "--table", "main.t", "--glue-stats", "on")

    assert code == 0, doc["problems"]
    step = _step(doc, "glue_stats")
    assert step["columns_published"] == 3
    assert step["batches"] == step["api_calls"] == 1
    assert ["em", "glue_type_unsupported"] in step["columns_skipped"]
    assert step["ndv_estimated"] is True
    (call,) = stats_calls
    published = {item["ColumnName"]: item for item in call["ColumnStatisticsList"]}
    assert set(published) == {"id", "nome", "ativo"}
    analyzed = {item["AnalyzedTime"] for item in published.values()}
    assert len(analyzed) == 1
    assert analyzed.pop() <= scan_entered[0]
    nome = published["nome"]["StatisticsData"]["StringColumnStatisticsData"]
    assert nome["MaximumLength"] == 6
    assert nome["NumberOfNulls"] == 1
    phase = _phase(doc, "glue_stats")
    assert phase["items"] == 3
    assert phase["calls"] == 1
    assert phase["bytes"] == doc["results"][0]["bytes_referenced"] > 0


@pytest.mark.aws
def test_sync_glue_stats_keeps_exit_4_on_unchanged_table(
    catalog: CatalogHarness,
    tmp_path: Path,
    glue_env: None,
    stats_calls: list[dict[str, Any]],
) -> None:
    """Statistics are not a "change": a table with no delta keeps exit 4."""
    config = _config(catalog, tmp_path)
    _seed(catalog)
    assert _sync(config, "--table", "main.t")[0] == 0

    code, doc = _sync(config, "--table", "main.t", "--glue-stats", "on")

    assert code == 4, doc["problems"]
    assert _step(doc, "glue_stats")["columns_published"] == 3
    assert len(stats_calls) == 1


@pytest.mark.aws
def test_sync_glue_stats_ndv_off_sample_and_dry_run(
    catalog: CatalogHarness,
    tmp_path: Path,
    glue_env: None,
    stats_calls: list[dict[str, Any]],
) -> None:
    """NDV off, ``sampled_percent`` and ``--dry-run``."""
    config = _config(catalog, tmp_path)
    _seed(catalog)
    assert _sync(config, "--table", "main.t")[0] == 0

    code, doc = _sync(
        config,
        "--table",
        "main.t",
        "--glue-stats",
        "on",
        "--glue-stats-ndv",
        "off",
        "--glue-stats-sample",
        "5",
    )
    assert code == 4, doc["problems"]
    step = _step(doc, "glue_stats")
    assert step["sampled_percent"] == 5.0
    assert ["id", "ndv_disabled"] in step["columns_skipped"]
    assert ["nome", "ndv_disabled"] in step["columns_skipped"]
    (call,) = stats_calls
    assert [i["ColumnName"] for i in call["ColumnStatisticsList"]] == ["ativo"]

    code, doc = _sync(config, "--table", "main.t", "--glue-stats", "on", "--dry-run")
    assert code == 4, doc["problems"]
    assert _step(doc, "glue_stats")["outcome"] == "dry_run"
    assert len(stats_calls) == 1  # dry run collects but does not publish


@pytest.mark.aws
def test_sync_glue_stats_column_error_is_warning_total_failure_is_error(
    catalog: CatalogHarness,
    tmp_path: Path,
    glue_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Error on one column => exit 0 with a ``problem``; failure of every
    batch => exit 1."""
    config = _config(catalog, tmp_path)
    _seed(catalog)
    assert _sync(config, "--table", "main.t")[0] == 0
    catalog.write("INSERT INTO lake.main.t VALUES (5, 'x', true, NULL)")
    mode = {"fail": False}
    real_client = aws.client

    def update(**kwargs: Any) -> dict[str, Any]:
        if mode["fail"]:
            raise ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
                "UpdateColumnStatisticsForTable",
            )
        first = kwargs["ColumnStatisticsList"][0]
        return {
            "Errors": [
                {
                    "ColumnStatistics": first,
                    "Error": {
                        "ErrorCode": "InvalidInputException",
                        "ErrorMessage": "x",
                    },
                }
            ]
        }

    def client(service: str, region: str, profile: str | None) -> Any:
        found = real_client(service, region, profile)
        if service == "glue":
            found.update_column_statistics_for_table = update
        return found

    monkeypatch.setattr(aws, "client", client)

    code, doc = _sync(config, "--table", "main.t", "--glue-stats", "on")
    assert code == 0, doc["problems"]
    (problem,) = doc["problems"]
    assert problem["code"] == "glue_column_stats"
    assert problem["level"] == "warning"
    assert _step(doc, "glue_stats")["columns_published"] == 2

    mode["fail"] = True
    code, doc = _sync(config, "--table", "main.t", "--glue-stats", "on")
    assert code == 1
    assert "AccessDenied" in doc["problems"][0]["message"]


# -- CLI: concurrent --all-tables ------------------------------------------


@pytest.mark.aws
def test_sync_all_tables_concurrent_sorted_and_human_report(
    catalog: CatalogHarness, tmp_path: Path, glue_env: None
) -> None:
    config = _config(catalog, tmp_path)
    for name in ("t_c", "t_a", "t_b"):
        catalog.write(f"CREATE TABLE lake.main.{name} (id INTEGER)")
        catalog.write(f"INSERT INTO lake.main.{name} VALUES (1), (2)")

    code, doc = _sync(config, "--all-tables", "--max-table-workers", "3")

    assert code == 0, doc["problems"]
    assert [r["table"] for r in doc["results"]] == ["main.t_a", "main.t_b", "main.t_c"]
    assert all(r["exit_code"] == 0 and r["files_added"] == 1 for r in doc["results"])
    assert doc["config"]["duckdb_threads"] >= 1

    catalog.write("INSERT INTO lake.main.t_b VALUES (3)")
    human = runner.invoke(app, ["sync", "--config", str(config), "--all-tables"])
    assert human.exit_code == 0
    lines = human.stdout.strip().splitlines()
    assert lines[-1].startswith("3 tables · 1 ok · 2 nothing to do · total ")
    assert lines[-1].endswith(" · exit 0")
    rows = [line for line in lines if "main.t_" in line]
    assert [row.split()[1] for row in rows] == ["main.t_a", "main.t_b", "main.t_c"]
    assert "+1" in rows[1]


def _fake_sync_table(
    codes: dict[str, ExitCode], seen: list[str], probe: Callable[[Any], None]
) -> Callable[..., tuple[ExitCode, Envelope]]:
    def fake(
        settings: Any, schema_name: str, table_name: str, **_: Any
    ) -> tuple[ExitCode, Envelope]:
        qualified = f"{schema_name}.{table_name}"
        seen.append(qualified)
        probe(settings)
        code = codes.get(qualified, ExitCode.SUCCESS)
        envelope = Envelope(command="sync", dry_run=False)
        envelope.results.append(
            {"table": qualified, "steps": [], "exit_code": int(code), "duration_ms": 1}
        )
        if code is ExitCode.ERROR:
            envelope.problems.append(Problem("x", "failed", table=qualified))
        return code, envelope

    return fake


def _three_tables(catalog: CatalogHarness) -> None:
    for name in ("a", "b", "c"):
        catalog.write(f"CREATE TABLE lake.main.{name} (id INTEGER)")


def test_sync_all_tables_partial_failure_does_not_stop_others(
    catalog: CatalogHarness, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _three_tables(catalog)
    seen: list[str] = []
    monkeypatch.setattr(
        "ducklake_iceberg.cli._sync_table",
        _fake_sync_table({"main.a": ExitCode.ERROR}, seen, lambda _: None),
    )

    code, doc = _sync(
        _config(catalog, tmp_path), "--all-tables", "--max-table-workers", "1"
    )

    assert code == 1
    assert sorted(seen) == ["main.a", "main.b", "main.c"]
    assert [r["exit_code"] for r in doc["results"]] == [1, 0, 0]


def test_sync_all_tables_fail_fast_cancels_not_started(
    catalog: CatalogHarness, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Via the CLI: the 1st table fails, the ones not started are cancelled."""
    _three_tables(catalog)
    seen: list[str] = []

    def probe(_: Any) -> None:
        # the worker may pick up main.b before the cancellation; holding it
        # guarantees main.c is still queued when the failure is collected
        if seen[-1] == "main.b":
            time.sleep(0.1)

    monkeypatch.setattr(
        "ducklake_iceberg.cli._sync_table",
        _fake_sync_table({"main.a": ExitCode.ERROR}, seen, probe),
    )

    code, doc = _sync(
        _config(catalog, tmp_path),
        "--all-tables",
        "--max-table-workers",
        "1",
        "--fail-fast",
    )

    assert code == 1
    assert seen[0] == "main.a"
    results = {r["table"]: r for r in doc["results"]}
    assert results["main.a"]["exit_code"] == 1
    assert results["main.c"]["error"] == "cancelled"
    assert results["main.c"]["exit_code"] == 1
    assert "main.c" not in seen
    codes = [p["code"] for p in doc["problems"]]
    assert "cancelled" in codes


def test_sync_workers_capped_is_warning_and_limits_reach_pools(
    catalog: CatalogHarness, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Via the CLI: flags/env > YAML, global ceiling, PyIceberg."""
    _three_tables(catalog)
    probes: list[tuple[int, int, str | None]] = []

    def probe(settings: Any) -> None:
        limits = concurrency.active_limits()
        probes.append(
            (
                settings.max_footer_workers,
                limits.max_io_workers,
                os.environ.get(concurrency.PYICEBERG_MAX_WORKERS_ENV),
            )
        )

    monkeypatch.setattr(
        "ducklake_iceberg.cli._sync_table", _fake_sync_table({}, [], probe)
    )
    monkeypatch.setenv("DUCKLAKE_ICEBERG_MAX_TOTAL_WORKERS", "16")
    config = _config(catalog, tmp_path, max_io_workers="3")

    code, doc = _sync(
        config,
        "--all-tables",
        "--max-table-workers",
        "4",
        "--max-footer-workers",
        "8",
    )

    assert code == 0, doc["problems"]
    (problem,) = doc["problems"]
    assert problem["code"] == "workers_capped"
    assert problem["level"] == "warning"
    assert "max_footer_workers reduced from 8 to 4" in problem["message"]
    assert doc["config"]["max_footer_workers"] == 4
    assert set(probes) == {(4, 3, "8")}


def test_sync_interrupted_reports_problem_and_exit_1(
    catalog: CatalogHarness, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SIGINT => exit 1 with a ``problem`` ``code="interrupted"``."""
    _three_tables(catalog)
    done = Envelope(command="sync", dry_run=False)
    done.results.append({"table": "main.a", "steps": [], "exit_code": 0})

    def interrupted(fn: Any, items: Any, **_: Any) -> Any:
        targets = list(items)
        raise Interrupted(
            [
                Outcome(targets[0], (ExitCode.SUCCESS, done), 0),
                *(Outcome(t, None, 1, concurrency.INTERRUPTED) for t in targets[1:]),
            ]
        )

    monkeypatch.setattr("ducklake_iceberg.cli.map_parallel", interrupted)

    code, doc = _sync(_config(catalog, tmp_path), "--all-tables")

    assert code == 1
    assert doc["status"] == "error"
    problem = next(p for p in doc["problems"] if p["code"] == "interrupted")
    assert "2 table(s)" in problem["message"]
    assert [r.get("error") for r in doc["results"]] == [
        None,
        "interrupted",
        "interrupted",
    ]


# -- footers phase metrics -------------------------------------------------


@pytest.mark.aws
def test_footers_phase_reports_items_bytes_and_calls(
    catalog: CatalogHarness, tmp_path: Path, glue_env: None
) -> None:
    config = _config(catalog, tmp_path, stats_source="footer")
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1), (2)")
    catalog.write("INSERT INTO lake.main.t VALUES (3)")

    code, doc = _sync(config, "--table", "main.t")

    assert code == 0, doc["problems"]
    phase = _phase(doc, "footers")
    assert phase["items"] == phase["calls"] == 2
    assert phase["bytes"] > 0
