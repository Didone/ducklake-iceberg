"""Closed-partition compaction on a CDC-like workload.

Simulates a CDC pipeline: a table partitioned by `epoch_day(created_at)`,
small insert batches followed by updates and deletes on the same day. The
first four days are "closed" (the snapshots are aged in the catalog, as a
fixture); the last one keeps receiving writes.

Acceptance criterion: after maintenance, the median is >= 50% of
`target_file_size` in the closed partitions. The SAME suite runs on SQLite and
Postgres (`catalog` fixture).
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from typer.testing import CliRunner

from iceduck.catalog.connection import open_catalog
from iceduck.checkpoint.command import run_checkpoint
from iceduck.checkpoint.model import PartitionSizeReport
from iceduck.cli import app
from iceduck.errors import ExitCode
from tests.integration.conftest import CatalogHarness, cli_env_for

pytestmark = [pytest.mark.integration]

runner = CliRunner()

#: small target, proportional to one day of the workload (~19 KB after the
#: deletes are applied): the compacted partition fits in a file >= 50% of it
TARGET = 24 * 1024
CLOSED_AFTER = 3600
CLOSED_DAYS = 4
BATCHES = 8
ROWS = 150


def _batch(day: int, batch: int) -> str:
    """One consumer batch: insert, update part of the day, sparse delete."""
    base = day * 10_000
    first, last = base + batch * ROWS, base + batch * ROWS + ROWS - 1
    return (
        "INSERT INTO lake.main.cdc "
        f"SELECT {first} + range, ({first} + range) % 997, 'new', "
        f"(range % 89) * 1.25, 1, "
        f"TIMESTAMP '2026-01-01' + INTERVAL {day} DAY + INTERVAL (range * 17) SECOND, "
        f"TIMESTAMP '2026-01-01' + INTERVAL {day} DAY "
        f"FROM range({ROWS});"
        "UPDATE lake.main.cdc SET status = 'paid', versao = versao + 1 "
        f"WHERE id BETWEEN {base} AND {last} AND id % 7 = {batch % 7};"
        f"DELETE FROM lake.main.cdc WHERE id BETWEEN {base} AND {last} "
        f"AND id % 53 = {batch};"
    )


def _scalar(harness: CatalogHarness, sql: str) -> Any:
    with open_catalog(harness.settings(), read_only=True) as con:
        return con.query_arrow(sql).column(0)[0].as_py()


def _closed_checksum(harness: CatalogHarness) -> tuple[int, int]:
    """Rows and sum of `versao` for the closed days (maintenance keeps them)."""
    with open_catalog(harness.settings(), read_only=True) as con:
        row = con.query_arrow(
            "SELECT count(*) AS n, sum(versao) AS v FROM lake.main.cdc "
            f"WHERE id < {CLOSED_DAYS * 10_000}"
        ).to_pylist()[0]
    return int(row["n"]), int(row["v"])


def _cdc_table(harness: CatalogHarness) -> None:
    harness.write(
        "CREATE TABLE lake.main.cdc (id BIGINT, cliente_id BIGINT, "
        "status VARCHAR, valor DECIMAL(18,2), versao BIGINT, "
        "created_at TIMESTAMP, updated_at TIMESTAMP);"
        "ALTER TABLE lake.main.cdc SET PARTITIONED BY (epoch_day(created_at));"
        + "".join(_batch(d, b) for d in range(CLOSED_DAYS) for b in range(BATCHES))
    )
    # Fixture: the closed days were written "yesterday" (outside the window).
    last = int(
        _scalar(harness, "SELECT max(snapshot_id) FROM lake_meta.ducklake_snapshot")
    )
    harness.meta_exec(
        "UPDATE ducklake_snapshot SET snapshot_time = '2026-01-01 00:00:00+00' "
        f"WHERE snapshot_id <= {last}"
    )
    # current day: open, keeps receiving writes
    harness.write("".join(_batch(CLOSED_DAYS, b) for b in range(3)))


def _report(
    harness: CatalogHarness, threshold: float | None = None
) -> PartitionSizeReport:
    report = run_checkpoint(
        harness.settings(),
        table="main.cdc",
        target_file_size_bytes=TARGET,
        rewrite_delete_threshold=threshold,
        closed_after_seconds=CLOSED_AFTER,
    )
    assert report.exit_code != ExitCode.ERROR, report.to_json()
    partitions = report.results[0].partition_distribution
    assert partitions is not None
    return partitions


def _closed_medians(report: PartitionSizeReport) -> list[float]:
    return [
        p.distribution.median_bytes
        for p in report.closed
        if p.distribution.median_bytes is not None
    ]


def test_closed_partitions_reach_half_target(catalog: CatalogHarness) -> None:
    _cdc_table(catalog)
    checksum = _closed_checksum(catalog)

    # before: dry-run classifies 4 closed days and 1 open, all fragmented
    result = runner.invoke(
        app,
        [
            "checkpoint",
            "--table",
            "main.cdc",
            "--dry-run",
            "--target-file-size",
            str(TARGET),
            "--rewrite-delete-threshold",
            "0",
            "--closed-after",
            str(CLOSED_AFTER),
            "--json",
        ],
        env=cli_env_for(catalog),
    )
    assert result.exit_code == 0, result.stdout
    before = json.loads(result.stdout)["results"][0]["tables"][0]
    dist = before["partition_distribution"]
    assert dist["closed_partitions"] == CLOSED_DAYS
    assert dist["open_partitions"] == 1
    assert dist["closed_criterion_met"] is False
    assert before["rewrite_delete_threshold"] == 0.0
    assert before["rewrite_candidates"] > 0

    # DuckLake default behavior (rewrite_delete_threshold 0.95): merge does not
    # fuse a file with deletes and rewrite does not reach them -> unmet
    default = _report(catalog)
    assert default.closed_criterion_met is False
    assert len(default.closed) == CLOSED_DAYS

    # recommended maintenance: threshold 0, two passes. The 1st rewrites the
    # files with deletes; the 2nd merges what the 1st rewrote (merge does not
    # see, in the same transaction, the files created by the rewrite).
    _report(catalog, threshold=0.0)
    catalog.write(_batch(CLOSED_DAYS, 3))  # the open day keeps writing
    after = _report(catalog, threshold=0.0)

    assert len(after.closed) == CLOSED_DAYS
    assert [p.state for p in after.partitions].count("open") == 1
    assert after.closed_criterion_met is True
    for part in after.closed:
        assert part.distribution.total_bytes >= TARGET / 2  # strict criterion
        assert part.distribution.median_bytes is not None
        assert part.distribution.median_bytes >= TARGET / 2, part.to_json()
    assert min(_closed_medians(after)) > max(_closed_medians(default))

    # maintenance does not change data
    assert _closed_checksum(catalog) == checksum


def test_partition_report_json_is_additive(catalog: CatalogHarness) -> None:
    catalog.write(
        "CREATE TABLE lake.main.flat (id INTEGER);"
        "INSERT INTO lake.main.flat SELECT range FROM range(0, 50);"
        "INSERT INTO lake.main.flat SELECT range FROM range(50, 100);"
    )
    report = run_checkpoint(catalog.settings(), table="main.flat")
    row = report.to_json()["tables"][0]
    dist = row["partition_distribution"]
    # unpartitioned table: a single "partition", open (written just now)
    assert dist["partition_count"] == 1
    assert dist["partitions"][0]["partition"] == "(no partition)"
    assert dist["partitions"][0]["state"] == "open"
    assert dist["closed_criterion_met"] is None
    # the previous keys remain
    assert row["file_size_distribution"]["file_count"] == 1
