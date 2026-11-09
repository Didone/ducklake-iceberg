from __future__ import annotations

import os
import threading
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, cast

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.io.pyarrow import PyArrowFileIO

from iceduck import concurrency
from iceduck.concurrency import (
    CANCELLED,
    INTERRUPTED,
    ConsolidatedReport,
    Interrupted,
    Limits,
    Outcome,
    map_parallel,
    worker_cursor,
)


def test_map_parallel_preserves_input_order_and_captures_errors() -> None:
    def run(value: int) -> int:
        if value == 2:
            raise RuntimeError("failure")
        time.sleep(0.01 * (3 - value))
        return value * 10

    outcomes = map_parallel(run, [1, 2, 3], workers=3)

    assert [outcome.item for outcome in outcomes] == [1, 2, 3]
    assert [outcome.value for outcome in outcomes] == [10, None, 30]
    assert [outcome.exit_code for outcome in outcomes] == [0, 1, 0]
    assert outcomes[1].error == "failure"


def test_limits_cap_nested_workers_to_global_budget() -> None:
    limits = Limits(
        max_table_workers=4,
        max_footer_workers=8,
        max_total_workers=16,
    ).for_all_tables()

    assert limits.max_table_workers * limits.max_footer_workers <= 16
    assert limits.max_footer_workers == 4
    assert limits.duckdb_threads == max(
        1, (os.cpu_count() or 1) // limits.max_table_workers
    )


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ([0, 1, 4], 1),
        ([0, 2, 4], 2),
        ([0, 3, 4], 3),
        ([0, 4], 0),
        ([4, 4], 4),
        ([0, 1, 4, 0], 1),
        ([0, 4, 4], 0),
        ([4, 4, 4], 4),
        ([2, 3, 0], 2),
    ],
)
def test_consolidated_exit_code_precedence(values: list[int], expected: int) -> None:
    report = ConsolidatedReport(
        command="sync",
        outcomes=[Outcome(str(index), None, code) for index, code in enumerate(values)],
    )

    assert report.exit_code == expected


# -- fail-fast, interruption, ordering, budget ------------------------------


def _erase(outcomes: Sequence[Outcome[Any, Any]]) -> list[Outcome[object, object]]:
    return cast(list[Outcome[object, object]], list(outcomes))


def test_fail_fast_cancels_tasks_not_started() -> None:
    """A failure on the 1st task, with 1 worker, cancels the unstarted ones."""
    started: list[int] = []

    def run(value: int) -> int:
        started.append(value)
        if value == 0:
            return 1
        # the worker may pick up the 2nd before the cancellation; it holds the
        # worker and ensures the following ones are still queued by then
        time.sleep(0.1)
        return 0

    outcomes = map_parallel(
        run, [0, 1, 2, 3], workers=1, fail_fast=True, exit_code_of=lambda v: v
    )

    assert started[0] == 0
    assert set(started) <= {0, 1}
    assert outcomes[0].exit_code == 1
    assert outcomes[0].error is None
    assert [o.error for o in outcomes[2:]] == [CANCELLED, CANCELLED]
    assert all(o.exit_code == 1 for o in outcomes if o.error == CANCELLED)
    assert ConsolidatedReport("sync", _erase(outcomes)).exit_code == 1


def test_fail_fast_preserves_running_and_ignores_nothing_to_do() -> None:
    """Exit 4 is not a failure; a running task finishes even after a failure."""
    gate = threading.Event()

    def run(value: int) -> int:
        if value == 0:
            gate.wait(timeout=5)
            return 0
        if value == 1:
            gate.set()
            return 2
        return 4

    outcomes = map_parallel(
        run, [0, 1, 2], workers=2, fail_fast=True, exit_code_of=lambda v: v
    )

    assert outcomes[0].value == 0
    assert outcomes[0].exit_code == 0
    assert outcomes[1].exit_code == 2
    assert outcomes[2].error == CANCELLED


def test_without_fail_fast_partial_failure_runs_everything() -> None:
    outcomes = map_parallel(
        lambda v: v, [1, 0, 4, 0], workers=1, exit_code_of=lambda v: v
    )
    assert [o.exit_code for o in outcomes] == [1, 0, 4, 0]
    assert all(o.error is None for o in outcomes)


def test_keyboard_interrupt_cancels_pending_and_waits_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SIGINT cancels pending tasks, waits for running ones, ``Interrupted``."""
    started = threading.Event()
    release = threading.Event()

    def run(value: int) -> int:
        if value == 1:
            started.set()
            release.wait(timeout=5)
        return value * 10

    def interrupted(fs: Any) -> Any:
        # SIGINT with the 1st task running; it only finishes after the pending
        # ones are canceled, which has to wait for it
        started.wait(timeout=5)
        threading.Timer(0.05, release.set).start()
        raise KeyboardInterrupt

    monkeypatch.setattr(concurrency, "as_completed", interrupted)
    with pytest.raises(Interrupted) as info:
        map_parallel(run, [1, 2, 3], workers=1)

    outcomes = info.value.outcomes
    assert isinstance(info.value, KeyboardInterrupt)
    assert outcomes[0].value == 10
    assert outcomes[0].exit_code == 0
    assert all(o.error == INTERRUPTED and o.exit_code == 1 for o in outcomes[1:])


def test_to_results_sorted_by_table_regardless_of_finish_order() -> None:
    """Shuffled durations: ``results[]`` sorted by name and stable."""
    delays = {"s.c": 0.03, "s.a": 0.0, "s.b": 0.015}

    def run(name: str) -> dict[str, object]:
        time.sleep(delays[name])
        return {"files_added": 1}

    runs = [
        ConsolidatedReport(
            "sync", _erase(map_parallel(run, ["s.c", "s.a", "s.b"], workers=3))
        ).to_results()
        for _ in range(2)
    ]

    assert [r["table"] for r in runs[0]] == ["s.a", "s.b", "s.c"]
    assert [r["table"] for r in runs[0]] == [r["table"] for r in runs[1]]


def test_limits_capped_from_reports_each_reduced_limit() -> None:
    """4 x 8 > 16 => effective footers and I/O = 4."""
    requested = Limits(max_table_workers=4, max_footer_workers=8, max_total_workers=16)
    effective = requested.for_all_tables()

    assert effective.max_footer_workers == 4
    assert effective.max_io_workers == 4
    assert effective.capped_from(requested) == [
        ("max_footer_workers", 8, 4),
        ("max_io_workers", 8, 4),
    ]
    assert Limits().for_all_tables().capped_from(Limits()) == []


def test_limits_reject_non_positive() -> None:
    with pytest.raises(ValueError, match="max_footer_workers"):
        Limits(max_footer_workers=0)


def test_apply_limits_sizes_io_pools_and_pyiceberg_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(concurrency.PYICEBERG_MAX_WORKERS_ENV, "11")
    assert concurrency.io_workers(100) == 8  # default without apply_limits

    concurrency.apply_limits(
        Limits(max_io_workers=2, max_footer_workers=3, pyiceberg_max_workers=5)
    )

    assert concurrency.io_workers(100) == 2
    assert concurrency.io_workers(0) == 1
    assert concurrency.footer_workers(100) == 3
    assert os.environ[concurrency.PYICEBERG_MAX_WORKERS_ENV] == "5"
    from pyiceberg.utils.concurrent import ExecutorFactory

    assert ExecutorFactory.max_workers() == 5

    concurrency.reset_limits()
    assert os.environ[concurrency.PYICEBERG_MAX_WORKERS_ENV] == "11"
    assert concurrency.active_limits() == Limits()


def test_metadata_upload_pool_uses_max_io_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pools formerly fixed at 8 follow ``max_io_workers``."""
    from iceduck.writer import avro

    sizes: list[int] = []

    def spy(max_workers: int) -> ThreadPoolExecutor:
        sizes.append(max_workers)
        return concurrency.ContextThreadPoolExecutor(max_workers=max_workers)

    monkeypatch.setattr("iceduck.writer.avro.ContextThreadPoolExecutor", spy)
    monkeypatch.setattr(avro, "upload", lambda *args, **kwargs: None)
    concurrency.apply_limits(Limits(max_io_workers=3))

    avro.upload_all(PyArrowFileIO(), [(f"/x/{i}", b"") for i in range(10)])

    assert sizes == [3]


# -- DuckDB threading model -----------------------------------------------


def test_shared_connection_across_threads_raises_resource_in_use() -> None:
    """Negative case: proves ``worker_cursor`` is needed."""
    con = duckdb.connect()
    errors: list[BaseException] = []

    def hammer() -> None:
        try:
            for _ in range(30):
                con.execute("SELECT sum(i) FROM range(2000000) t(i)").fetchall()
        except duckdb.ResourceInUseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=hammer) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    con.close()

    assert errors, "a shared connection should refuse the 2nd call"
    assert "busy" in str(errors[0])


def test_worker_cursor_per_thread_has_no_contention() -> None:
    con = duckdb.connect()

    def query(_: int) -> int:
        with worker_cursor(con) as cursor:
            row = cursor.execute("SELECT sum(i) FROM range(200000) t(i)").fetchone()
            assert row is not None
            return int(row[0])

    outcomes = map_parallel(query, range(16), workers=8)
    con.close()

    assert all(o.error is None for o in outcomes)
    assert {o.value for o in outcomes} == {sum(range(200000))}


def test_registered_relation_is_invisible_in_derived_cursor() -> None:
    """A ``con.register`` relation does not carry over to the cursor."""
    con = duckdb.connect()
    con.register("parent_only", pa.table({"a": [1, 2]}))
    assert con.execute("SELECT count(*) FROM parent_only").fetchone() == (2,)

    with worker_cursor(con) as cursor, pytest.raises(duckdb.CatalogException):
        cursor.execute("SELECT count(*) FROM parent_only")
    con.close()


def _parquet_files(tmp_path: Path, count: int) -> list[Path]:
    paths = []
    for index in range(count):
        path = tmp_path / f"f{index:02d}.parquet"
        pq.write_table(pa.table({"id": list(range(index, index + 50))}), path)
        paths.append(path)
    return paths


def test_footers_with_dedicated_cursors_match_serial(tmp_path: Path) -> None:
    """8 workers, one dedicated cursor each; same as serial."""
    paths = _parquet_files(tmp_path, 32)
    con = duckdb.connect()
    sql = "SELECT num_rows, num_row_groups FROM parquet_file_metadata($path)"

    def footer(path: Path) -> tuple[Any, ...] | None:
        with worker_cursor(con) as cursor:
            return cursor.execute(sql, {"path": str(path)}).fetchone()

    serial = [footer(path) for path in paths]
    parallel = map_parallel(footer, paths, workers=8)
    con.close()

    assert all(o.error is None for o in parallel)
    assert [o.value for o in parallel] == serial


def test_parallel_footer_phase_is_not_slower_than_serial(tmp_path: Path) -> None:
    """32 local files; regression check without a fixed speedup."""
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.typedef import Record
    from pyiceberg.types import LongType, NestedField

    from iceduck.writer.stats import FileToExport, build_data_files

    schema = Schema(NestedField(1, "id", LongType(), required=False))
    arrow_schema = pa.schema(
        [pa.field("id", pa.int64(), metadata={b"PARQUET:field_id": b"1"})]
    )
    files = []
    for index in range(32):
        path = tmp_path / f"d{index:02d}.parquet"
        pq.write_table(pa.table({"id": list(range(100))}, schema=arrow_schema), path)
        files.append(
            FileToExport(
                path=str(path),
                file_size_in_bytes=path.stat().st_size,
                record_count=100,
                partition_values=Record(),
            )
        )
    spec = PartitionSpec(spec_id=0)
    io = PyArrowFileIO()

    def timed(workers: int) -> tuple[float, list[Any]]:
        start = time.perf_counter()
        result = build_data_files(files, schema, spec, {}, io, max_workers=workers)
        return time.perf_counter() - start, result

    timed(8)  # warm up PyArrow imports and caches
    serial_s, serial = timed(1)
    parallel_s, parallel = timed(8)

    assert [d.file_path for d in parallel] == [d.file_path for d in serial]
    assert [d.record_count for d in parallel] == [d.record_count for d in serial]
    # slack for CI noise: parallel must not be noticeably slower
    assert parallel_s <= serial_s * 1.5 + 0.25
