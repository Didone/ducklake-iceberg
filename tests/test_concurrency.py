from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.io.pyarrow import PyArrowFileIO

from ducklake_iceberg import concurrency
from ducklake_iceberg.concurrency import Limits, worker_cursor


def test_context_pool_preserves_context_and_input_order() -> None:
    """Workers see the submitter's context vars; ``map`` keeps input order."""
    var: ContextVar[str] = ContextVar("probe", default="unset")
    var.set("main")

    def run(value: int) -> tuple[int, str]:
        time.sleep(0.01 * (3 - value))
        return value * 10, var.get()

    with concurrency.ContextThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(run, [1, 2, 3]))

    assert results == [(10, "main"), (20, "main"), (30, "main")]


def test_context_pool_propagates_worker_errors() -> None:
    def run(value: int) -> int:
        if value == 2:
            raise RuntimeError("failure")
        return value

    with concurrency.ContextThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run, v) for v in (1, 2, 3)]

    assert futures[0].result() == 1
    with pytest.raises(RuntimeError, match="failure"):
        futures[1].result()
    assert futures[2].result() == 3


def test_limits_capped_reduces_every_limit_to_the_global_budget() -> None:
    requested = Limits(
        max_footer_workers=8,
        max_io_workers=8,
        max_total_workers=4,
        pyiceberg_max_workers=6,
    )
    effective = requested.capped()

    assert effective.max_total_workers == 4
    assert effective.max_footer_workers == 4
    assert effective.max_io_workers == 4
    assert effective.pyiceberg_max_workers == 4
    assert effective.capped_from(requested) == [
        ("max_footer_workers", 8, 4),
        ("max_io_workers", 8, 4),
        ("pyiceberg_max_workers", 6, 4),
    ]


def test_limits_within_the_budget_are_untouched() -> None:
    assert Limits().capped() == Limits()
    assert Limits().capped().capped_from(Limits()) == []


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
    from ducklake_iceberg.writer import avro

    sizes: list[int] = []

    def spy(max_workers: int) -> ThreadPoolExecutor:
        sizes.append(max_workers)
        return concurrency.ContextThreadPoolExecutor(max_workers=max_workers)

    monkeypatch.setattr("ducklake_iceberg.writer.avro.ContextThreadPoolExecutor", spy)
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

    with concurrency.ContextThreadPoolExecutor(max_workers=8) as pool:
        values = list(pool.map(query, range(16)))
    con.close()

    assert set(values) == {sum(range(200000))}


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
    with concurrency.ContextThreadPoolExecutor(max_workers=8) as pool:
        parallel = list(pool.map(footer, paths))
    con.close()

    assert parallel == serial


def test_parallel_footer_phase_is_not_slower_than_serial(tmp_path: Path) -> None:
    """32 local files; regression check without a fixed speedup."""
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.typedef import Record
    from pyiceberg.types import LongType, NestedField

    from ducklake_iceberg.writer.stats import FileToExport, build_data_files

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
