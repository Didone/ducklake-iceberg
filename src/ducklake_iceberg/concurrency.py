from __future__ import annotations

import contextvars
import os
import time
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor, as_completed, wait
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final, Generic, ParamSpec, TypeVar

from ducklake_iceberg.obs.redaction import scrub

if TYPE_CHECKING:
    import duckdb

T = TypeVar("T")
R = TypeVar("R")
P = ParamSpec("P")

#: ``Outcome.error`` of a task not started because of ``--fail-fast``
CANCELLED: Final[str] = "cancelled"
#: ``Outcome.error`` of a task not started because of SIGINT
INTERRUPTED: Final[str] = "interrupted"
#: limits that reconciliation with the global cap may reduce
_CAPPABLE: Final[tuple[str, ...]] = (
    "max_table_workers",
    "max_footer_workers",
    "max_io_workers",
    "pyiceberg_max_workers",
)
#: variable read by PyIceberg's ``ExecutorFactory`` (singleton pool)
PYICEBERG_MAX_WORKERS_ENV: Final[str] = "PYICEBERG_MAX_WORKERS"


@dataclass(frozen=True, slots=True)
class Limits:
    max_table_workers: int = 4
    max_footer_workers: int = 8
    max_io_workers: int = 8
    max_total_workers: int = 32
    duckdb_threads: int | None = None
    duckdb_memory_limit: str | None = None
    pyiceberg_max_workers: int = 8

    def __post_init__(self) -> None:
        for name in (
            "max_table_workers",
            "max_footer_workers",
            "max_io_workers",
            "max_total_workers",
            "pyiceberg_max_workers",
        ):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be >= 1")
        if self.duckdb_threads is not None and self.duckdb_threads < 1:
            raise ValueError("duckdb_threads must be >= 1")

    def for_all_tables(self) -> Limits:
        """Effective limits reconciled with the global cap.

        Per-table pools (footers, I/O) are multiplied by the concurrent
        tables; each one ends up at ``max(1, total // tables)``. The PyIceberg
        pool is unique in the process and only honors the cap.
        """
        per_table = max(1, self.max_total_workers // self.max_table_workers)
        return Limits(
            max_table_workers=min(self.max_table_workers, self.max_total_workers),
            max_footer_workers=min(self.max_footer_workers, per_table),
            max_io_workers=min(self.max_io_workers, per_table),
            max_total_workers=self.max_total_workers,
            duckdb_threads=self.duckdb_threads
            or max(1, (os.cpu_count() or 1) // self.max_table_workers),
            duckdb_memory_limit=self.duckdb_memory_limit,
            pyiceberg_max_workers=min(
                self.pyiceberg_max_workers, self.max_total_workers
            ),
        )

    def capped_from(self, requested: Limits) -> list[tuple[str, int, int]]:
        """``(limit, requested, effective)`` for each limit reduced by the cap."""
        return [
            (name, getattr(requested, name), getattr(self, name))
            for name in _CAPPABLE
            if getattr(self, name) != getattr(requested, name)
        ]


#: effective limits of the running command (process-wide; see ``apply_limits``)
_active: Limits = Limits()
#: value of ``PYICEBERG_MAX_WORKERS`` before the first ``apply_limits``
_saved_pyiceberg_env: tuple[bool, str | None] | None = None


def apply_limits(limits: Limits) -> None:
    """Install the effective limits for the process's internal pools.

    The I/O pools (metadata upload, manifest reads, delete conversion) consult
    ``io_workers``; the PyIceberg pool is sized by ``PYICEBERG_MAX_WORKERS`` on
    first creation, so the variable is written here, before any use. This is
    process state: ``reset_limits`` undoes it (see ``tests/conftest.py``).
    """
    global _active, _saved_pyiceberg_env
    _active = limits
    if _saved_pyiceberg_env is None:
        current = os.environ.get(PYICEBERG_MAX_WORKERS_ENV)
        _saved_pyiceberg_env = (current is not None, current)
    os.environ[PYICEBERG_MAX_WORKERS_ENV] = str(limits.pyiceberg_max_workers)


def active_limits() -> Limits:
    """Limits installed by ``apply_limits`` (defaults if none)."""
    return _active


def io_workers(tasks: int) -> int:
    """I/O pool size for ``tasks`` tasks: ``[1, max_io_workers]``."""
    return max(1, min(_active.max_io_workers, tasks))


def footer_workers(tasks: int) -> int:
    """Footer pool size for ``tasks`` tasks."""
    return max(1, min(_active.max_footer_workers, tasks))


def reset_limits() -> None:
    """Restore the defaults and ``PYICEBERG_MAX_WORKERS`` (tests)."""
    global _active, _saved_pyiceberg_env
    _active = Limits()
    if _saved_pyiceberg_env is not None:
        had, value = _saved_pyiceberg_env
        if had and value is not None:
            os.environ[PYICEBERG_MAX_WORKERS_ENV] = value
        else:
            os.environ.pop(PYICEBERG_MAX_WORKERS_ENV, None)
        _saved_pyiceberg_env = None


@contextmanager
def worker_cursor(
    con: duckdb.DuckDBPyConnection,
) -> Iterator[duckdb.DuckDBPyConnection]:
    cursor = con.cursor()
    try:
        yield cursor
    finally:
        cursor.close()


class ContextThreadPoolExecutor(ThreadPoolExecutor):
    """``ThreadPoolExecutor`` that runs each task in a copy of the context.

    The stdlib ``ThreadPoolExecutor`` does not propagate ``contextvars``:
    without the copy, log lines emitted in workers lose ``run_id``,
    ``command``, and ``table``, and the phase counters (``obs.metrics``) are
    lost. The copy is taken in ``submit`` (in the submitting thread) and there
    is one per task: a ``Context`` cannot be active in two threads at once.
    ``map`` goes through ``submit`` and inherits the behavior. Every pool in
    the package uses this class.
    """

    def submit(
        self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs
    ) -> Future[R]:
        context = contextvars.copy_context()

        def run() -> R:
            return context.run(fn, *args, **kwargs)

        return super().submit(run)


@dataclass(frozen=True, slots=True)
class Outcome(Generic[T, R]):
    item: T
    value: R | None
    exit_code: int
    error: str | None = None
    duration_ms: int = 0


class Interrupted(KeyboardInterrupt):
    """SIGINT during ``map_parallel``: pending tasks canceled, running ones
    completed; ``outcomes`` carries the result of each item."""

    def __init__(self, outcomes: list[Outcome[Any, Any]]) -> None:
        super().__init__(INTERRUPTED)
        self.outcomes: list[Outcome[Any, Any]] = outcomes


def map_parallel(
    fn: Callable[[T], R],
    items: Iterable[T],
    *,
    workers: int,
    fail_fast: bool = False,
    exit_code_of: Callable[[R], int] | None = None,
) -> list[Outcome[T, R]]:
    """Run ``fn`` in a thread pool; return order = input order.

    ``exit_code_of`` extracts the exit code from a normal result (0 without it).
    A task fails when it raises or finishes with an exit code outside
    ``{0, 4}``; with ``fail_fast``, the first failure cancels the tasks not yet
    started (``error = "cancelled"``) and preserves the running ones.
    ``KeyboardInterrupt`` cancels the pending ones, waits for the running ones,
    and raises ``Interrupted`` with all the results.
    """
    if workers < 1:
        raise ValueError("workers must be >= 1")
    ordered = list(items)
    if not ordered:
        return []
    outcomes: list[Outcome[T, R] | None] = [None] * len(ordered)
    interrupted = False
    with ContextThreadPoolExecutor(max_workers=min(workers, len(ordered))) as pool:
        futures: dict[Future[tuple[R, int]], tuple[int, T, int]] = {}
        for index, item in enumerate(ordered):
            futures[pool.submit(_timed_call, fn, item)] = (
                index,
                item,
                time.perf_counter_ns(),
            )
        stopping = False
        try:
            for future in as_completed(futures):
                index, item, submitted_at = futures[future]
                outcome = _outcome_of(future, item, submitted_at, exit_code_of)
                outcomes[index] = outcome
                if fail_fast and not stopping and outcome.exit_code not in (0, 4):
                    stopping = True
                    _cancel_pending(futures)
        except KeyboardInterrupt:
            interrupted = True
            _cancel_pending(futures)
            wait(futures)
            for future, (index, item, submitted_at) in futures.items():
                if outcomes[index] is None:
                    outcomes[index] = (
                        Outcome(item, None, 1, INTERRUPTED)
                        if future.cancelled()
                        else _outcome_of(future, item, submitted_at, exit_code_of)
                    )
    result = [
        outcome if outcome is not None else Outcome(ordered[index], None, 1, CANCELLED)
        for index, outcome in enumerate(outcomes)
    ]
    if interrupted:
        raise Interrupted(result)
    return result


def _cancel_pending(futures: Iterable[Future[tuple[R, int]]]) -> None:
    """Cancel the tasks not yet started; running ones continue to the end."""
    for pending in futures:
        pending.cancel()


def _outcome_of(
    future: Future[tuple[R, int]],
    item: T,
    submitted_at: int,
    exit_code_of: Callable[[R], int] | None,
) -> Outcome[T, R]:
    if future.cancelled():
        return Outcome(item, None, 1, CANCELLED)
    try:
        value, duration_ms = future.result()
    except Exception as exc:
        return Outcome(
            item,
            None,
            int(getattr(exc, "exit_code", 1)),
            scrub(str(exc)),
            max(0, (time.perf_counter_ns() - submitted_at) // 1_000_000),
        )
    code = int(exit_code_of(value)) if exit_code_of is not None else 0
    return Outcome(item, value, code, duration_ms=duration_ms)


def _timed_call(fn: Callable[[T], R], item: T) -> tuple[R, int]:
    start = time.perf_counter_ns()
    value = fn(item)
    return value, max(0, (time.perf_counter_ns() - start) // 1_000_000)


@dataclass
class ConsolidatedReport:
    command: str
    outcomes: list[Outcome[object, object]] = field(default_factory=list)

    @property
    def exit_code(self) -> int:
        codes = [outcome.exit_code for outcome in self.outcomes]
        if not codes:
            return 4
        for code in (1, 2, 3):
            if code in codes:
                return code
        return 4 if codes and all(code == 4 for code in codes) else 0

    def to_results(self) -> list[dict[str, object]]:
        results: list[dict[str, object]] = []
        for outcome in sorted(self.outcomes, key=lambda item: str(item.item)):
            result: dict[str, object] = {}
            if isinstance(outcome.value, dict):
                result.update(outcome.value)
            elif outcome.value is not None:
                result["result"] = outcome.value
            result.update(
                {
                    "table": str(outcome.item),
                    "exit_code": outcome.exit_code,
                    "duration_ms": outcome.duration_ms,
                }
            )
            if outcome.error:
                result["error"] = outcome.error
            results.append(result)
        return results
