"""Process-wide limits for the internal thread pools.

The footer and I/O pools of one table export are sized by the limits installed
with ``apply_limits``; PyIceberg's own pool is sized by the
``PYICEBERG_MAX_WORKERS`` variable, which is written back here.
"""

from __future__ import annotations

import contextvars
import os
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, ParamSpec, TypeVar

if TYPE_CHECKING:
    import duckdb

R = TypeVar("R")
P = ParamSpec("P")

#: variable read by PyIceberg's ``ExecutorFactory`` (singleton pool)
PYICEBERG_MAX_WORKERS_ENV: Final[str] = "PYICEBERG_MAX_WORKERS"


@dataclass(frozen=True, slots=True)
class Limits:
    max_footer_workers: int = 8
    max_io_workers: int = 8
    max_total_workers: int = 32
    duckdb_threads: int | None = None
    duckdb_memory_limit: str | None = None
    pyiceberg_max_workers: int = 8

    def __post_init__(self) -> None:
        for name in (
            "max_footer_workers",
            "max_io_workers",
            "max_total_workers",
            "pyiceberg_max_workers",
        ):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be >= 1")
        if self.duckdb_threads is not None and self.duckdb_threads < 1:
            raise ValueError("duckdb_threads must be >= 1")

    def capped(self) -> Limits:
        """Effective limits reconciled with the global cap on workers."""
        cap = self.max_total_workers
        return Limits(
            max_footer_workers=min(self.max_footer_workers, cap),
            max_io_workers=min(self.max_io_workers, cap),
            max_total_workers=cap,
            duckdb_threads=self.duckdb_threads,
            duckdb_memory_limit=self.duckdb_memory_limit,
            pyiceberg_max_workers=min(self.pyiceberg_max_workers, cap),
        )

    def capped_from(self, requested: Limits) -> list[tuple[str, int, int]]:
        """``(limit, requested, effective)`` for each limit reduced by the cap."""
        return [
            (name, getattr(requested, name), getattr(self, name))
            for name in (
                "max_footer_workers",
                "max_io_workers",
                "pyiceberg_max_workers",
            )
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
