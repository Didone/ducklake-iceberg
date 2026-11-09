"""Phase timing and process counters (performance yardstick for ``sync``).

Off by default: with ``DUCKLAKE_ICEBERG_TIMINGS=1`` the envelope gains a ``timings`` key
with the accumulated time per phase (ms) and counters of expensive operations
(DuckDB connections, credential resolutions, AWS calls). Phase names follow
the observability ``Phase`` names so that metrics can reuse them.

Thread-safe: phases and counters may be recorded from workers.
"""

from __future__ import annotations

import os
import threading
import time
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Final

from ducklake_iceberg.obs.metrics import enter_phase, exit_phase, note_api_call

ENV_FLAG: Final[str] = "DUCKLAKE_ICEBERG_TIMINGS"

_LOCK = threading.Lock()
_PHASES: dict[str, float] = defaultdict(float)
_COUNTERS: dict[str, int] = defaultdict(int)


def enabled() -> bool:
    return os.environ.get(ENV_FLAG, "") not in ("", "0", "false")


@contextmanager
def phase(name: str) -> Iterator[None]:
    """Accumulate the block's wall-clock time under ``name`` (reentrant by name).

    When ``name`` is a known ``Phase`` and metrics are active in the context,
    the phase also enters ``RunMetrics`` and emits ``phase.start``/``phase.end``
    (``obs.metrics``), even when the block raises.
    """
    start = time.perf_counter_ns()
    handle = enter_phase(name)
    error: BaseException | None = None
    try:
        yield
    except BaseException as exc:
        error = exc
        raise
    finally:
        elapsed_ns = time.perf_counter_ns() - start
        with _LOCK:
            _PHASES[name] += elapsed_ns / 1_000_000_000
        exit_phase(handle, elapsed_ns, error)


def count(name: str, n: int = 1) -> None:
    with _LOCK:
        _COUNTERS[name] += n


def counter(name: str) -> int:
    with _LOCK:
        return _COUNTERS.get(name, 0)


def instrument_session(session: Any) -> None:
    """Count the AWS calls (``aws.<service>.<operation>``) of a Session.

    Each call also enters ``calls`` of the phase open in the caller's context
    (``obs.metrics.note_api_call``).
    """

    def _before_call(model: Any, **_: Any) -> None:
        count(f"aws.{model.service_model.service_name}.{model.name}")
        note_api_call()

    session.events.register("before-call.*.*", _before_call)


def snapshot() -> dict[str, Any]:
    with _LOCK:
        return {
            "phases_ms": {k: round(v * 1000, 1) for k, v in sorted(_PHASES.items())},
            "counters": dict(sorted(_COUNTERS.items())),
        }


def reset() -> None:
    """Clear phases and counters (tests)."""
    with _LOCK:
        _PHASES.clear()
        _COUNTERS.clear()


__all__ = [
    "ENV_FLAG",
    "count",
    "counter",
    "enabled",
    "instrument_session",
    "phase",
    "reset",
    "snapshot",
]
