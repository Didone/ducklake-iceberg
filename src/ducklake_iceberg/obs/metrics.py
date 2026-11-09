"""Per-phase metrics.

``timing.phase(name)`` is the package's only stopwatch: it feeds the
``timings`` key (``DUCKLAKE_ICEBERG_TIMINGS=1``, legacy names consumed by external
tooling) and, when the name matches a ``Phase``, the ``RunMetrics`` active in
the context (``collect``). Rules:

- a phase's duration is **exclusive**: nested mapped phases are subtracted
  from the parent phase, so ``sum(phases[].duration_ms)`` never exceeds the
  total; whatever is not a phase becomes overhead;
- the same phase run more than once (e.g. ``doctor`` before and after the
  checkpoint) is summed into a single entry;
- ``items``/``bytes``/``calls``/``retries`` are per-phase counters that may be
  added before, during, or after the phase runs;
- AWS calls made by the process Sessions (``aws.session``) are attributed to
  the open phase through botocore's ``before-call`` hook; phases that count
  their own calls (``_EXPLICIT_CALL_PHASES``) are left out of the hook, so
  calls are not counted twice;
- an exception with exit code 0 or 4 (``NothingToDo``) is not a phase
  failure.

Everything is thread-safe: the package's pools copy the context into the
workers and the counters go to the same ``RunMetrics``.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, get_args

from ducklake_iceberg.obs.logging import log_event

Phase = Literal[
    "resolve",
    "doctor",
    "checkpoint",
    "delta",
    "enumerate",
    "footers",
    "stats",
    "deletes",
    "manifest",
    "manifest_list",
    "metadata_json",
    "s3_put",
    "glue",
    "validate",
    "retention",
    "glue_stats",
]
PHASES: tuple[Phase, ...] = get_args(Phase)

#: ``timing.phase`` name -> ``Phase``. Names that already are a ``Phase`` map
#: to themselves; the others are legacy ``timings`` subdivisions.
#: ``export.write`` is not a phase: the ``manifest``, ``manifest_list``,
#: ``metadata_json``, and ``s3_put`` phases are measured inside it.
_PHASE_BY_TIMING: dict[str, Phase] = {
    **{name: name for name in PHASES},
    "export.base": "delta",
    "export.manifests_read": "delta",
    "export.catalog": "enumerate",
    "export.stats": "stats",
    "export.deletes": "deletes",
    "register": "glue",
}

#: phases that count their own API calls (idempotent retry)
_EXPLICIT_CALL_PHASES: frozenset[Phase] = frozenset({"glue_stats"})
#: phases whose ``bytes`` make up ``bytes_written``
_WRITTEN_PHASES: frozenset[Phase] = frozenset(
    {"manifest", "manifest_list", "metadata_json", "deletes"}
)
_COUNTERS: tuple[str, ...] = ("items", "bytes", "calls", "retries")


@dataclass(slots=True)
class PhaseMetric:
    phase: Phase
    started_at: datetime
    duration_ms: int
    items: int = 0
    bytes: int = 0
    calls: int = 0
    retries: int = 0
    ok: bool = True
    error_code: str | None = None


@dataclass(slots=True)
class RunMetrics:
    run_id: str
    command: str
    table: str | None = None
    phases: list[PhaseMetric] = field(default_factory=list)
    #: domain totals added by the command (``files_added``...)
    summary: dict[str, int] = field(default_factory=dict)
    _started_ns: int = field(default_factory=time.perf_counter_ns, repr=False)
    #: counters not yet applied to a ``phases`` entry
    _pending: dict[str, dict[str, int]] = field(default_factory=dict, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @property
    def duration_ms(self) -> int:
        return max(
            0, math.ceil((time.perf_counter_ns() - self._started_ns) / 1_000_000)
        )

    def _entry(self, phase: str) -> PhaseMetric | None:
        return next((m for m in self.phases if m.phase == phase), None)

    def _flush(self) -> None:
        """Apply the pending counters to phases that already have an entry."""
        for phase in list(self._pending):
            entry = self._entry(phase)
            if entry is not None:
                _apply(entry, self._pending.pop(phase))

    def totals(self) -> dict[str, int]:
        """``api_calls``, ``retries``, and the domain totals from ``summary``."""
        with self._lock:
            self._flush()
            calls = sum(m.calls for m in self.phases) + sum(
                c.get("calls", 0) for c in self._pending.values()
            )
            retries = sum(m.retries for m in self.phases) + sum(
                c.get("retries", 0) for c in self._pending.values()
            )
        return {"api_calls": calls, "retries": retries, **self.summary}

    def bytes_written(self) -> int:
        """manifests + manifest list + metadata.json + delete files."""
        with self._lock:
            self._flush()
            return sum(m.bytes for m in self.phases if m.phase in _WRITTEN_PHASES)

    def to_json(self) -> dict[str, Any]:
        """``metrics`` block of a ``results[]`` item."""
        totals = self.totals()
        with self._lock:
            phases = [
                {
                    **asdict(metric),
                    "phase": metric.phase,
                    "started_at": metric.started_at.isoformat(),
                }
                for metric in self.phases
            ]
        return {
            "run_id": self.run_id,
            "duration_ms": self.duration_ms,
            "phases": phases,
            "totals": totals,
        }

    def record(
        self,
        phase: Phase,
        *,
        started_at: datetime,
        duration_ms: int,
        ok: bool = True,
        error_code: str | None = None,
    ) -> PhaseMetric:
        """Record a run of the phase (or add it to the existing entry)."""
        with self._lock:
            entry = self._entry(phase)
            if entry is None:
                entry = PhaseMetric(
                    phase=phase, started_at=started_at, duration_ms=duration_ms
                )
                entry.ok, entry.error_code = ok, error_code
                self.phases.append(entry)
            else:
                entry.duration_ms += duration_ms
                entry.ok = entry.ok and ok
                entry.error_code = entry.error_code or error_code
            _apply(entry, self._pending.pop(phase, {}))
            return entry

    def _add(self, phase: str, **counts: int) -> None:
        with self._lock:
            pending = self._pending.setdefault(phase, {})
            for name, value in counts.items():
                if value:
                    pending[name] = pending.get(name, 0) + value

    def add_call(self, phase: Phase, *, retry: bool = False) -> None:
        self._add(phase, calls=1, retries=int(retry))

    def add_retry(self, phase: Phase) -> None:
        self._add(phase, retries=1)

    def add_work(self, phase: Phase, *, items: int = 0, byte_count: int = 0) -> None:
        self._add(phase, items=items, bytes=byte_count)


def _apply(entry: PhaseMetric, counts: dict[str, int]) -> None:
    for name in _COUNTERS:
        if counts.get(name):
            setattr(entry, name, getattr(entry, name) + counts[name])


_ACTIVE: ContextVar[RunMetrics | None] = ContextVar(
    "ducklake_iceberg_metrics", default=None
)


@dataclass(slots=True)
class _Frame:
    """Open mapped phase; accumulates the time of child phases."""

    phase: Phase
    started_at: datetime
    parent: _Frame | None
    child_ns: int = 0


_FRAME: ContextVar[_Frame | None] = ContextVar("ducklake_iceberg_phase", default=None)
_FRAME_LOCK = threading.Lock()


def activate(metrics: RunMetrics) -> Token[RunMetrics | None]:
    return _ACTIVE.set(metrics)


def deactivate(token: Token[RunMetrics | None]) -> None:
    _ACTIVE.reset(token)


@contextmanager
def collect(metrics: RunMetrics) -> Iterator[RunMetrics]:
    """Activate ``metrics`` in the block's context (and in the workers it creates)."""
    token = activate(metrics)
    try:
        yield metrics
    finally:
        deactivate(token)


def current() -> RunMetrics | None:
    return _ACTIVE.get()


def current_phase() -> Phase | None:
    frame = _FRAME.get()
    return frame.phase if frame is not None else None


def add_work(phase: Phase, *, items: int = 0, byte_count: int = 0) -> None:
    metrics = current()
    if metrics is not None:
        metrics.add_work(phase, items=items, byte_count=byte_count)


def add_call(phase: Phase, *, retry: bool = False) -> None:
    metrics = current()
    if metrics is not None:
        metrics.add_call(phase, retry=retry)


def add_retry(phase: Phase) -> None:
    metrics = current()
    if metrics is not None:
        metrics.add_retry(phase)


def note_api_call() -> None:
    """botocore ``before-call`` hook: count the call in the open phase.

    With no open phase, the call only enters ``totals.api_calls``.
    """
    metrics = current()
    if metrics is None:
        return
    frame = _FRAME.get()
    if frame is None:
        metrics._add("_unattributed", calls=1)
    elif frame.phase not in _EXPLICIT_CALL_PHASES:
        metrics._add(frame.phase, calls=1)


PhaseHandle = tuple[_Frame, Token[_Frame | None]]


def enter_phase(name: str) -> PhaseHandle | None:
    """Open phase ``name`` if it is a ``Phase`` and metrics are active."""
    phase = _PHASE_BY_TIMING.get(name)
    if phase is None or current() is None:
        return None
    frame = _Frame(phase=phase, started_at=datetime.now(UTC), parent=_FRAME.get())
    token = _FRAME.set(frame)
    log_event("phase.start", level=logging.DEBUG, phase=phase)
    return frame, token


def _failed(error: BaseException | None) -> bool:
    if error is None:
        return False
    code = getattr(error, "exit_code", 1)
    try:
        return int(code) not in (0, 4)
    except (TypeError, ValueError):
        return True


def exit_phase(
    handle: PhaseHandle | None, elapsed_ns: int, error: BaseException | None
) -> None:
    """Close the phase: record the exclusive duration and emit ``phase.end``."""
    if handle is None:
        return
    frame, token = handle
    try:
        _FRAME.reset(token)
    except ValueError:  # closed in another context (generator finalized elsewhere)
        _FRAME.set(frame.parent)
    with _FRAME_LOCK:
        child_ns = frame.child_ns
        if frame.parent is not None:
            frame.parent.child_ns += elapsed_ns
    metrics = current()
    if metrics is None:
        return
    failed = _failed(error)
    entry = metrics.record(
        frame.phase,
        started_at=frame.started_at,
        duration_ms=max(0, elapsed_ns - child_ns) // 1_000_000,
        ok=not failed,
        error_code=type(error).__name__ if failed and error is not None else None,
    )
    log_event(
        "phase.end",
        level=logging.INFO,
        phase=frame.phase,
        duration_ms=entry.duration_ms,
        items=entry.items,
        bytes=entry.bytes,
        calls=entry.calls,
        retries=entry.retries,
        ok=not failed,
        error_code=entry.error_code if failed else None,
    )


__all__ = [
    "PHASES",
    "Phase",
    "PhaseHandle",
    "PhaseMetric",
    "RunMetrics",
    "activate",
    "add_call",
    "add_retry",
    "add_work",
    "collect",
    "current",
    "current_phase",
    "deactivate",
    "enter_phase",
    "exit_phase",
    "note_api_call",
]
