"""Concurrency: retry and exception classification.

Implements retry logic with exponential backoff and jitter, classification of
Glue exceptions as retryable/fatal/unknown, and reconciliation of unknown
state.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Final, TypeVar

from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)

from iceduck.errors import DivergenceError, IceduckError
from iceduck.obs.logging import log_event
from iceduck.obs.metrics import Phase, add_call, add_retry

LOG = logging.getLogger("iceduck")

T = TypeVar("T")


# -- enums and structures -------------------------------------------------------


class RetryClass(StrEnum):
    """Exception classification for the retry policy."""

    RETRYABLE = "retryable"  # retry after replanning
    CONFLICT = "conflict"  # lost CAS: always replan
    UNKNOWN_STATE = "unknown_state"  # reconcile before deciding
    FATAL = "fatal"  # abort


@dataclass(frozen=True)
class RetryPolicy:
    """Retry policy with exponential backoff and jitter."""

    max_attempts: int = 5  # 1 tentativa + 4 retries
    base_delay_s: float = 0.5
    factor: float = 2.0
    max_delay_s: float = 8.0
    budget_s: float = 60.0  # sum of the waits
    deadline_s: float = 300.0  # total wall-clock time of the commit


@dataclass(frozen=True)
class AttemptLog:
    """Record of one commit attempt."""

    attempt: int
    outcome: str  # "ok" | "ok (reconciled)" | RetryClass | "fatal" | "aborted"
    error_code: str | None
    slept_s: float
    base_snapshot_id: int | None
    metadata_location: str | None


@dataclass
class RetryContext:
    """Retry context with accumulated state."""

    policy: RetryPolicy
    attempts: list[AttemptLog] = field(default_factory=list)
    total_sleep_s: float = 0.0
    start_time: float = field(default_factory=lambda: time.monotonic())

    def elapsed(self) -> float:
        """Wall-clock seconds since the commit started."""
        return time.monotonic() - self.start_time

    def record(
        self,
        attempt: int,
        outcome: str,
        *,
        error_code: str | None = None,
        slept_s: float = 0.0,
    ) -> None:
        """Append an attempt and record it in the structured log."""
        entry = AttemptLog(
            attempt=attempt,
            outcome=str(outcome),
            error_code=error_code,
            slept_s=slept_s,
            base_snapshot_id=None,
            metadata_location=None,
        )
        self.attempts.append(entry)
        LOG.info(
            "attempt %d: %s (code=%s, wait=%.2fs)",
            attempt,
            entry.outcome,
            error_code,
            slept_s,
        )


# -- exceptions -----------------------------------------------------------------


class CommitExhausted(IceduckError):  # noqa: N818 - name is part of the public contract
    """Retry exhausted after max_attempts, wait budget, or deadline (exit 1)."""

    def __init__(self, message: str, *, attempts: list[AttemptLog]) -> None:
        super().__init__(message)
        self.attempts = attempts


class CommitStateUnknown(IceduckError):  # noqa: N818 - name is part of the public contract
    """Commit state unknown (requires reconciliation)."""


# -- exception classification ---------------------------------------------------

#: ``ClientError`` codes by class.
_CONFLICT_CODES: Final[frozenset[str]] = frozenset({"ConcurrentModificationException"})
_UNKNOWN_CODES: Final[frozenset[str]] = frozenset({"OperationTimeoutException"})
_RETRYABLE_CODES: Final[frozenset[str]] = frozenset(
    {
        "InternalServiceException",
        "ResourceNotReadyException",
        "FederationSourceRetryableException",
        "ServiceUnavailable",
        "ServiceUnavailableException",
        "RequestTimeout",
        "ThrottlingException",  # unverified: RETRYABLE if it shows up
    }
)
_FATAL_CODES: Final[frozenset[str]] = frozenset(
    {
        "EntityNotFoundException",
        "InvalidInputException",
        "GlueEncryptionException",
        "AlreadyExistsException",
        "ResourceNumberLimitExceededException",
        "FederationSourceException",
        "AccessDeniedException",
        "ValidationException",
    }
)

#: code whose backoff is doubled (longer backoff, x2).
_SLOW_BACKOFF_CODES: Final[frozenset[str]] = frozenset({"ResourceNotReadyException"})


def _http_status(exc: ClientError) -> int | None:
    meta = exc.response.get("ResponseMetadata", {})
    status = meta.get("HTTPStatusCode") if isinstance(meta, dict) else None
    return status if isinstance(status, int) else None


def classify_glue_error(exc: BaseException) -> RetryClass:
    """Classify a Glue exception as retryable, fatal, or unknown.

    Args:
        exc: The caught exception.

    Returns:
        The appropriate retry class. When in doubt, ``FATAL``: retrying blindly
        could overwrite someone else's commit.
    """
    exc_name = type(exc).__name__

    # Exceptions mapped by PyIceberg (verified in 0.12.0)
    if "CommitFailed" in exc_name or "ConcurrentModification" in exc_name:
        return RetryClass.CONFLICT
    if "NoSuchTable" in exc_name:
        return RetryClass.FATAL
    if "CommitStateUnknown" in exc_name:
        return RetryClass.UNKNOWN_STATE

    if isinstance(exc, ClientError):
        code = _error_code(exc)
        if code in _CONFLICT_CODES:
            return RetryClass.CONFLICT
        if code in _UNKNOWN_CODES:
            return RetryClass.UNKNOWN_STATE
        if code in _RETRYABLE_CODES:
            return RetryClass.RETRYABLE
        if code in _FATAL_CODES:
            return RetryClass.FATAL
        # Other server errors (5xx) without a known code
        status = _http_status(exc)
        if status is not None and status >= 500:
            return RetryClass.RETRYABLE
        return RetryClass.FATAL

    # Network error BEFORE sending: the request did not reach the service
    if isinstance(exc, ConnectTimeoutError | EndpointConnectionError):
        return RetryClass.RETRYABLE

    # Network error AFTER sending: the UpdateTable may have been applied
    if isinstance(exc, ReadTimeoutError | ConnectionClosedError):
        return RetryClass.UNKNOWN_STATE

    # Any other exception: fatal, to be safe
    return RetryClass.FATAL


def _error_code(exc: ClientError) -> str:
    error = exc.response.get("Error", {})
    code = error.get("Code", "") if isinstance(error, dict) else ""
    return str(code)


# -- backoff and jitter ---------------------------------------------------------


def _compute_delay(
    attempt: int,
    *,
    base: float,
    factor: float,
    max_delay: float,
    multiplier: float = 1.0,
) -> float:
    """Compute the delay with exponential backoff and full jitter.

    Args:
        attempt: Attempt number (1-indexed).
        base: Base delay in seconds.
        factor: Multiplicative factor.
        max_delay: Cap per wait.
        multiplier: Multiplier of the nominal delay (x2 for ``ResourceNotReady``).

    Returns:
        Delay in ``[0, min(max_delay, multiplier * base * factor^(n-1))]``.
    """
    nominal = multiplier * base * (factor ** (attempt - 1))
    capped = min(nominal, max_delay)
    # Full jitter: uniform(0, capped)
    return random.uniform(0, capped)  # noqa: S311 - jitter, not crypto


def _delay_multiplier(exc: BaseException) -> float:
    if isinstance(exc, ClientError) and _error_code(exc) in _SLOW_BACKOFF_CODES:
        return 2.0
    return 1.0


# -- main retry -----------------------------------------------------------------


def commit_with_retry(
    plan_fn: Callable[[], T],
    commit_fn: Callable[[T], object],
    *,
    policy: RetryPolicy | None = None,
    reconcile_fn: Callable[[T], bool],
    guard_fn: Callable[[], None],
) -> object:
    """Run a commit with retry, replanning, and reconciliation.

    Args:
        plan_fn: Replans and returns the plan; called on every attempt.
        commit_fn: Runs ``catalog.commit_table`` with the plan.
        policy: Retry policy (uses the default if None).
        reconcile_fn: True if our commit won. Called after every non-fatal
            failure, **before** replanning.
        guard_fn: Raises ``DivergenceError`` if an external write happened.
            Called before each attempt.

    Returns:
        The result of ``commit_fn``, or ``None`` when reconciliation found
        that the commit had already won.

    Raises:
        CommitExhausted: Attempts, wait budget, or deadline exhausted.
        DivergenceError: External write detected (exit 3).
        Exception: The exception itself when classified as ``FATAL``.
    """
    if policy is None:
        policy = RetryPolicy()

    ctx = RetryContext(policy=policy)
    last_code: str | None = None

    for attempt in range(1, policy.max_attempts + 1):
        if ctx.elapsed() >= policy.deadline_s:
            raise CommitExhausted(
                f"commit exceeded {policy.budget_s}s/{policy.deadline_s}s "
                f"(attempt {attempt}); last error: {last_code or 'unknown'}",
                attempts=ctx.attempts,
            )

        # Guard against external writes: never retry over one
        try:
            guard_fn()
        except DivergenceError:
            LOG.error("external write detected; aborting without retrying")
            ctx.record(attempt, "aborted", error_code="external_write")
            raise

        # Replanning is mandatory on every attempt
        LOG.info("planning commit (attempt %d/%d)", attempt, policy.max_attempts)
        plan = plan_fn()

        try:
            # the ``UpdateTable`` call itself enters ``calls`` through the
            # botocore hook (obs.metrics); only the repetition is counted here
            if attempt > 1:
                add_retry("glue")
            result = commit_fn(plan)
        except Exception as exc:
            cls = classify_glue_error(exc)
            last_code = _extract_error_code(exc)
            if cls is RetryClass.CONFLICT:
                log_event(
                    "glue.conflict",
                    level=logging.WARNING,
                    error_code=last_code,
                    attempt=attempt,
                )
            LOG.warning(
                "attempt %d failed: %s (class=%s, code=%s)",
                attempt,
                exc,
                cls,
                last_code,
            )
            if cls is RetryClass.FATAL:
                ctx.record(attempt, "fatal", error_code=last_code)
                raise

            # Reconcile before replanning: a botocore resend may have won and
            # come back as a conflict.
            if reconcile_fn(plan):
                ctx.record(attempt, "ok (reconciled)", error_code=last_code)
                return None

            reason = _stop_reason(ctx, attempt)
            if reason is not None:
                ctx.record(attempt, cls, error_code=last_code)
                LOG.error("retry stopped: %s", reason)
                break

            delay = _compute_delay(
                attempt,
                base=policy.base_delay_s,
                factor=policy.factor,
                max_delay=policy.max_delay_s,
                multiplier=_delay_multiplier(exc),
            )
            remaining = min(
                policy.budget_s - ctx.total_sleep_s,
                policy.deadline_s - ctx.elapsed(),
            )
            slept = max(0.0, min(delay, remaining))
            if slept > 0:
                time.sleep(slept)
                ctx.total_sleep_s += slept
            ctx.record(attempt, cls, error_code=last_code, slept_s=slept)
            continue

        ctx.record(attempt, "ok")
        return result

    raise CommitExhausted(
        f"commit failed after {len(ctx.attempts)} attempts; "
        f"last error: {last_code or 'unknown'}",
        attempts=ctx.attempts,
    )


def call_with_retry(
    fn: Callable[[], T],
    *,
    phase: Phase,
    policy: RetryPolicy | None = None,
) -> T:
    """**Idempotent** call with exponential backoff and jitter.

    For calls that can be resent without reconciliation (e.g.
    ``UpdateColumnStatisticsForTable``, which overwrites the statistics):
    retries the ``RETRYABLE``, ``CONFLICT``, and ``UNKNOWN_STATE`` classes up
    to ``policy.max_attempts`` or the wait budget; ``FATAL`` is raised at once.
    Each attempt counts in the phase's ``calls``; repetitions, in ``retries``.
    """
    if policy is None:
        policy = RetryPolicy()
    ctx = RetryContext(policy=policy)
    for attempt in range(1, policy.max_attempts + 1):
        add_call(phase, retry=attempt > 1)
        try:
            return fn()
        except Exception as exc:
            if classify_glue_error(exc) is RetryClass.FATAL:
                raise
            if _stop_reason(ctx, attempt) is not None:
                raise
            delay = _compute_delay(
                attempt,
                base=policy.base_delay_s,
                factor=policy.factor,
                max_delay=policy.max_delay_s,
                multiplier=_delay_multiplier(exc),
            )
            slept = max(0.0, min(delay, policy.budget_s - ctx.total_sleep_s))
            LOG.warning(
                "%s: attempt %d failed (%s); retrying in %.2fs",
                phase,
                attempt,
                _extract_error_code(exc),
                slept,
            )
            if slept > 0:
                time.sleep(slept)
                ctx.total_sleep_s += slept
    raise AssertionError("unreachable: the loop always returns or raises")


def _stop_reason(ctx: RetryContext, attempt: int) -> str | None:
    """Reason there is no next attempt, or ``None`` to keep going."""
    policy = ctx.policy
    if attempt >= policy.max_attempts:
        return f"maximum of {policy.max_attempts} attempts reached"
    if ctx.total_sleep_s >= policy.budget_s:
        return f"wait budget ({policy.budget_s:.1f}s) exhausted"
    if ctx.elapsed() >= policy.deadline_s:
        return f"total deadline ({policy.deadline_s:.1f}s) exhausted"
    return None


def _extract_error_code(exc: BaseException | None) -> str | None:
    """Extract the error code from an exception."""
    if exc is None:
        return None
    if isinstance(exc, ClientError):
        return _error_code(exc) or None
    return type(exc).__name__
