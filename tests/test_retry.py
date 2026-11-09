"""Retry: error classification and the commit loop."""

from __future__ import annotations

import logging
from typing import Any

import pytest
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)
from pyiceberg.exceptions import (
    CommitFailedException,
    CommitStateUnknownException,
    NoSuchTableError,
)

from iceduck.errors import DivergenceError
from iceduck.retry import (
    CommitExhausted,
    RetryClass,
    RetryPolicy,
    _compute_delay,
    _extract_error_code,
    classify_glue_error,
    commit_with_retry,
)


def _client_error(code: str, status: int = 400) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": "x"},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "UpdateTable",
    )


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (_client_error("ConcurrentModificationException"), RetryClass.CONFLICT),
        (CommitFailedException("cas"), RetryClass.CONFLICT),
        (_client_error("InternalServiceException", 500), RetryClass.RETRYABLE),
        (_client_error("OperationTimeoutException"), RetryClass.UNKNOWN_STATE),
        (_client_error("ResourceNotReadyException"), RetryClass.RETRYABLE),
        (_client_error("FederationSourceRetryableException"), RetryClass.RETRYABLE),
        (_client_error("ThrottlingException"), RetryClass.RETRYABLE),
        (_client_error("EntityNotFoundException"), RetryClass.FATAL),
        (NoSuchTableError("t"), RetryClass.FATAL),
        (_client_error("InvalidInputException"), RetryClass.FATAL),
        (_client_error("GlueEncryptionException"), RetryClass.FATAL),
        (_client_error("AlreadyExistsException"), RetryClass.FATAL),
        (_client_error("ResourceNumberLimitExceededException"), RetryClass.FATAL),
        (_client_error("FederationSourceException"), RetryClass.FATAL),
        (_client_error("AccessDeniedException"), RetryClass.FATAL),
        (CommitStateUnknownException("?"), RetryClass.UNKNOWN_STATE),
        (EndpointConnectionError(endpoint_url="https://glue"), RetryClass.RETRYABLE),
        (ConnectTimeoutError(endpoint_url="https://glue"), RetryClass.RETRYABLE),
        (ReadTimeoutError(endpoint_url="https://glue"), RetryClass.UNKNOWN_STATE),
        (
            ConnectionClosedError(endpoint_url="https://glue"),
            RetryClass.UNKNOWN_STATE,
        ),
        (_client_error("SomethingNew", 503), RetryClass.RETRYABLE),
        (_client_error("SomethingNew", 400), RetryClass.FATAL),
        (RuntimeError("read timeout"), RetryClass.FATAL),
    ],
)
def test_classify_glue_error(exc: BaseException, expected: RetryClass) -> None:
    assert classify_glue_error(exc) is expected


def test_classify_client_error_without_metadata_is_fatal() -> None:
    exc = ClientError({"Error": {"Code": "Weird"}}, "UpdateTable")
    assert classify_glue_error(exc) is RetryClass.FATAL


def test_extract_error_code() -> None:
    assert _extract_error_code(None) is None
    assert _extract_error_code(_client_error("X")) == "X"
    assert _extract_error_code(ClientError({}, "op")) is None
    assert _extract_error_code(ValueError()) == "ValueError"


def test_compute_delay_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("iceduck.retry.random.uniform", lambda a, b: b)
    assert _compute_delay(1, base=0.5, factor=2, max_delay=8) == 0.5
    assert _compute_delay(4, base=0.5, factor=2, max_delay=8) == 4.0
    assert _compute_delay(6, base=0.5, factor=2, max_delay=8) == 8.0
    assert _compute_delay(2, base=0.5, factor=2, max_delay=8, multiplier=2) == 2.0


class _Clock:
    """Fake monotonic clock: ``sleep`` advances the time."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    c = _Clock()
    monkeypatch.setattr("iceduck.retry.time.monotonic", c.monotonic)
    monkeypatch.setattr("iceduck.retry.time.sleep", c.sleep)
    monkeypatch.setattr("iceduck.retry.random.uniform", lambda a, b: b)
    return c


class _Stub:
    def __init__(self, failures: list[BaseException], *, reconcile: bool = False):
        self.failures = list(failures)
        self.plans = 0
        self.commits = 0
        self.reconciles = 0
        self.guards = 0
        self.reconcile = reconcile

    def plan(self) -> int:
        self.plans += 1
        return self.plans

    def commit(self, plan: int) -> object:
        self.commits += 1
        if self.failures:
            raise self.failures.pop(0)
        return f"ok-{plan}"

    def reconcile_fn(self, plan: int) -> bool:
        self.reconciles += 1
        return self.reconcile

    def guard(self) -> None:
        self.guards += 1

    def run(self, policy: RetryPolicy | None = None) -> Any:
        return commit_with_retry(
            self.plan,
            self.commit,
            policy=policy,
            reconcile_fn=self.reconcile_fn,
            guard_fn=self.guard,
        )


def test_success_after_two_conflicts_replans_each_time(clock: _Clock) -> None:
    stub = _Stub([CommitFailedException("a"), CommitFailedException("b")])
    assert stub.run() == "ok-3"
    assert stub.plans == 3  # replanning is mandatory
    assert stub.guards == 3
    assert stub.reconciles == 2
    assert clock.sleeps == [0.5, 1.0]


def test_always_conflict_exhausts_with_five_logs(
    clock: _Clock, caplog: pytest.LogCaptureFixture
) -> None:
    stub = _Stub([_client_error("ConcurrentModificationException")] * 10)
    with (
        caplog.at_level(logging.INFO, logger="iceduck"),
        pytest.raises(CommitExhausted) as info,
    ):
        stub.run()
    attempts = info.value.attempts
    assert [a.attempt for a in attempts] == [1, 2, 3, 4, 5]
    assert all(a.outcome == "conflict" for a in attempts)
    assert attempts[-1].slept_s == 0.0  # the last failure is also recorded
    assert info.value.exit_code == 1
    assert "ConcurrentModificationException" in info.value.message
    assert sum(clock.sleeps) <= 60
    assert all(s <= min(8, 0.5 * 2**i) for i, s in enumerate(clock.sleeps))
    assert "attempt 5" in caplog.text


def test_resource_not_ready_doubles_backoff(clock: _Clock) -> None:
    stub = _Stub([_client_error("ResourceNotReadyException")])
    assert stub.run() == "ok-2"
    assert clock.sleeps == [1.0]


def test_unknown_state_reconciled_returns_none(clock: _Clock) -> None:
    stub = _Stub([_client_error("OperationTimeoutException")], reconcile=True)
    assert stub.run() is None
    assert stub.commits == 1
    assert clock.sleeps == []


def test_fatal_raises_immediately(clock: _Clock) -> None:
    exc = _client_error("AccessDeniedException")
    stub = _Stub([exc])
    with pytest.raises(ClientError):
        stub.run()
    assert stub.commits == 1
    assert stub.reconciles == 0


def test_guard_aborts_without_commit(clock: _Clock) -> None:
    stub = _Stub([])

    def guard() -> None:
        raise DivergenceError("external")

    with pytest.raises(DivergenceError):
        commit_with_retry(
            stub.plan, stub.commit, reconcile_fn=stub.reconcile_fn, guard_fn=guard
        )
    assert stub.plans == 0


def test_budget_exhausted(clock: _Clock) -> None:
    policy = RetryPolicy(budget_s=0.7)
    stub = _Stub([CommitFailedException("x")] * 10)
    with pytest.raises(CommitExhausted) as info:
        stub.run(policy)
    # 0.5 + 0.2 (truncated to the budget), then stops
    assert clock.sleeps == [0.5, pytest.approx(0.2)]
    assert len(info.value.attempts) == 3


def test_deadline_exhausted_before_attempt(clock: _Clock) -> None:
    policy = RetryPolicy(deadline_s=1.0, budget_s=100.0)
    stub = _Stub([CommitFailedException("x")] * 10)
    with pytest.raises(CommitExhausted) as info:
        stub.run(policy)
    assert "1.0s" in info.value.message
    assert sum(clock.sleeps) <= 1.0


def test_deadline_stops_after_failure(
    clock: _Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = RetryPolicy(deadline_s=10.0)
    stub = _Stub([CommitFailedException("x")] * 10)

    def slow_commit(plan: int) -> object:
        clock.now += 20.0
        return stub.commit(plan)

    with pytest.raises(CommitExhausted) as info:
        commit_with_retry(
            stub.plan,
            slow_commit,
            policy=policy,
            reconcile_fn=stub.reconcile_fn,
            guard_fn=stub.guard,
        )
    assert len(info.value.attempts) == 1
    assert clock.sleeps == []


def test_default_policy_values() -> None:
    p = RetryPolicy()
    assert (p.max_attempts, p.base_delay_s, p.factor, p.max_delay_s) == (5, 0.5, 2, 8)
    assert (p.budget_s, p.deadline_s) == (60.0, 300.0)
