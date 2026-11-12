"""Benchmark failures must remain visible and never retain CLI payloads."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from scripts.bench_sync_support import (
    append_sample,
    distribution,
    measure,
    percentile,
    summarize,
)


def _process(exit_code: int = 4, **updates: Any) -> subprocess.CompletedProcess[bytes]:
    envelope = {
        "version": 1,
        "command": "sync",
        "status": "nothing-to-do" if exit_code == 4 else "error",
        "exit_code": exit_code,
        "duration_ms": 1,
        "results": [],
        "config": {"password": "never-store"},
    }
    envelope.update(updates)
    return subprocess.CompletedProcess(
        [], exit_code, json.dumps(envelope).encode(), b"secret stderr"
    )


def test_interpolated_percentiles_and_bootstrap_reproducible() -> None:
    assert percentile([], 0.95) is None
    assert percentile([10.0, 0.0, 20.0], 0.95) == 19.0
    assert distribution([1.0, 2.0, 3.0], resamples=30) == distribution(
        [1.0, 2.0, 3.0], resamples=30
    )


def test_sample_keeps_failure_but_not_payload() -> None:
    with patch("scripts.bench_sync_support.subprocess.run", return_value=_process(1)):
        sample = measure(["unused"], 4)
    assert sample["correct"] is False
    assert sample["actual_exit"] == 1
    assert sample["error"] == "unexpected_contract_or_exit"
    assert "never-store" not in json.dumps(sample)
    assert "secret stderr" not in json.dumps(sample)
    assert sample["native_http_attempts"] is None


@pytest.mark.parametrize("stdout", [b"not-json", b"[]", b"null", b"{}"])
def test_malformed_envelope_is_a_sample(stdout: bytes) -> None:
    with patch(
        "scripts.bench_sync_support.subprocess.run",
        return_value=subprocess.CompletedProcess([], 0, stdout, b""),
    ):
        assert measure(["unused"], 0)["error"] == "invalid_envelope"


def test_timeout_partial_output_is_not_a_completed_latency() -> None:
    with patch(
        "scripts.bench_sync_support.subprocess.run",
        side_effect=subprocess.TimeoutExpired([], 60, output=b"private"),
    ):
        sample = measure(["unused"], 0)
    result = summarize([sample])
    assert result["timed_out"] == 1
    assert result["completed"] == 0
    assert result["latency_ms"]["mean"] is None
    assert sample["stdout_bytes"] == 7
    assert "private" not in json.dumps(sample)


def test_append_preserves_warmups_and_failed_attempts(tmp_path: Path) -> None:
    with patch("scripts.bench_sync_support.subprocess.run", return_value=_process()):
        good = measure(["unused"], 4)
    failed = {**good, "actual_exit": 1, "correct": False, "warmup": False}
    warm = {**good, "warmup": True}
    path = tmp_path / "samples.jsonl"
    for item in (warm, good, failed):
        append_sample(path, item)
    samples = [json.loads(line) for line in path.read_text().splitlines()]
    result = summarize(samples)
    assert result["attempted"] == 3
    assert result["warmups"] == 1
    assert result["failed"] == 1
    assert result["completed"] == 2
    assert result["correct"] == 1


def test_loopback_http_observer_excludes_setup_and_never_keeps_payload() -> None:
    from scripts.bench_sync_support import LocalHttpObserver

    def app(environ: Any, respond: Any) -> list[bytes]:
        respond("404 Not Found", [("Authorization", "do-not-store")])
        return [b"sensitive payload"]

    def respond(*args: Any) -> None:
        pass

    observer = LocalHttpObserver(app)
    request = {"REQUEST_METHOD": "GET", "PATH_INFO": "/bucket/object"}
    observer(request, respond)
    observer.begin()
    observer(request, respond)
    observer({"HTTP_X_AMZ_TARGET": "AWSGlue.GetTable"}, respond)
    result = observer.end()
    assert result == {"s3.GET": 1, "glue.GetTable": 1, "status.404": 2}
    assert "payload" not in json.dumps(result)
    assert "do-not-store" not in json.dumps(result)


def test_comparison_does_not_turn_local_measurement_into_aws_pass() -> None:
    from scripts.bench_sync_support import compare

    before = {"latency_ms": {"mean": 1000.0, "p50": 900.0, "p95": 2000.0}}
    after = {"latency_ms": {"mean": 1200.0, "p50": 950.0, "p95": 2100.0}}
    comparison = compare(before, after, "N")
    assert comparison["mean"]["regression_allowance_met"] is False
    assert comparison["p95"]["regression_allowance_met"] is True
    assert comparison["authoritative_aws_verdict"] == "unverified"
