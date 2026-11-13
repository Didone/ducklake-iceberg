"""Durable, payload-free benchmark samples and reproducible statistics."""

from __future__ import annotations

import hashlib
import json
import math
import os
import random
import statistics
import subprocess
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = fraction * (len(ordered) - 1)
    lo, hi = math.floor(position), math.ceil(position)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (position - lo)


def distribution(
    values: list[float], seed: int = 42, resamples: int = 10_000
) -> dict[str, Any]:
    if not values:
        return {"n": 0, "mean": None, "p50": None, "p95": None}
    rng = random.Random(seed)  # noqa: S311 - reproducible bootstrap, not security
    means, tails = [], []
    for _ in range(resamples):
        sample = rng.choices(values, k=len(values))
        means.append(statistics.mean(sample))
        tail = percentile(sample, 0.95)
        assert tail is not None
        tails.append(tail)
    return {
        "n": len(values),
        "mean": statistics.mean(values),
        "p50": percentile(values, 0.5),
        "p95": percentile(values, 0.95),
        "min": min(values),
        "max": max(values),
        "sample_sd": statistics.stdev(values) if len(values) > 1 else 0.0,
        "bootstrap_seed": seed,
        "bootstrap_resamples": resamples,
        "mean_ci95": [percentile(means, 0.025), percentile(means, 0.975)],
        "p95_ci95": [percentile(tails, 0.025), percentile(tails, 0.975)],
    }


def append_sample(path: Path, sample: dict[str, Any]) -> None:
    """Append and fsync every attempt, preserving earlier results on interruption."""
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(sample, allow_nan=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def measure(
    cmd: list[str], expected_exit: int, timeout: float = 60.0
) -> dict[str, Any]:
    """Time process creation through pipe collection; never save raw payloads."""
    sample: dict[str, Any] = {
        "schema_version": 1,
        "timestamp": datetime.now(UTC).isoformat(),
        "expected_exit": expected_exit,
        "actual_exit": None,
        "timeout": False,
        "error": None,
        "correct": False,
        "sdk_http_attempts": None,
        "native_http_attempts": None,
        "backend_sessions": None,
        "backend_round_trips": None,
        "stdout_bytes": 0,
        "stderr_bytes": 0,
    }
    started = time.perf_counter_ns()
    try:
        result = subprocess.run(  # noqa: S603 - caller supplies an explicit argv
            cmd,
            capture_output=True,
            timeout=timeout,
            check=False,
            env={**os.environ, "DUCKLAKE_ICEBERG_TIMINGS": "1"},
        )
    except subprocess.TimeoutExpired as exc:
        sample["partial_stdout_sha256"] = hashlib.sha256(exc.stdout or b"").hexdigest()
        sample["partial_stderr_sha256"] = hashlib.sha256(exc.stderr or b"").hexdigest()
        sample.update(
            timeout=True,
            error="timeout",
            stdout_bytes=len(exc.stdout or b""),
            stderr_bytes=len(exc.stderr or b""),
        )
        return sample
    except OSError:
        sample["error"] = "process_start_failed"
        return sample
    finally:
        sample["wall_ms"] = (time.perf_counter_ns() - started) / 1_000_000
    if sample["wall_ms"] > timeout * 1000:
        sample.update(timeout=True, error="deadline_exceeded")
        return sample
    sample.update(
        actual_exit=result.returncode,
        stdout_bytes=len(result.stdout),
        stderr_bytes=len(result.stderr),
    )
    try:
        envelope = json.loads(result.stdout)
        valid = (
            envelope["version"] == 1
            and envelope["command"] == "sync"
            and envelope["exit_code"] == result.returncode == expected_exit
            and isinstance(envelope["results"], list)
            and isinstance(envelope["duration_ms"], int | float)
        )
        if not math.isfinite(envelope["duration_ms"]) or envelope["duration_ms"] < 0:
            raise ValueError("invalid duration")
        status = {0: "ok", 1: "error", 2: "blocked", 3: "diverged", 4: "nothing-to-do"}
        valid = valid and envelope.get("status") == status.get(expected_exit)
        sample["correct"] = bool(valid)
        sample["error"] = None if valid else "unexpected_contract_or_exit"
        sample["duration_ms"] = envelope["duration_ms"]
        sample["outside_envelope_ms"] = sample["wall_ms"] - envelope["duration_ms"]
        timings = envelope.get("timings") or {}
        # Only fixed numeric diagnostic names/values; no config, SQL, paths or payload.
        for name in ("counters", "phases_ms"):
            sample[name] = {
                k: v
                for k, v in timings.get(name, {}).items()
                if k.replace(".", "").replace("_", "").isalnum()
                and isinstance(v, int | float)
                and math.isfinite(v)
            }
        sample["sdk_http_attempts"] = (
            sum(
                v
                for k, v in sample["counters"].items()
                if k.startswith("aws.http_attempts.")
            )
            if any(k.startswith("aws.http_attempts.") for k in sample["counters"])
            else None
        )
    except (ValueError, TypeError, KeyError, AttributeError):
        sample.update(correct=False, error="invalid_envelope")
    return sample


def summarize(samples: list[dict[str, Any]], seed: int = 42) -> dict[str, Any]:
    admitted = [s for s in samples if not s.get("warmup") and not s.get("excluded")]
    complete = [
        s for s in admitted if s["actual_exit"] is not None and not s["timeout"]
    ]
    return {
        "attempted": len(samples),
        "admitted": len(admitted),
        "warmups": sum(bool(s.get("warmup")) for s in samples),
        "excluded": sum(bool(s.get("excluded")) for s in samples),
        "completed": len(complete),
        "correct": sum(bool(s["correct"]) for s in admitted),
        "failed": sum(not s["correct"] and not s["timeout"] for s in admitted),
        "timed_out": sum(bool(s["timeout"]) for s in admitted),
        "latency_ms": distribution([s["wall_ms"] for s in complete], seed),
        "blocks": {
            str(block): distribution(
                [s["wall_ms"] for s in complete if s.get("block") == block], seed
            )
            for block in sorted({s.get("block", 0) for s in admitted})
        },
    }


def compare(
    baseline: dict[str, Any], candidate: dict[str, Any], scenario: str
) -> dict[str, Any]:
    """Descriptive local comparison; never upgrades simulation to an AWS pass."""
    targets = {
        "mean": 1500.0 if scenario == "N" else 3000.0,
        "p95": 3000.0 if scenario == "N" else 5000.0,
    }
    baseline_passes = all(
        baseline["latency_ms"][metric] is not None
        and baseline["latency_ms"][metric] <= target
        for metric, target in targets.items()
    )
    result: dict[str, Any] = {}
    for metric in ("mean", "p50", "p95"):
        before, after = baseline["latency_ms"][metric], candidate["latency_ms"][metric]
        if before is None or after is None:
            result[metric] = {
                "baseline_ms": before,
                "candidate_ms": after,
                "gain_ms": None,
            }
            continue
        result[metric] = {
            "baseline_ms": before,
            "candidate_ms": after,
            "gain_ms": before - after,
            "gain_percent": 100 * (before - after) / before if before else None,
        }
        if metric in targets:
            result[metric]["reference_target_met_locally"] = after <= targets[metric]
            result[metric]["regression_allowance_met"] = (
                after <= before + max(0.1 * before, 100.0)
                if baseline_passes
                else after <= targets[metric]
            )
    result["authoritative_aws_verdict"] = "unverified"
    return result


class LocalHttpObserver:
    """Count received loopback HTTP requests without retaining URLs or headers.

    A server request is a separate observation of an SDK/native request, not an
    additional request to add to client counters. Fixture/audit actors stay off.
    """

    def __init__(self, app: Any) -> None:
        import threading

        self.app = app
        self.lock = threading.Lock()
        self.active = False
        self.counts: dict[str, int] = {}

    def begin(self) -> None:
        with self.lock:
            self.counts = {}
            self.active = True

    def end(self) -> dict[str, int]:
        with self.lock:
            self.active = False
            return dict(self.counts)

    def __call__(self, environ: Any, start_response: Any) -> Any:
        with self.lock:
            admitted = self.active
        target = environ.get("HTTP_X_AMZ_TARGET", "")
        method = environ.get("REQUEST_METHOD", "")
        if target.startswith("AWSGlue."):
            operation = "glue." + target.split(".")[-1]
        else:
            operation = "s3." + method
            if method == "GET" and environ.get("PATH_INFO", "").count("/") <= 1:
                operation += ".probe_or_list"

        def observed_response(status: str, headers: Any, exc_info: Any = None) -> Any:
            if admitted:
                with self.lock:
                    self.counts[operation] = self.counts.get(operation, 0) + 1
                    code = "status." + status.split()[0]
                    self.counts[code] = self.counts.get(code, 0) + 1
            return start_response(status, headers, exc_info)

        return self.app(environ, observed_response)


@contextmanager
def local_services(
    prefix: str = "sync-bench-",
    observer_factory: Callable[[Any], LocalHttpObserver] = LocalHttpObserver,
) -> Iterator[tuple[str, LocalHttpObserver, Path]]:
    """Own the loopback server, fake credentials and temporary fixture workspace.

    All benchmark profiles use this same lifecycle. Restore both inherited and
    newly introduced environment keys, even when fixture setup or audit fails.
    """
    import boto3
    from moto.server import ThreadedMotoServer

    def isolated(key: str) -> bool:
        return (
            key.startswith(("AWS_", "DUCKLAKE_ICEBERG_"))
            or key == "PYICEBERG_MAX_WORKERS"
        )

    old_env = {key: value for key, value in os.environ.items() if isolated(key)}
    old_session = boto3.DEFAULT_SESSION
    try:
        with ExitStack() as stack:
            server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
            server.start()
            stack.callback(server.stop)
            assert server._server is not None
            observer = observer_factory(server._server.app)
            server._server.app = observer
            host, port = server.get_host_and_port()
            endpoint = f"http://{host}:{port}"
            for key in list(os.environ):
                if isolated(key):
                    os.environ.pop(key)
            os.environ.update(
                {
                    "AWS_ACCESS_KEY_ID": "testing",
                    "AWS_SECRET_ACCESS_KEY": "testing",
                    "AWS_DEFAULT_REGION": "us-east-1",
                    "AWS_ENDPOINT_URL": endpoint,
                    "AWS_ENDPOINT_URL_S3": endpoint,
                    "AWS_ENDPOINT_URL_GLUE": endpoint,
                    "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "false",
                    "AWS_EC2_METADATA_DISABLED": "true",
                    "PYICEBERG_MAX_WORKERS": "8",
                }
            )
            boto3.DEFAULT_SESSION = None
            s3 = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1")
            stack.callback(s3.close)
            glue = boto3.client("glue", endpoint_url=endpoint, region_name="us-east-1")
            stack.callback(glue.close)
            s3.create_bucket(Bucket="benchmark-metadata")
            glue.create_database(DatabaseInput={"Name": "benchmark"})
            workspace = stack.enter_context(tempfile.TemporaryDirectory(prefix=prefix))
            yield endpoint, observer, Path(workspace)
    finally:
        boto3.DEFAULT_SESSION = old_session
        for key in list(os.environ):
            if isolated(key):
                os.environ.pop(key)
        os.environ.update(old_env)


def operation_budgets(
    sample: dict[str, Any], manifests: int, deletes: int = 0
) -> dict[str, Any]:
    """Evaluate retry-free reference budgets from received loopback requests."""
    scenario = sample["scenario"]
    counters, http = sample.get("counters", {}), sample.get("server_http", {})
    footers = counters.get("footers.files", 0)
    limits = {
        "N": (1, 1, 0, 0, 25),
        "I": (5, 4, 2, 8, 30),
        "A": (3, manifests + 4, 2, 6, 30),
        "F": (3, manifests + 4, 2, 6, 50),
        "D": (
            3,
            manifests + 4 + 2 * deletes + 2 * footers,
            2 + deletes + footers,
            8,
            35,
        ),
        "U": (
            3,
            manifests + 4 + 2 * deletes + 2 * footers,
            2 + deletes + footers,
            8,
            35,
        ),
    }[scenario]
    observed = {
        "glue": sum(v for k, v in http.items() if k.startswith("glue.")),
        "gets": sum(v for k, v in http.items() if k.startswith("s3.GET")),
        "heads": http.get("s3.HEAD", 0),
        "writes": http.get("s3.PUT", 0) + http.get("s3.POST", 0),
        "queries": counters.get("catalog.queries"),
    }
    checks = {
        name: observed[name] is not None and observed[name] <= maximum
        for name, maximum in zip(observed, limits, strict=True)
    }
    checks.update(
        {
            "no_list_or_bucket_probe": not http.get("s3.GET.probe_or_list", 0),
            "no_multipart_reference": not http.get("s3.POST", 0),
            "no_source_delete": not http.get("s3.DELETE", 0),
            "no_database_creation": not http.get("glue.CreateDatabase", 0),
            "one_root": counters.get("duckdb.connect") == 1,
            "attachment_budget": counters.get("ducklake.attach.successes")
            == (3 if scenario == "F" else 1),
            "complete_catalog_stats": scenario not in ("I", "A", "F") or footers == 0,
        }
    )
    return {
        "observed": observed,
        "limits": dict(zip(observed, limits, strict=True)),
        "checks": checks,
        "local_pass": all(checks.values()),
        "aws_verdict": "unverified",
    }
