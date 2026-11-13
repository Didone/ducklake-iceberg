"""Separate local observer diagnostics; never pooled with reference latency.

Ten paired control/traced attempts per scenario use the same candidate CLI.
Only loopback server observation changes; CLI timing counters stay enabled.
No AWS resources are used. Run after, never alongside, reference cohorts.
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import shutil
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

from bench_sync_e2e import SCENARIOS, Fixture
from bench_sync_support import (
    LocalHttpObserver,
    append_sample,
    distribution,
    local_services,
    measure,
    summarize,
    write_json,
)


class TraceObserver(LocalHttpObserver):
    def __init__(self, app: Any) -> None:
        super().__init__(app)
        self.trace: list[dict[str, Any]] = []

    def begin(self) -> None:
        self.trace = []
        super().begin()

    def __call__(self, environ: Any, respond: Any) -> Any:
        with self.lock:
            traced = self.active
        started = time.perf_counter_ns() if traced else 0
        target = environ.get("HTTP_X_AMZ_TARGET", "")
        operation = (
            "glue." + target.split(".")[-1]
            if target.startswith("AWSGlue.")
            else "s3." + environ.get("REQUEST_METHOD", "unknown")
        )

        def headers(status: str, values: Any, exc_info: Any = None) -> Any:
            if traced:
                with self.lock:
                    self.trace.append(
                        {
                            "request_alias": uuid.uuid4().hex,
                            "operation": operation,
                            "status": int(status.split()[0]),
                            "received_ns": started,
                            "server_headers_ms": (time.perf_counter_ns() - started)
                            / 1_000_000,
                        }
                    )
            return respond(status, values, exc_info)

        return super().__call__(environ, headers)


def run(opts: argparse.Namespace) -> int:
    opts.output.mkdir(parents=True, exist_ok=False)
    (opts.output / "traces").mkdir()
    manifest = {
        "schema_version": 1,
        "profile": "local-mock",
        "suite": "diagnostics",
        "candidate_revision": opts.candidate_revision,
        "seed": opts.seed,
        "pairs_per_scenario": opts.pairs,
        "scenarios": list(SCENARIOS),
        "scope": "incremental server observer; CLI counters enabled in both arms",
    }
    write_json(opts.output / "manifest.json", manifest)
    samples: list[dict[str, Any]] = []
    rng = random.Random(opts.seed)  # noqa: S311 - reproducible paired order
    try:
        with local_services("sync-diagnostics-", TraceObserver) as (
            endpoint,
            observer,
            workspace,
        ):
            for scenario in SCENARIOS:
                for pair in range(opts.pairs):
                    arms = ["control", "trace"]
                    rng.shuffle(arms)
                    for arm in arms:
                        identifier = uuid.uuid4().hex
                        root = Path(workspace) / identifier
                        root.mkdir()
                        os.environ["DUCKLAKE_ICEBERG_STATE_DIR"] = str(root / "state")
                        fixture = None
                        sample: dict[str, Any] = {
                            "id": identifier,
                            "scenario": scenario,
                            "pair": pair,
                            "arm": arm,
                            "warmup": False,
                            "block": 0,
                            "excluded": False,
                        }
                        try:
                            fixture = Fixture(
                                root, endpoint, scenario, opts.candidate_exe
                            )
                            fixture.prepare()
                            if arm == "trace":
                                observer.begin()
                            sample.update(
                                measure(
                                    fixture.cmd, SCENARIOS[scenario][1], opts.timeout
                                )
                            )
                            sample["server_http"] = (
                                observer.end() if arm == "trace" else None
                            )
                            if arm == "trace":
                                write_json(
                                    opts.output / "traces" / f"{identifier}.json",
                                    observer.trace,
                                )
                            audit = fixture.audit() if sample["correct"] else {}
                            sample["validation"] = audit
                            sample["correct"] = bool(
                                sample["correct"]
                                and audit.get("exact_values")
                                and audit.get("source_intact")
                                and audit.get("zero_copy")
                            )
                        except Exception as exc:
                            sample.update(correct=False, error=type(exc).__name__)
                            sample.setdefault("actual_exit", None)
                            sample.setdefault("timeout", False)
                        finally:
                            observer.end()
                            append_sample(opts.output / "samples.jsonl", sample)
                            samples.append(sample)
                            try:
                                if fixture is not None:
                                    fixture.cleanup()
                            finally:
                                shutil.rmtree(root)
                        print(
                            f"{scenario} {pair} {arm}: {sample['correct']}", flush=True
                        )
    finally:
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for sample in samples:
            groups[sample["scenario"] + "." + sample["arm"]].append(sample)
        summaries = {key: summarize(value, opts.seed) for key, value in groups.items()}
        overhead = {}
        for scenario in SCENARIOS:
            differences = []
            for pair in range(opts.pairs):
                paired = {
                    s["arm"]: s
                    for s in samples
                    if s["scenario"] == scenario
                    and s["pair"] == pair
                    and s.get("correct")
                }
                if set(paired) == {"control", "trace"}:
                    differences.append(
                        paired["trace"]["wall_ms"] - paired["control"]["wall_ms"]
                    )
            overhead[scenario] = distribution(differences, opts.seed)
        write_json(
            opts.output / "summary.json",
            {"groups": summaries, "paired_overhead_ms": overhead},
        )
        (opts.output / "report.md").write_text(
            "# Local observer diagnostics\n\n"
            "CLI counters remain enabled in both arms.\n\n"
            + "\n".join(
                f"- {name}: mean paired overhead {value['mean']} ms"
                for name, value in overhead.items()
            )
            + "\n"
        )
    return int(not samples or any(not sample["correct"] for sample in samples))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-exe", required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--pairs", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--output", type=Path, required=True)
    opts = parser.parse_args()
    if opts.pairs < 10 or opts.timeout <= 0:
        parser.error("at least ten diagnostic pairs and a positive timeout required")
    if not Path(opts.candidate_exe).is_absolute() or not os.access(
        opts.candidate_exe, os.X_OK
    ):
        parser.error("candidate executable must be an absolute executable path")
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    raise SystemExit(run(opts))


if __name__ == "__main__":
    main()
