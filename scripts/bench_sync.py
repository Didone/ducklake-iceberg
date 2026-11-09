"""Performance benchmark for ``ducklake-iceberg sync`` (target: < 1 s per table).

Runs the command N times, each in a fresh process (like the E2E tests), with
``DUCKLAKE_ICEBERG_TIMINGS=1``, and prints p50/p95 of wall-clock time, in-process time
(``duration_ms``), ``startup`` (the difference between the two) and each
phase/counter in the envelope.

    python scripts/bench_sync.py --config .e2e-runs/<run>/ducklake-iceberg.yaml \\
        --table main.t_time -n 5

``--prepare`` runs a shell command before each iteration (for example, an
INSERT into DuckLake to measure the "1 new file" case). ``--args`` replaces the
measured command (default: ``sync``), for example ``--args "sync --dry-run"``.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


def _percentile(values: list[float], q: float) -> float:
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=100, method="inclusive")[int(q) - 1]


def _default_exe() -> list[str]:
    found = shutil.which("ducklake-iceberg")
    if found:
        return [found]
    return [sys.executable, "-c", "from ducklake_iceberg.cli import main; main()"]


def _run_once(cmd: list[str]) -> dict[str, Any]:
    env = {**os.environ, "DUCKLAKE_ICEBERG_TIMINGS": "1"}
    started = time.perf_counter()
    proc = subprocess.run(  # noqa: S603 - command built by this script itself
        cmd, capture_output=True, text=True, env=env, check=False
    )
    wall_ms = (time.perf_counter() - started) * 1000
    try:
        envelope = json.loads(proc.stdout)
    except json.JSONDecodeError:
        sys.stderr.write(proc.stdout + proc.stderr)
        raise SystemExit(f"output is not JSON (exit {proc.returncode})") from None
    return {"wall_ms": wall_ms, "exit_code": proc.returncode, "envelope": envelope}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--table", required=True)
    parser.add_argument("-n", type=int, default=5)
    parser.add_argument("--args", default="sync", help="subcommand and extra flags")
    parser.add_argument("--exe", default=None, help="ducklake-iceberg executable")
    parser.add_argument("--prepare", default=None, help="shell command before each run")
    parser.add_argument("--json", action="store_true", help="output the result as JSON")
    opts = parser.parse_args()

    exe = shlex.split(opts.exe) if opts.exe else _default_exe()
    cmd = [
        *exe,
        *shlex.split(opts.args),
        "--table",
        opts.table,
        "--config",
        str(opts.config),
        "--json",
    ]

    runs: list[dict[str, Any]] = []
    for i in range(opts.n):
        if opts.prepare:
            subprocess.run(opts.prepare, shell=True, check=True)  # noqa: S602
        run = _run_once(cmd)
        runs.append(run)
        sys.stderr.write(
            f"run {i + 1}/{opts.n}: exit {run['exit_code']} "
            f"wall {run['wall_ms']:.0f} ms\n"
        )

    series: dict[str, list[float]] = {
        "wall_ms": [],
        "duration_ms": [],
        "startup_ms": [],
    }
    counters: dict[str, list[float]] = {}
    for run in runs:
        env = run["envelope"]
        series["wall_ms"].append(run["wall_ms"])
        series["duration_ms"].append(float(env["duration_ms"]))
        series["startup_ms"].append(run["wall_ms"] - float(env["duration_ms"]))
        timings = env.get("timings") or {}
        for name, ms in timings.get("phases_ms", {}).items():
            series.setdefault(f"phase.{name}", []).append(float(ms))
        for name, n in timings.get("counters", {}).items():
            counters.setdefault(name, []).append(float(n))

    summary = {
        "cmd": cmd,
        "runs": opts.n,
        "exit_codes": sorted({r["exit_code"] for r in runs}),
        "ms": {
            k: {
                "p50": round(_percentile(v, 50), 1),
                "p95": round(_percentile(v, 95), 1),
            }
            for k, v in series.items()
        },
        "counters": {k: max(v) for k, v in sorted(counters.items())},
    }
    if opts.json:
        print(json.dumps(summary, indent=2))
        return
    print(f"{' '.join(cmd)}\nexit codes: {summary['exit_codes']}")
    print(f"{'metric':<32}{'p50 ms':>10}{'p95 ms':>10}")
    for name, stats in summary["ms"].items():
        print(f"{name:<32}{stats['p50']:>10.1f}{stats['p95']:>10.1f}")
    print("counters (max per run):")
    for name, n in summary["counters"].items():
        print(f"  {name:<30}{int(n):>6}")


if __name__ == "__main__":
    main()
