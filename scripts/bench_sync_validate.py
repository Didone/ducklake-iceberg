"""Rebuild a private reference benchmark report from its durable sample ledger."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from bench_sync_support import compare, summarize, write_json


def validate(directory: Path) -> int:
    manifest = json.loads((directory / "manifest.json").read_text())
    samples = [
        json.loads(line)
        for line in (directory / "samples.jsonl").read_text().splitlines()
    ]
    revisions = [
        name for name, value in manifest["revisions"].items() if value is not None
    ]
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    identities: set[str] = set()
    for sample in samples:
        if sample["id"] in identities:
            raise ValueError("duplicate sample identity")
        identities.add(sample["id"])
        if (
            sample["revision"] not in revisions
            or sample["scenario"] not in manifest["scenarios"]
        ):
            raise ValueError("sample is outside the declared cohort")
        groups[sample["scenario"] + "." + sample["revision"]].append(sample)
    checks = {}
    for scenario in manifest["scenarios"]:
        for revision in revisions:
            name = scenario + "." + revision
            group = groups[name]
            warmups = [sample for sample in group if sample.get("warmup")]
            admitted = [
                sample
                for sample in group
                if not sample.get("warmup") and not sample.get("excluded")
            ]
            blocks: dict[int, int] = defaultdict(int)
            for sample in admitted:
                blocks[sample["block"]] += 1
            checks[name] = {
                "attempts": len(group),
                "warmups": len(warmups),
                "admitted": len(admitted),
                "expected_counts": len(warmups) == manifest["warmups"]
                and len(admitted) == manifest["samples"],
                "indices": sorted(sample["index"] for sample in admitted)
                == list(range(manifest["samples"]))
                and sorted(sample["index"] for sample in warmups)
                == list(range(-manifest["warmups"], 0)),
                "blocks": dict(blocks),
                "block_shape": len(blocks) == manifest["blocks"]
                and all(
                    count == manifest["samples"] // manifest["blocks"]
                    for count in blocks.values()
                ),
                "correct": all(sample.get("correct") for sample in admitted),
                "independent_reader": all(
                    sample.get("validation", {}).get("independent_duckdb_reader")
                    == "pass"
                    for sample in admitted
                ),
                "no_timeouts": not any(sample.get("timeout") for sample in admitted),
                "no_exclusions": not any(sample.get("excluded") for sample in group),
                "local_budgets": all(
                    sample.get("budget", {}).get("local_pass") for sample in admitted
                ),
            }
    pairs: dict[tuple[str, int], set[str]] = defaultdict(set)
    for sample in samples:
        if sample.get("fixture_fingerprint"):
            pairs[(sample["scenario"], sample["index"])].add(
                sample["fixture_fingerprint"]
            )
    equivalent = all(sample.get("fixture_fingerprint") for sample in samples) and all(
        len(fingerprints) == 1 for fingerprints in pairs.values()
    )
    summary = {key: summarize(value, manifest["seed"]) for key, value in groups.items()}
    comparisons = {
        reference: {
            scenario: compare(
                summary[scenario + "." + reference],
                summary[scenario + ".candidate"],
                scenario,
            )
            for scenario in manifest["scenarios"]
        }
        for reference in ("baseline", "corrected")
        if reference in revisions
    }
    write_json(
        directory / "rebuilt-summary.json",
        {"groups": summary, "comparisons": comparisons},
    )
    ledger = {
        "schema_version": 1,
        "cohorts": checks,
        "equivalent_fixture_pairs": equivalent,
        "aws_latency": "unverified",
        "aws_requests": "unverified",
        "note": "Local simulation cannot establish authoritative AWS acceptance.",
    }
    write_json(directory / "cohort-validation.json", ledger)
    report = [
        "# Local reference comparison",
        "",
        "Fresh processes; real SQLite/Parquet; simulated S3/Glue. "
        "AWS gates remain unverified.",
        "",
        "| Scenario | Revision | n | Mean ms | p50 ms | p95 ms | "
        "Correct | Local budgets |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for scenario in manifest["scenarios"]:
        for revision in revisions:
            name = scenario + "." + revision
            item, check = summary[name], checks[name]
            values = item["latency_ms"]
            report.append(
                f"| {scenario} | {revision} | {values['n']} | "
                f"{values['mean']} | {values['p50']} | {values['p95']} | "
                f"{check['correct']} | {check['local_budgets']} |"
            )
    report.extend(
        [
            "",
            "All warmups, failures and timeouts remain in samples.jsonl. "
            "Confidence intervals, block summaries and unrounded comparisons "
            "are in rebuilt-summary.json.",
            "",
            f"Equivalent paired fixture fingerprints: {equivalent}.",
            "",
        ]
    )
    (directory / "validated-report.md").write_text("\n".join(report))
    required = (
        "expected_counts",
        "indices",
        "block_shape",
        "correct",
        "independent_reader",
        "no_timeouts",
        "no_exclusions",
    )
    failed = not equivalent or any(
        not check[key] for check in checks.values() for key in required
    )
    failed = failed or any(
        not check["local_budgets"]
        for name, check in checks.items()
        if name.endswith(".candidate")
    )
    failed = failed or any(
        not comparison[metric].get(key, False)
        for reference in comparisons.values()
        for comparison in reference.values()
        for metric in ("mean", "p95")
        for key in ("reference_target_met_locally", "regression_allowance_met")
    )
    return int(failed)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    opts = parser.parse_args()
    raise SystemExit(validate(opts.directory))


if __name__ == "__main__":
    main()
