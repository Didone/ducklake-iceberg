"""A complete ledger is necessary before a benchmark can pass locally."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import Mock

import pytest


@pytest.fixture
def validator(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "scripts"))
    return importlib.import_module("bench_sync_validate")


def _ledger(directory: Path, **updates: Any) -> None:
    manifest: dict[str, Any] = {
        "revisions": {"baseline": "base", "candidate": "candidate"},
        "scenarios": ["N"],
        "warmups": 1,
        "samples": 1,
        "blocks": 1,
        "seed": 42,
    }
    (directory / "manifest.json").write_text(json.dumps(manifest))
    samples = []
    for revision in manifest["revisions"]:
        for index in (-1, 0):
            samples.append(
                {
                    "id": f"{revision}.{index}",
                    "revision": revision,
                    "scenario": "N",
                    "index": index,
                    "block": 0,
                    "warmup": index < 0,
                    "fixture_fingerprint": "same-values-and-shape",
                    "correct": True,
                    "actual_exit": 4,
                    "timeout": False,
                    "wall_ms": 1000,
                    "budget": {"local_pass": True},
                    "validation": {"independent_duckdb_reader": "pass"},
                    **(updates if revision == "candidate" and index == 0 else {}),
                }
            )
    (directory / "samples.jsonl").write_text(
        "".join(json.dumps(sample) + "\n" for sample in samples)
    )


def test_complete_local_ledger_keeps_aws_unverified(
    validator: ModuleType, tmp_path: Path
) -> None:
    _ledger(tmp_path)
    assert validator.validate(tmp_path) == 0
    result = json.loads((tmp_path / "cohort-validation.json").read_text())
    assert result["aws_latency"] == result["aws_requests"] == "unverified"


@pytest.mark.parametrize(
    "updates",
    [
        {"index": 1},
        {"fixture_fingerprint": None},
        {"fixture_fingerprint": "different"},
        {"correct": False},
        {"timeout": True},
        {"excluded": True},
        {"budget": {"local_pass": False}},
        {"validation": {"independent_duckdb_reader": "unverified"}},
        {"wall_ms": 1200},  # Passes absolute N limit, fails regression allowance.
        {"wall_ms": 3500},
    ],
)
def test_invalid_or_slow_ledger_cannot_pass(
    validator: ModuleType, tmp_path: Path, updates: dict[str, Any]
) -> None:
    _ledger(tmp_path, **updates)
    assert validator.validate(tmp_path) == 1


def test_duplicate_identity_rejected(validator: ModuleType, tmp_path: Path) -> None:
    _ledger(tmp_path, id="baseline.0")
    with pytest.raises(ValueError, match="duplicate"):
        validator.validate(tmp_path)


def test_fixture_audit_includes_all_object_pages(validator: ModuleType) -> None:
    reference = importlib.import_module("bench_sync_e2e")
    fixture = reference.Fixture.__new__(reference.Fixture)
    fixture.table = "table_alias"
    fixture.s3 = Mock()
    fixture.s3.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "table_alias/first", "ETag": "one"}]},
        {},
        {"Contents": [{"Key": "table_alias/last", "ETag": "two"}]},
    ]
    assert fixture.objects() == {"table_alias/first": "one", "table_alias/last": "two"}
