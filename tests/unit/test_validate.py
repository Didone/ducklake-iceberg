"""Pure rules of ``iceduck.validate``."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from iceduck.errors import ExitCode
from iceduck.validate import (
    CheckId,
    CheckResult,
    CheckStatus,
    Divergence,
    PredicateChoice,
    Tolerances,
    ValidationReport,
    _absent_value,
    _compare_samples,
    _equal,
    _iceberg_filter,
    _row_count_hints,
)

US = Tolerances()
MS = Tolerances(timestamp_precision="ms")


def _report(*statuses: CheckStatus, **extra: Any) -> ValidationReport:
    checks = [
        CheckResult(
            CheckId.ROW_COUNT,
            status,
            1,
            {"ducklake_rows": 1, "iceberg_metadata_rows": 1},
            [Divergence("scalar", None, 1, 2)]
            if status is CheckStatus.DIVERGENT
            else [],
        )
        for status in statuses
    ]
    return ValidationReport(
        schema_name="main",
        table_name="t",
        glue_identifier=("analytics", "t"),
        ducklake_snapshot_id=42,
        iceberg_snapshot_id=7,
        checks=checks,
        tolerances_applied=[],
        **extra,
    )


def test_timestamp_micros_diverge_with_us_and_pass_with_ms() -> None:
    """Sub-millisecond remainder diverges with ``us``; passes with ``ms``."""
    lake = [(1, datetime(2024, 1, 1, 10, 0, 0, 123456))]
    iceberg = [(1, datetime(2024, 1, 1, 10, 0, 0, 123000))]

    diffs = _compare_samples(lake, iceberg, ("id", "ts"), US, key_width=1)
    assert [(d.kind, d.column) for d in diffs] == [("field", "ts")]
    assert diffs[0].key == {"id": 1}
    assert _compare_samples(lake, iceberg, ("id", "ts"), MS, key_width=1) == []


def test_float_sum_uses_relative_tolerance() -> None:
    """A sum in a different order passes within ``rtol=1e-9``."""
    values = [0.1 * i for i in range(1, 1000)]
    forward, backward = sum(values), sum(reversed(values))
    assert _equal(forward, backward, US, floating=True)
    assert not _equal(1.0, 1.0 + 1e-6, US, floating=True)
    assert not _equal(forward, backward + 1e-3, US, floating=False)
    assert _equal(float("nan"), float("nan"), US, floating=True)
    assert _equal(None, None, US, floating=False)
    assert not _equal(None, 0, US, floating=False)


def test_sample_classification_row_missing_and_row_extra() -> None:
    """Missing in Iceberg -> ``row_missing``; extra in Iceberg -> ``row_extra``."""
    lake = [(1, "a"), (2, "b")]
    iceberg = [(2, "b"), (3, "c")]

    diffs = _compare_samples(lake, iceberg, ("id", "s"), US, key_width=1)

    assert [(d.kind, d.key) for d in diffs] == [
        ("row_missing", {"id": 1}),
        ("row_extra", {"id": 3}),
    ]


def test_inlined_hint_points_to_checkpoint() -> None:
    """Rows in DuckLake and an empty manifest point to ``iceduck checkpoint``."""
    hints = _row_count_hints(10, 0, 0, None)
    assert any("iceduck checkpoint" in hint for hint in hints)
    assert not any("--deep" in hint for hint in hints)


def test_count_hints_direct_to_deep_and_retention() -> None:
    """Without ``--deep`` it is inconclusive; ``deep < meta`` hints retention."""
    assert any("--deep" in hint for hint in _row_count_hints(10, 9, 9, None))
    assert any("retention" in hint for hint in _row_count_hints(10, 10, 10, 7))
    assert any("writer defect" in hint for hint in _row_count_hints(10, 10, 12, None))
    assert _row_count_hints(10, 10, 10, 10) == []


def test_exit_code_priority_error_over_divergence_over_pass() -> None:
    """Error beats divergence; divergence beats pass."""
    assert _report(CheckStatus.PASS).exit_code is ExitCode.SUCCESS
    assert (
        _report(CheckStatus.PASS, CheckStatus.DIVERGENT).exit_code is ExitCode.DIVERGED
    )
    assert _report(CheckStatus.DIVERGENT, CheckStatus.ERROR).exit_code is ExitCode.ERROR
    assert _report(CheckStatus.SKIPPED).exit_code is ExitCode.NOTHING_TO_DO


def test_external_write_is_always_divergence() -> None:
    """Iceberg snapshot ahead of the published one -> exit 3, even with no diff."""
    report = _report(
        CheckStatus.PASS,
        external_write=True,
        published_iceberg_snapshot_id=5,
    )
    assert report.exit_code is ExitCode.DIVERGED
    assert report.message == (
        "external write detected: Iceberg snapshot 7 ≠ published 5"
    )


def test_summary_message_counts_checks() -> None:
    report = _report(CheckStatus.PASS, CheckStatus.PASS)
    assert report.message == "validate: 2/2 checks ok (snapshot 42)"
    payload = report.as_dict()
    assert payload["checks"][0]["message"].startswith("row_count: DuckLake 1")


def test_absent_values_are_above_maximum() -> None:
    assert _absent_value("int32", 10) == 11
    assert _absent_value("float64", 2.5) == 5.0
    assert str(_absent_value("varchar", "zz")) > "zz"
    assert _absent_value("boolean", True) is None


def test_range_choice_builds_half_open_filter() -> None:
    choice = PredicateChoice("id", "int32", 10, "range", 3, "...", upper=13)
    assert "LessThan(" in repr(_iceberg_filter(choice))
    inclusive = PredicateChoice(
        "id", "int32", 10, "range", 3, "...", upper=10, upper_inclusive=True
    )
    assert "LessThanOrEqual(" in repr(_iceberg_filter(inclusive))
