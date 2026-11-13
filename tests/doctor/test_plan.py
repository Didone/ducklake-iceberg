"""Tests for the execution plan and gate short-circuiting.

The integration cases live in `tests/integration/test_revalidation.py`.
Synthetic rules (fakes) cover the short-circuit without a catalog; the real
rules cover the declared order.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from jsonschema import validate

from ducklake_iceberg.doctor import rules as real_rules
from ducklake_iceberg.doctor.model import (
    DoctorContext,
    DoctorTarget,
    Finding,
    RuleStatus,
    Severity,
)
from ducklake_iceberg.doctor.plan import (
    PLAN_ORDER,
    ExecutionPlan,
    PlanError,
    execution_plan,
)
from ducklake_iceberg.doctor.runner import run

_SCHEMA = json.loads((Path(__file__).parent / "schema" / "doctor-v1.json").read_text())


@pytest.fixture(autouse=True)
def _ensure_real_rules_registered(monkeypatch: pytest.MonkeyPatch) -> None:
    """Use real rules in an owned registry, restoring prior state afterward."""
    from ducklake_iceberg.doctor import registry

    monkeypatch.setattr(registry, "_REGISTRY", {})
    for rule in real_rules.RULES:
        registry.register(rule)


class FakeRule:
    def __init__(
        self,
        rule_id: str,
        *,
        severity: Severity = Severity.BLOCKER,
        emits: Sequence[Finding] | None = None,
        raises: Exception | None = None,
        requires: tuple[str, ...] = (),
    ) -> None:
        self.id = rule_id
        self.title = rule_id
        self.severity = severity
        self.remediation = f"fix {rule_id}"
        self.requires = requires
        self._emits = tuple(emits or ())
        self._raises = raises
        self.calls = 0

    def check(self, ctx: DoctorContext) -> Sequence[Finding]:
        self.calls += 1
        if self._raises is not None:
            raise self._raises
        return self._emits


def _finding(rule_id: str, severity: Severity) -> Finding:
    return Finding(
        rule_id=rule_id,
        severity=severity,
        subject="sales.orders",
        detail=f"{severity.json}",
        remediation="x",
    )


class SpyConnection:
    def __init__(self) -> None:
        self.commands: list[str] = []

    def execute(self, sql: str, *a: Any, **k: Any) -> SpyConnection:
        self.commands.append(sql)
        return self


@pytest.fixture
def ctx() -> DoctorContext:
    target = DoctorTarget(
        catalog="lake",
        metadata_catalog="lake_meta",
        schema_name="sales",
        table_name="orders",
        table_id=7,
        snapshot_id=42,
        schema_version=1,
    )
    return DoctorContext(con=SpyConnection(), target=target)  # type: ignore[arg-type]


# synthetic gate equivalent to inlined-data + 5 dependents
def _gate_graph(
    *, gate_emits: Sequence[Finding] | None = None, gate_raises: Exception | None = None
) -> list[FakeRule]:
    gate = FakeRule("inlined-data", emits=gate_emits, raises=gate_raises)
    deps = [
        FakeRule("partial-files", requires=("inlined-data",)),
        FakeRule("delete-parquet", requires=("inlined-data",)),
        FakeRule("delete-puffin", requires=("inlined-data",)),
        FakeRule("mapping-divergence", requires=("inlined-data",)),
        FakeRule("encryption-key", requires=("inlined-data",)),
    ]
    logical = [FakeRule("unsupported-type"), FakeRule("calendar-transform")]
    return [gate, *deps, *logical]


# --------------------------------------------------------------------------
# 1 - plan with the real rules
# --------------------------------------------------------------------------
def test_real_plan_matches_plan_order() -> None:
    plan = execution_plan(list(real_rules.RULES))
    assert len(plan) == 9
    assert tuple(r.id for r in plan) == PLAN_ORDER


# 2 - PLAN_ORDER is a topological order
def test_plan_order_is_topological() -> None:
    plan = execution_plan(list(real_rules.RULES))
    for rule in plan:
        for gate in rule.requires:
            assert plan.index_of(gate) < plan.index_of(rule.id)


# 3 - dependents_of the gate
def test_dependents_of_gate() -> None:
    plan = execution_plan(list(real_rules.RULES))
    assert set(plan.dependents_of("inlined-data")) == {
        "partial-files",
        "delete-parquet",
        "delete-puffin",
        "mapping-divergence",
        "encryption-key",
    }


# 4 - dependents_of the other rules is empty
@pytest.mark.parametrize(
    "rule_id", ["partial-files", "unsupported-type", "calendar-transform"]
)
def test_dependents_of_non_gate_empty(rule_id: str) -> None:
    plan = execution_plan(list(real_rules.RULES))
    assert plan.dependents_of(rule_id) == ()


# 5 - rule registered but absent from PLAN_ORDER (strict, via registry)
def test_extra_rule_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    from ducklake_iceberg.doctor import plan as plan_mod

    extra = FakeRule("intruder")
    monkeypatch.setattr(plan_mod, "all_rules", lambda: (*real_rules.RULES, extra))
    with pytest.raises(PlanError, match="outside the plan"):
        plan_mod.execution_plan()


# 6 - PLAN_ORDER with an id missing from the registry
def test_missing_rule_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    from ducklake_iceberg.doctor import plan as plan_mod

    incomplete = tuple(r for r in real_rules.RULES if r.id != "encryption-key")
    monkeypatch.setattr(plan_mod, "all_rules", lambda: incomplete)
    with pytest.raises(PlanError, match="missing from the registry"):
        plan_mod.execution_plan()


# 7 - requires pointing to a rule that appears later (forward edge)
def test_forward_edge_raises() -> None:
    # real ids (a subset of PLAN_ORDER) to take the declared path; inlined-data
    # requires partial-files, which comes after it -> not topological.
    bad_gate = FakeRule("inlined-data", requires=("partial-files",))
    dep = FakeRule("partial-files", requires=("inlined-data",))
    with pytest.raises(PlanError, match="not topological"):
        execution_plan([bad_gate, dep])


# 8 - cycle A->B->C->A (synthetic ids, generic path)
def test_cycle_raises() -> None:
    a = FakeRule("a", requires=("c",))
    b = FakeRule("b", requires=("a",))
    c = FakeRule("c", requires=("b",))
    with pytest.raises(PlanError, match="cycle"):
        execution_plan([a, b, c])


# 9 - dependents_of is a transitive closure (2 levels)
def test_dependents_of_transitive() -> None:
    a = FakeRule("a")
    b = FakeRule("b", requires=("a",))
    c = FakeRule("c", requires=("b",))
    plan = execution_plan([a, b, c])
    assert plan.dependents_of("a") == ("b", "c")


# 10 - gate with BLOCKER skips the 5 dependents
def test_gate_blocker_skips_dependents(ctx: DoctorContext) -> None:
    graph = _gate_graph(gate_emits=[_finding("inlined-data", Severity.BLOCKER)])
    report = run(ctx, graph)
    by_id = {r.rule_id: r for r in report.results}
    deps = (
        "partial-files",
        "delete-parquet",
        "delete-puffin",
        "mapping-divergence",
        "encryption-key",
    )
    for dep in deps:
        assert by_id[dep].status is RuleStatus.SKIPPED
        assert by_id[dep].skipped_by == ("inlined-data",)
        assert by_id[dep].skip_reason == "upstream_blocker"
    # check() of the 5 was not called
    for fake in graph:
        if fake.id in deps:
            assert fake.calls == 0
    # the logical layer runs
    assert by_id["unsupported-type"].status is RuleStatus.PASSED
    assert by_id["calendar-transform"].status is RuleStatus.PASSED


# 11 - gate with only INFO skips nothing
def test_gate_info_skips_nothing(ctx: DoctorContext) -> None:
    graph = _gate_graph(gate_emits=[_finding("inlined-data", Severity.INFO)])
    report = run(ctx, graph)
    assert not any(r.status is RuleStatus.SKIPPED for r in report.results)


# 12 - gate with only DEGRADED skips nothing
def test_gate_degraded_skips_nothing(ctx: DoctorContext) -> None:
    gate = FakeRule(
        "inlined-data",
        severity=Severity.DEGRADED,
        emits=[_finding("inlined-data", Severity.DEGRADED)],
    )
    rest = _gate_graph()[1:]
    report = run(ctx, [gate, *rest])
    assert not any(r.status is RuleStatus.SKIPPED for r in report.results)


# 13 - gate with ERROR skips with upstream_error and exit 1
def test_gate_error_skips_with_reason(ctx: DoctorContext) -> None:
    graph = _gate_graph(gate_raises=RuntimeError("x"))
    report = run(ctx, graph)
    by_id = {r.rule_id: r for r in report.results}
    assert by_id["partial-files"].status is RuleStatus.SKIPPED
    assert by_id["partial-files"].skip_reason == "upstream_error"
    assert report.exit_code() == 1


# 14 - unsupported-type blocks, gate passes -> nothing skipped
def test_logical_blocker_skips_nothing(ctx: DoctorContext) -> None:
    graph = _gate_graph()
    for fake in graph:
        if fake.id == "unsupported-type":
            fake._emits = (_finding("unsupported-type", Severity.BLOCKER),)
    report = run(ctx, graph)
    assert not any(r.status is RuleStatus.SKIPPED for r in report.results)
    assert report.exit_code() == 2


# 15 - partial-files blocks (it is nobody's gate) -> nothing skipped
def test_partial_files_not_a_gate(ctx: DoctorContext) -> None:
    graph = _gate_graph()
    for fake in graph:
        if fake.id == "partial-files":
            fake._emits = (_finding("partial-files", Severity.BLOCKER),)
    report = run(ctx, graph)
    assert not any(r.status is RuleStatus.SKIPPED for r in report.results)


# 16 - --json with short-circuit
def test_json_short_circuit(ctx: DoctorContext) -> None:
    graph = _gate_graph(gate_emits=[_finding("inlined-data", Severity.BLOCKER)])
    report = run(ctx, graph)
    payload: dict[str, Any] = report.to_json()
    validate(instance=payload, schema=_SCHEMA)
    assert len(payload["rules"]) == 8
    assert payload["summary"]["rules_skipped"] == 5
    assert payload["summary"]["short_circuited_by"] == ["inlined-data"]
    assert payload["summary"]["revalidation_required"] is True
    assert payload["revalidation"] is None
    assert [r["order"] for r in payload["rules"]] == list(range(8))


# 17 - --json without short-circuit
def test_json_no_short_circuit(ctx: DoctorContext) -> None:
    graph = _gate_graph()  # gate passes
    report = run(ctx, graph)
    payload: dict[str, Any] = report.to_json()
    validate(instance=payload, schema=_SCHEMA)
    assert payload["summary"]["short_circuited_by"] == []
    assert payload["summary"]["revalidation_required"] is False
    assert all(r["skip_reason"] is None for r in payload["rules"])


# 18 - select_plan pulls in the gate
def test_select_plan_pulls_gate() -> None:
    from ducklake_iceberg.doctor.plan import select_plan

    plan = select_plan(["partial-files"])
    ids = [r.id for r in plan]
    assert ids == ["inlined-data", "partial-files"]


# 22 - synthetic revalidation: a gate that always blocks -> revalidation_required
def test_revalidation_required_flag(ctx: DoctorContext) -> None:
    graph = _gate_graph(gate_emits=[_finding("inlined-data", Severity.BLOCKER)])
    report = run(ctx, graph)
    assert report.summary()["revalidation_required"] is True


# 23 - order determinism
def test_order_is_deterministic(ctx: DoctorContext) -> None:
    orders = []
    for _ in range(10):
        report = run(ctx, _gate_graph())
        orders.append(tuple(r.rule_id for r in report.results))
    assert len(set(orders)) == 1


# 24 - order regression: PLAN_ORDER starts with inlined-data
def test_plan_order_starts_with_inlined_data() -> None:
    # Verified at runtime: with inlined data, ducklake_data_file and
    # ducklake_list_files return empty (not an error); exporting would produce
    # a valid Iceberg table with no rows. That is why inlined-data runs first.
    assert PLAN_ORDER[0] == "inlined-data"


# extra - execution via a prebuilt ExecutionPlan (select_plan + run)
def test_run_accepts_prebuilt_plan(ctx: DoctorContext) -> None:
    plan = execution_plan(_gate_graph())
    assert isinstance(plan, ExecutionPlan)
    report = run(ctx, plan)
    assert len(report.results) == 8
