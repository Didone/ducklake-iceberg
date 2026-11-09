"""Tests for the rule framework (model, registry, plan, runner).

All of them use **synthetic** rules (fakes): the framework is tested without
touching the catalog. The real rules are tested separately. The connection is
a spy that records the commands issued (`BEGIN`/`COMMIT`) and the rule queries.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import duckdb
import pytest
from jsonschema import validate

from ducklake_iceberg.doctor import registry
from ducklake_iceberg.doctor.model import (
    DoctorContext,
    DoctorTarget,
    Finding,
    RuleStatus,
    Severity,
    quote_ident,
)
from ducklake_iceberg.doctor.runner import run

_SCHEMA = json.loads((Path(__file__).parent / "schema" / "doctor-v1.json").read_text())


# --------------------------------------------------------------------------
# fakes
# --------------------------------------------------------------------------
class FakeRule:
    """Configurable synthetic rule. Counts the calls to ``check``."""

    def __init__(
        self,
        rule_id: str,
        *,
        severity: Severity = Severity.BLOCKER,
        emits: Sequence[Finding] | None = None,
        raises: Exception | None = None,
        requires: tuple[str, ...] = (),
        title: str | None = None,
    ) -> None:
        self.id = rule_id
        self.title = title or rule_id
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


def _finding(
    rule_id: str, severity: Severity, subject: str = "sales.orders"
) -> Finding:
    return Finding(
        rule_id=rule_id,
        severity=severity,
        subject=subject,
        detail=f"{severity.json} on {subject}",
        remediation=f"fix {rule_id}",
        data={"n": 3},
    )


class SpyConnection:
    """Fake connection that records every SQL issued via ``execute``."""

    def __init__(self) -> None:
        self.commands: list[str] = []

    def execute(self, sql: str, *args: Any, **kwargs: Any) -> SpyConnection:
        self.commands.append(sql)
        return self

    def arrow(self) -> Any:  # pragma: no cover - not used by the fakes
        raise AssertionError("fake must not query the catalog")


@pytest.fixture
def target() -> DoctorTarget:
    return DoctorTarget(
        catalog="lake",
        metadata_catalog="lake_meta",
        schema_name="sales",
        table_name="orders",
        table_id=7,
        snapshot_id=42,
        schema_version=3,
    )


@pytest.fixture
def ctx(target: DoctorTarget) -> DoctorContext:
    spy = SpyConnection()
    # DoctorContext types `con` as duckdb.DuckDBPyConnection; the spy is
    # structurally compatible with what the runner uses (execute).
    return DoctorContext(con=spy, target=target)  # type: ignore[arg-type]


@pytest.fixture(autouse=True)
def _clean_registry() -> Any:
    registry._reset()
    yield
    registry._reset()


# --------------------------------------------------------------------------
# 1 - two rules without findings
# --------------------------------------------------------------------------
def test_all_pass(ctx: DoctorContext) -> None:
    rules = [FakeRule("a"), FakeRule("b")]
    report = run(ctx, rules)
    assert [r.status for r in report.results] == [
        RuleStatus.PASSED,
        RuleStatus.PASSED,
    ]
    assert report.exit_code() == 0


# --------------------------------------------------------------------------
# 2 - one rule emits a BLOCKER
# --------------------------------------------------------------------------
def test_single_blocker(ctx: DoctorContext) -> None:
    rule = FakeRule("a", emits=[_finding("a", Severity.BLOCKER)])
    report = run(ctx, [rule])
    (result,) = report.results
    assert result.status is RuleStatus.FAILED
    assert report.exit_code() == 2
    assert report.summary()["blocker"] == 1


# --------------------------------------------------------------------------
# 3 - DEGRADED + INFO, no blocker
# --------------------------------------------------------------------------
def test_degraded_and_info(ctx: DoctorContext) -> None:
    rules = [
        FakeRule(
            "deg",
            severity=Severity.DEGRADED,
            emits=[_finding("deg", Severity.DEGRADED)],
        ),
        FakeRule(
            "inf",
            severity=Severity.INFO,
            emits=[_finding("inf", Severity.INFO)],
        ),
    ]
    report = run(ctx, rules)
    assert report.exit_code() == 0
    assert report.summary()["worst_severity"] == "degraded"


# --------------------------------------------------------------------------
# 4 - a rule raises duckdb.Error; the others still run
# --------------------------------------------------------------------------
def test_rule_error_isolated(ctx: DoctorContext) -> None:
    boom = FakeRule("boom", raises=duckdb.Error("SQL failure"))
    ok = FakeRule("ok")
    report = run(ctx, [boom, ok])
    by_id = {r.rule_id: r for r in report.results}
    assert by_id["boom"].status is RuleStatus.ERROR
    assert by_id["boom"].error is not None
    assert "SQL failure" in by_id["boom"].error
    assert by_id["ok"].status is RuleStatus.PASSED
    assert ok.calls == 1
    assert report.exit_code() == 1


# --------------------------------------------------------------------------
# 5 - ERROR takes precedence over BLOCKER
# --------------------------------------------------------------------------
def test_error_precedes_blocker(ctx: DoctorContext) -> None:
    boom = FakeRule("boom", raises=RuntimeError("x"))
    blk = FakeRule("blk", emits=[_finding("blk", Severity.BLOCKER)])
    report = run(ctx, [boom, blk])
    assert report.has_error
    assert report.has_blocker
    assert report.exit_code() == 1


# --------------------------------------------------------------------------
# 6 - a finding above the declared severity becomes ERROR
# --------------------------------------------------------------------------
def test_finding_above_declared_is_error(ctx: DoctorContext) -> None:
    rule = FakeRule(
        "over",
        severity=Severity.DEGRADED,
        emits=[_finding("over", Severity.BLOCKER)],
    )
    report = run(ctx, [rule])
    (result,) = report.results
    assert result.status is RuleStatus.ERROR
    assert result.findings == ()  # does not enter the report
    assert report.exit_code() == 1


# --------------------------------------------------------------------------
# 7 - rule skipped by a gate: check() is not called
# --------------------------------------------------------------------------
def test_skipped_by_gate(ctx: DoctorContext) -> None:
    gate = FakeRule("gate", emits=[_finding("gate", Severity.BLOCKER)])
    dependent = FakeRule("dep", requires=("gate",))
    report = run(ctx, [gate, dependent])
    by_id = {r.rule_id: r for r in report.results}
    assert by_id["dep"].status is RuleStatus.SKIPPED
    assert by_id["dep"].skipped_by == ("gate",)
    assert by_id["dep"].skip_reason == "upstream_blocker"
    assert dependent.calls == 0


def test_skipped_by_error_gate(ctx: DoctorContext) -> None:
    gate = FakeRule("gate", raises=RuntimeError("x"))
    dependent = FakeRule("dep", requires=("gate",))
    report = run(ctx, [gate, dependent])
    by_id = {r.rule_id: r for r in report.results}
    assert by_id["dep"].status is RuleStatus.SKIPPED
    assert by_id["dep"].skip_reason == "upstream_error"
    assert dependent.calls == 0


# --------------------------------------------------------------------------
# 8 - --json serialization validates against the v1 schema
# --------------------------------------------------------------------------
def test_json_matches_schema(ctx: DoctorContext) -> None:
    gate = FakeRule(
        "gate",
        title="Gate",
        emits=[_finding("gate", Severity.BLOCKER)],
    )
    dependent = FakeRule("dep", requires=("gate",))
    degraded = FakeRule(
        "deg",
        severity=Severity.DEGRADED,
        emits=[_finding("deg", Severity.DEGRADED)],
    )
    report = run(ctx, [gate, dependent, degraded])
    payload: dict[str, Any] = report.to_json()
    validate(instance=payload, schema=_SCHEMA)
    rules_out: list[dict[str, Any]] = payload["rules"]
    assert len(rules_out) == 3
    assert payload["exit_code"] == report.exit_code()
    # order is ascending and equal to the index
    assert [r["order"] for r in rules_out] == [0, 1, 2]


# --------------------------------------------------------------------------
# 9 - --json determinism (ignoring generated_at and duration_ms)
# --------------------------------------------------------------------------
def test_json_is_deterministic(ctx: DoctorContext) -> None:
    def _normalized() -> str:
        rules = [
            FakeRule("gate", emits=[_finding("gate", Severity.BLOCKER)]),
            FakeRule("dep", requires=("gate",)),
        ]
        payload: dict[str, Any] = run(ctx, rules).to_json()
        payload.pop("generated_at")
        rules_out: list[dict[str, Any]] = payload["rules"]
        for rule in rules_out:
            rule.pop("duration_ms")
        return json.dumps(payload, sort_keys=True)

    assert _normalized() == _normalized()


# --------------------------------------------------------------------------
# 10 - single transaction: exactly one BEGIN and one COMMIT
# --------------------------------------------------------------------------
def test_single_transaction(target: DoctorTarget) -> None:
    spy = SpyConnection()
    ctx = DoctorContext(con=spy, target=target)  # type: ignore[arg-type]
    run(ctx, [FakeRule("a"), FakeRule("b")])
    begins = [c for c in spy.commands if c.startswith("BEGIN")]
    commits = [c for c in spy.commands if c == "COMMIT"]
    assert len(begins) == 1
    assert len(commits) == 1
    assert spy.commands[0].startswith("BEGIN")
    assert spy.commands[-1] == "COMMIT"


# --------------------------------------------------------------------------
# 11 - COMMIT despite a rule with ERROR; never ROLLBACK
# --------------------------------------------------------------------------
def test_commit_despite_rule_error(target: DoctorTarget) -> None:
    spy = SpyConnection()
    ctx = DoctorContext(con=spy, target=target)  # type: ignore[arg-type]
    run(ctx, [FakeRule("boom", raises=RuntimeError("x"))])
    assert "COMMIT" in spy.commands
    assert "ROLLBACK" not in spy.commands


# --------------------------------------------------------------------------
# 12 - register with a repeated id
# --------------------------------------------------------------------------
def test_register_duplicate_id() -> None:
    registry.register(FakeRule("dup"))
    with pytest.raises(RuntimeError, match="duplicate rule"):
        registry.register(FakeRule("dup"))


# --------------------------------------------------------------------------
# 13 - object without check passed to register
# --------------------------------------------------------------------------
def test_register_rejects_non_rule() -> None:
    class NoCheck:
        id = "x"
        title = "x"
        severity = Severity.INFO
        remediation = "x"
        requires: tuple[str, ...] = ()

    with pytest.raises(TypeError, match="check"):
        registry.register(NoCheck())  # type: ignore[arg-type]


def test_register_rejects_missing_attr() -> None:
    class Partial:
        id = "x"

        def check(self, ctx: DoctorContext) -> Sequence[Finding]:
            return ()

    with pytest.raises(TypeError, match="title"):
        registry.register(Partial())  # type: ignore[arg-type]


# --------------------------------------------------------------------------
# 14 - quote_ident
# --------------------------------------------------------------------------
@pytest.mark.parametrize("bad", ["lake_meta; DROP", "", "1x", '"quoted"', "a.b"])
def test_quote_ident_rejects(bad: str) -> None:
    with pytest.raises(ValueError, match="invalid identifier"):
        quote_ident(bad)


def test_quote_ident_accepts() -> None:
    assert quote_ident("lake_meta") == '"lake_meta"'
    assert quote_ident("_x9") == '"_x9"'


# --------------------------------------------------------------------------
# 16 - to_json is pure, parseable, and free of ANSI
# --------------------------------------------------------------------------
def test_json_is_pure(ctx: DoctorContext) -> None:
    report = run(ctx, [FakeRule("a")])
    text = json.dumps(report.to_json())
    reparsed = json.loads(text)
    assert "\x1b[" not in text
    assert reparsed["command"] == "doctor"


# --------------------------------------------------------------------------
# extra - dependents_of is a transitive closure
# --------------------------------------------------------------------------
def test_transitive_skip(ctx: DoctorContext) -> None:
    gate = FakeRule("g", emits=[_finding("g", Severity.BLOCKER)])
    mid = FakeRule("m", requires=("g",))
    leaf = FakeRule("l", requires=("m",))
    report = run(ctx, [gate, mid, leaf])
    by_id = {r.rule_id: r for r in report.results}
    assert by_id["m"].status is RuleStatus.SKIPPED
    assert by_id["l"].status is RuleStatus.SKIPPED
    assert mid.calls == 0
    assert leaf.calls == 0


# --------------------------------------------------------------------------
# plan - validation, order, and index
# --------------------------------------------------------------------------
def test_plan_rejects_orphan_requires() -> None:
    from ducklake_iceberg.doctor.plan import PlanError, execution_plan

    with pytest.raises(PlanError, match="nonexistent gate"):
        execution_plan([FakeRule("dep", requires=("absent",))])


def test_plan_rejects_cycle() -> None:
    from ducklake_iceberg.doctor.plan import PlanError, execution_plan

    a = FakeRule("a", requires=("b",))
    b = FakeRule("b", requires=("a",))
    with pytest.raises(PlanError, match="cycle"):
        execution_plan([a, b])


def test_plan_index_and_dependents() -> None:
    from ducklake_iceberg.doctor.plan import execution_plan

    gate = FakeRule("g")
    mid = FakeRule("m", requires=("g",))
    leaf = FakeRule("l", requires=("m",))
    plan = execution_plan([gate, mid, leaf])
    assert plan.index_of("g") == 0
    assert plan.index_of("l") == 2
    assert plan.dependents_of("g") == ("m", "l")
    assert plan.dependents_of("l") == ()
    with pytest.raises(KeyError):
        plan.index_of("nope")


def test_plan_topological_order_respects_requires() -> None:
    from ducklake_iceberg.doctor.plan import execution_plan

    # 'dep' is registered before the gate, but must run after it.
    plan = execution_plan([FakeRule("dep", requires=("gate",)), FakeRule("gate")])
    assert [r.id for r in plan.order] == ["gate", "dep"]


# --------------------------------------------------------------------------
# sql - visibility predicate
# --------------------------------------------------------------------------
def test_visible_predicate() -> None:
    from ducklake_iceberg.doctor.sql import visible

    bare = visible()
    assert "$snapshot_id >= begin_snapshot" in bare
    assert "end_snapshot IS NULL OR $snapshot_id < end_snapshot" in bare

    aliased = visible("f")
    assert "f.begin_snapshot" in aliased
    assert "f.end_snapshot" in aliased


# --------------------------------------------------------------------------
# registry - get / all_rules
# --------------------------------------------------------------------------
def test_registry_get_and_all() -> None:
    rule = FakeRule("x")
    registry.register(rule)
    assert registry.get("x") is rule
    assert registry.all_rules() == (rule,)


# --------------------------------------------------------------------------
# report - max_severity per rule
# --------------------------------------------------------------------------
def test_rule_result_max_severity(ctx: DoctorContext) -> None:
    rule = FakeRule(
        "multi",
        emits=[
            _finding("multi", Severity.INFO),
            _finding("multi", Severity.BLOCKER),
        ],
    )
    report = run(ctx, [rule])
    (result,) = report.results
    assert result.max_severity is Severity.BLOCKER


def test_rule_result_max_severity_none(ctx: DoctorContext) -> None:
    report = run(ctx, [FakeRule("empty")])
    (result,) = report.results
    assert result.max_severity is None
