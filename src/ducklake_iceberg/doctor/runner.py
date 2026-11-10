"""Rule runner for `doctor`.

Core requirement: **a single read transaction for all rules**. Without it, a
concurrent flush, compaction, or snapshot expiration would make two rules see
different catalogs, and the report would describe a state that never existed.

Non-negotiable invariants:

1. Per-rule isolation: an exception becomes `RuleStatus.ERROR` and the loop
   continues.
2. The transaction always ends with `COMMIT` (it is a read; a `ROLLBACK` on a
   rule error brings no benefit). An exception only escapes if the connection
   itself drops.
3. Skipped rules do not call `check()`.
4. The order comes from the plan, not from the registry.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import replace
from datetime import UTC, datetime

import duckdb

from ducklake_iceberg.doctor.model import (
    DoctorContext,
    Finding,
    Rule,
    RuleStatus,
    Severity,
)
from ducklake_iceberg.doctor.plan import ExecutionPlan, execution_plan
from ducklake_iceberg.doctor.report import DoctorReport, Revalidation, RuleResult

LOG = logging.getLogger("ducklake_iceberg.doctor")


@contextmanager
def read_transaction(con: duckdb.DuckDBPyConnection) -> Iterator[None]:
    """`BEGIN ... COMMIT` on the raw connection.

    The runner isolates rule exceptions before they get back to the `with`, so
    `COMMIT` runs in normal execution. A connection failure lets the exception
    escape without `COMMIT`; the partial report is not presented.
    """
    con.execute("BEGIN TRANSACTION")
    yield
    con.execute("COMMIT")


def run(
    ctx: DoctorContext,
    rules: Sequence[Rule] | ExecutionPlan | None = None,
    *,
    revalidation: Revalidation | None = None,
    manage_transaction: bool = True,
) -> DoctorReport:
    """Run the rule plan and return the report.

    `manage_transaction=True` (default) opens `BEGIN`/`COMMIT` on the
    connection. When the caller already holds a read transaction (the CLI
    opens a single one for all tables of `--all-tables`), passing `False`
    avoids a nested `BEGIN`; the guarantee of "one transaction for all rules"
    still holds, now under the caller's control.
    """
    plan = rules if isinstance(rules, ExecutionPlan) else execution_plan(rules)
    ctx = replace(ctx, _query_cache={})

    scope: AbstractContextManager[object] = (
        read_transaction(ctx.con) if manage_transaction else nullcontext()
    )
    with scope:
        results = _execute_plan(plan, ctx)

    return DoctorReport(
        target=ctx.target,
        format_version=ctx.format_version,
        generated_at=datetime.now(UTC).isoformat(timespec="seconds"),
        results=tuple(results),
        revalidation=revalidation,
    )


def _execute_plan(plan: ExecutionPlan, ctx: DoctorContext) -> list[RuleResult]:
    results: list[RuleResult] = []
    blocked_by: dict[str, tuple[str, ...]] = {}
    skip_reason: dict[str, str] = {}

    for order, rule in enumerate(plan):
        gates = blocked_by.get(rule.id, ())
        if gates:
            reason = skip_reason.get(rule.id, "upstream_blocker")
            results.append(_skipped(rule, order, gates, reason))
            continue
        result = _run_one(rule, ctx, order)
        results.append(result)
        if _blocks(result):
            reason = (
                "upstream_error"
                if result.status is RuleStatus.ERROR
                else "upstream_blocker"
            )
            for dependent in plan.dependents_of(rule.id):
                blocked_by[dependent] = (
                    *blocked_by.get(dependent, ()),
                    rule.id,
                )
                skip_reason.setdefault(dependent, reason)
    return results


def _skipped(rule: Rule, order: int, gates: tuple[str, ...], reason: str) -> RuleResult:
    return RuleResult(
        rule_id=rule.id,
        status=RuleStatus.SKIPPED,
        title=rule.title,
        declared_severity=rule.severity,
        requires=tuple(rule.requires),
        order=order,
        skipped_by=gates,
        skip_reason=reason,
    )


def _blocks(result: RuleResult) -> bool:
    """A result blocks its dependents if it errored or emitted a BLOCKER."""
    if result.status is RuleStatus.ERROR:
        return True
    return any(f.severity is Severity.BLOCKER for f in result.findings)


def _run_one(rule: Rule, ctx: DoctorContext, order: int) -> RuleResult:
    started = time.perf_counter()
    try:
        raw = tuple(rule.check(ctx))
    except Exception as exc:
        return _error(rule, order, f"{type(exc).__name__}: {exc}", started)

    findings = tuple(_normalize(rule, f) for f in raw)
    violations = [f for f in findings if f.severity > rule.severity]
    if violations:
        return _error(
            rule,
            order,
            (
                f"rule emitted a severity above the declared one "
                f"({violations[0].severity.json} > {rule.severity.json})"
            ),
            started,
        )
    return RuleResult(
        rule_id=rule.id,
        status=RuleStatus.FAILED if findings else RuleStatus.PASSED,
        title=rule.title,
        declared_severity=rule.severity,
        requires=tuple(rule.requires),
        order=order,
        findings=findings,
        duration_ms=_elapsed_ms(started),
    )


def _normalize(rule: Rule, finding: Finding) -> Finding:
    """Ensure the finding carries the `rule_id` of the rule that emitted it."""
    if finding.rule_id == rule.id:
        return finding
    LOG.warning(
        "rule %r emitted a finding with rule_id %r; normalizing",
        rule.id,
        finding.rule_id,
    )
    return Finding(
        rule_id=rule.id,
        severity=finding.severity,
        subject=finding.subject,
        detail=finding.detail,
        remediation=finding.remediation,
        data=finding.data,
    )


def _error(rule: Rule, order: int, message: str, started: float) -> RuleResult:
    return RuleResult(
        rule_id=rule.id,
        status=RuleStatus.ERROR,
        title=rule.title,
        declared_severity=rule.severity,
        requires=tuple(rule.requires),
        order=order,
        error=message,
        duration_ms=_elapsed_ms(started),
    )


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)
