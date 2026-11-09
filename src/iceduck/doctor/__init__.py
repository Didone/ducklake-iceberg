"""Pre-flight compatibility check for DuckLake -> Iceberg v2.

Provides the framework: vocabulary, the `Rule` protocol, registry, execution
plan, single-transaction runner, report, and rendering.
"""

from __future__ import annotations

from iceduck.doctor.model import (
    Capability,
    DoctorContext,
    DoctorTarget,
    Finding,
    Rule,
    RuleStatus,
    Severity,
    quote_ident,
)
from iceduck.doctor.plan import (
    PLAN_ORDER,
    ExecutionPlan,
    PlanError,
    execution_plan,
    select_plan,
)
from iceduck.doctor.registry import all_rules, get, register
from iceduck.doctor.render import render
from iceduck.doctor.report import DoctorReport, Revalidation, RuleResult
from iceduck.doctor.runner import read_transaction, run

__all__ = [
    "PLAN_ORDER",
    "Capability",
    "DoctorContext",
    "DoctorReport",
    "DoctorTarget",
    "ExecutionPlan",
    "Finding",
    "PlanError",
    "Revalidation",
    "Rule",
    "RuleResult",
    "RuleStatus",
    "Severity",
    "all_rules",
    "execution_plan",
    "get",
    "quote_ident",
    "read_transaction",
    "register",
    "render",
    "run",
    "select_plan",
]
