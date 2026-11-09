"""Declarative registry of the `doctor` rules.

Loading the `ducklake_iceberg.doctor.rules` package populates the registry as a side
effect of the module-level `register(...)` calls. Protocol validation is
structural and explicit (`Rule` is not `runtime_checkable`).
"""

from __future__ import annotations

from ducklake_iceberg.doctor.model import Rule, Severity

_REGISTRY: dict[str, Rule] = {}


def register(rule: Rule) -> Rule:
    _validate(rule)
    if rule.id in _REGISTRY:
        raise RuntimeError(f"duplicate rule: {rule.id!r}")
    _REGISTRY[rule.id] = rule
    return rule


def _validate(rule: Rule) -> None:
    missing = [
        attr
        for attr in ("id", "title", "severity", "remediation", "requires")
        if not hasattr(rule, attr)
    ]
    if missing or not callable(getattr(rule, "check", None)):
        raise TypeError(
            f"{rule!r} does not satisfy Rule: missing {missing or ['check']}"
        )
    if not isinstance(rule.severity, Severity):
        raise TypeError(f"{rule.id!r}: severity must be a Severity")


def all_rules() -> tuple[Rule, ...]:
    return tuple(_REGISTRY.values())


def get(rule_id: str) -> Rule:
    return _REGISTRY[rule_id]


def _reset() -> None:
    """Clear the registry. For tests only."""
    _REGISTRY.clear()
