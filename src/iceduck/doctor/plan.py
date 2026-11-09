"""Execution plan of the rules.

The framework needs a deterministic order and a map of dependents to
propagate `SKIPPED`. For the real rules, the order is **declared** in
`PLAN_ORDER` and validated as a topological ordering of the graph; it is not
derived by `sorted()`, because orchestrators compare reports across runs and
machines.

For arbitrary rule sets (synthetic rules in tests, whose ids are not in
`PLAN_ORDER`), the order is derived topologically from the `requires` edges,
with ties broken by registration order.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass

from iceduck.doctor.model import Rule
from iceduck.doctor.registry import all_rules

#: Declared order of the real rules. The `inlined-data` gate comes before any
#: file enumeration; the logical layer closes the report.
PLAN_ORDER: tuple[str, ...] = (
    "inlined-data",
    "partial-files",
    "delete-parquet",
    "delete-puffin",
    "mapping-divergence",
    "encryption-key",
    "unsupported-type",
    "calendar-transform",
    "parquet-compression",
)


class PlanError(RuntimeError):
    """Cycle, orphan `requires`, non-topological order, or rule outside the plan."""


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    order: tuple[Rule, ...]  # final topological order
    dependents: dict[str, tuple[str, ...]]  # gate -> direct dependents

    def __iter__(self) -> Iterator[Rule]:
        return iter(self.order)

    def __len__(self) -> int:
        return len(self.order)

    def index_of(self, rule_id: str) -> int:
        for i, rule in enumerate(self.order):
            if rule.id == rule_id:
                return i
        raise KeyError(rule_id)

    def dependents_of(self, rule_id: str) -> tuple[str, ...]:
        """Transitive closure of the dependents of ``rule_id``, in plan order."""
        seen: set[str] = set()
        stack = list(self.dependents.get(rule_id, ()))
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            stack.extend(self.dependents.get(current, ()))
        return tuple(r.id for r in self.order if r.id in seen)


def execution_plan(rules: Sequence[Rule] | None = None) -> ExecutionPlan:
    """Order, validate, and freeze the plan. Raises on an invalid graph.

    Real path (no `rules`, or all ids in `PLAN_ORDER`): declared order,
    validated as topological. Synthetic path (ids outside `PLAN_ORDER`):
    generic topological ordering.
    """
    use_registry = rules is None
    source = tuple(rules if rules is not None else all_rules())
    registry = {r.id: r for r in source}

    for rule in source:
        for gate in rule.requires:
            if gate not in registry:
                raise PlanError(
                    f"rule {rule.id!r} depends on a nonexistent gate: {gate!r}"
                )

    ids = set(registry)
    if use_registry or ids <= set(PLAN_ORDER):
        order = _declared_order(registry, strict=use_registry)
    else:
        order = _topological_order(source, registry)

    dependents: dict[str, list[str]] = {r.id: [] for r in order}
    for rule in order:
        for gate in rule.requires:
            dependents[gate].append(rule.id)

    return ExecutionPlan(
        order=order,
        dependents={gate: tuple(deps) for gate, deps in dependents.items()},
    )


def _declared_order(registry: dict[str, Rule], *, strict: bool) -> tuple[Rule, ...]:
    """Order by `PLAN_ORDER`, validating that it is a topological order.

    `strict=True` (real registry) requires exact coverage: every registered
    rule must be in `PLAN_ORDER` and vice versa. `strict=False` admits a
    subset of `PLAN_ORDER` (a partial selection of known rules).
    """
    if strict:
        missing = [rid for rid in PLAN_ORDER if rid not in registry]
        if missing:
            raise PlanError(f"plan rules missing from the registry: {missing}")
        extra = sorted(set(registry) - set(PLAN_ORDER))
        if extra:
            raise PlanError(f"registered rules outside the plan: {extra}")

    present = [rid for rid in PLAN_ORDER if rid in registry]
    seen: set[str] = set()
    for rule_id in present:
        for gate in registry[rule_id].requires:
            if gate in registry and gate not in seen:
                raise PlanError(
                    f"{rule_id} declares requires={gate!r}, which appears after "
                    f"it in PLAN_ORDER: the order is not topological"
                )
        seen.add(rule_id)
    return tuple(registry[rid] for rid in present)


def select_plan(rule_ids: Sequence[str]) -> ExecutionPlan:
    """Plan for a subset of real rules (`--rule`), pulling in the gates.

    If a selected rule declares `requires`, its gates are added to the plan
    unconditionally, so selecting `partial-files` alone is impossible by
    construction. The result preserves the relative order of `PLAN_ORDER`.
    """
    registry = {r.id: r for r in all_rules()}
    unknown = [rid for rid in rule_ids if rid not in registry]
    if unknown:
        raise PlanError(f"unknown rule(s): {unknown}")

    wanted: set[str] = set()
    pending = list(rule_ids)
    while pending:
        rid = pending.pop()
        if rid in wanted:
            continue
        wanted.add(rid)
        pending.extend(registry[rid].requires)

    subset = [registry[rid] for rid in PLAN_ORDER if rid in wanted]
    return execution_plan(subset)


def _topological_order(
    source: Sequence[Rule], registry: dict[str, Rule]
) -> tuple[Rule, ...]:
    """Kahn's algorithm with ties broken by registration order (deterministic)."""
    indegree = {r.id: len(set(r.requires)) for r in source}
    position = {r.id: i for i, r in enumerate(source)}

    ready = sorted(
        (rid for rid, deg in indegree.items() if deg == 0),
        key=lambda rid: position[rid],
    )
    ordered: list[Rule] = []
    while ready:
        current = ready.pop(0)
        ordered.append(registry[current])
        for rule in source:
            if current in rule.requires:
                indegree[rule.id] -= 1
                if indegree[rule.id] == 0:
                    ready.append(rule.id)
                    ready.sort(key=lambda rid: position[rid])

    if len(ordered) != len(source):
        remaining = sorted(set(registry) - {r.id for r in ordered})
        raise PlanError(f"cycle in the rule graph: {remaining}")
    return tuple(ordered)
