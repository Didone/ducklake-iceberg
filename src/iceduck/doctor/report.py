"""Per-rule result and aggregated `doctor` report.

`DoctorReport` is the runner's product: it holds the result of each rule in
plan order, derives the exit code, and serializes the version 1 `--json`
document.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from iceduck.doctor.model import (
    DoctorTarget,
    Finding,
    JsonValue,
    RuleStatus,
    Severity,
)

#: Version of the `--json` contract. An incompatible change requires a bump.
JSON_VERSION: int = 1

#: Gates whose blocking has an automatic remediation (today only
#: `inlined-data`, via `flush`). They trigger `revalidation_required` when
#: they skip dependents.
AUTO_REMEDIABLE_GATES: frozenset[str] = frozenset({"inlined-data"})


@dataclass(frozen=True, slots=True)
class Revalidation:
    """Marks the post-flush revalidation round."""

    after: str  # "flush"
    previous_snapshot_id: int
    previous_exit_code: int

    def to_json(self) -> dict[str, JsonValue]:
        return {
            "after": self.after,
            "previous_snapshot_id": self.previous_snapshot_id,
            "previous_exit_code": self.previous_exit_code,
        }


@dataclass(frozen=True, slots=True)
class RuleResult:
    rule_id: str
    status: RuleStatus
    title: str = ""
    declared_severity: Severity = Severity.INFO
    requires: tuple[str, ...] = ()
    order: int = 0
    findings: tuple[Finding, ...] = ()
    skipped_by: tuple[str, ...] = ()
    skip_reason: str | None = None
    error: str | None = None
    duration_ms: int = 0

    @property
    def max_severity(self) -> Severity | None:
        return max((f.severity for f in self.findings), default=None)

    def to_json(self) -> dict[str, JsonValue]:
        return {
            "id": self.rule_id,
            "title": self.title,
            "status": str(self.status),
            "declared_severity": self.declared_severity.json,
            "requires": list(self.requires),
            "order": self.order,
            "duration_ms": self.duration_ms,
            "skipped_by": list(self.skipped_by),
            "skip_reason": self.skip_reason,
            "error": self.error,
            "findings": [f.to_json() for f in self.findings],
        }


@dataclass(frozen=True, slots=True)
class DoctorReport:
    target: DoctorTarget
    format_version: int
    generated_at: str  # ISO-8601 UTC
    results: tuple[RuleResult, ...] = field(default_factory=tuple)
    revalidation: Revalidation | None = None

    @property
    def has_error(self) -> bool:
        return any(r.status is RuleStatus.ERROR for r in self.results)

    @property
    def has_blocker(self) -> bool:
        return any(
            f.severity is Severity.BLOCKER for r in self.results for f in r.findings
        )

    def exit_code(self) -> int:
        if self.has_error:
            return 1
        if self.has_blocker:
            return 2
        return 0

    # -- summary counters ---------------------------------------------

    def _severity_count(self, severity: Severity) -> int:
        return sum(
            1 for r in self.results for f in r.findings if f.severity is severity
        )

    def _worst_severity(self) -> str | None:
        worst = max(
            (f.severity for r in self.results for f in r.findings),
            default=None,
        )
        return worst.json if worst is not None else None

    def _short_circuited_by(self) -> list[str]:
        """Ids of the gates that actually skipped someone in this run."""
        gates: list[str] = []
        for result in self.results:
            if result.status is RuleStatus.SKIPPED:
                for gate in result.skipped_by:
                    if gate not in gates:
                        gates.append(gate)
        return gates

    def _revalidation_required(self) -> bool:
        """True if a rule was SKIPPED by a gate that has an automatic remediation."""
        return any(
            r.status is RuleStatus.SKIPPED
            and r.skip_reason == "upstream_blocker"
            and any(gate in AUTO_REMEDIABLE_GATES for gate in r.skipped_by)
            for r in self.results
        )

    def summary(self) -> dict[str, JsonValue]:
        run = sum(
            1
            for r in self.results
            if r.status in (RuleStatus.PASSED, RuleStatus.FAILED)
        )
        skipped = sum(1 for r in self.results if r.status is RuleStatus.SKIPPED)
        errored = sum(1 for r in self.results if r.status is RuleStatus.ERROR)
        short_circuited = self._short_circuited_by()
        return {
            "rules_total": len(self.results),
            "rules_run": run,
            "rules_skipped": skipped,
            "rules_errored": errored,
            "blocker": self._severity_count(Severity.BLOCKER),
            "degraded": self._severity_count(Severity.DEGRADED),
            "info": self._severity_count(Severity.INFO),
            "worst_severity": self._worst_severity(),
            "short_circuited_by": list(short_circuited),
            "revalidation_required": self._revalidation_required(),
        }

    def to_json(self) -> dict[str, JsonValue]:
        return {
            "version": JSON_VERSION,
            "command": "doctor",
            "generated_at": self.generated_at,
            "target": {
                "catalog": self.target.catalog,
                "metadata_catalog": self.target.metadata_catalog,
                "schema": self.target.schema_name,
                "table": self.target.table_name,
                "table_id": self.target.table_id,
                "snapshot_id": self.target.snapshot_id,
                "schema_version": self.target.schema_version,
            },
            "iceberg": {"format_version": self.format_version},
            "summary": self.summary(),
            "exit_code": self.exit_code(),
            "revalidation": (
                self.revalidation.to_json() if self.revalidation is not None else None
            ),
            "rules": [r.to_json() for r in self.results],
        }
