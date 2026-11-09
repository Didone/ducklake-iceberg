"""`rich` rendering of the `doctor` report.

Color comes only from `rich` styles (honors `NO_COLOR` and non-TTY terminals);
ASCII-safe glyphs are used with `--no-unicode` or on a non-interactive
terminal. The `--json` document is the job of `report.to_json`; this module
only produces the human-readable output.
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass

from rich.console import Console

from iceduck.doctor.model import RuleStatus, Severity
from iceduck.doctor.report import DoctorReport, RuleResult

_WRAP = 80


@dataclass(frozen=True, slots=True)
class _Glyphs:
    blocker: str
    degraded: str
    info: str
    passed: str
    skipped: str
    error: str


_UNICODE = _Glyphs(
    blocker="✗",
    degraded="!",
    info="i",
    passed="✓",
    skipped="⊘",
    error="⚠",
)
_ASCII = _Glyphs(
    blocker="x",
    degraded="!",
    info="i",
    passed="v",
    skipped="o",
    error="!",
)

_SEVERITY_STYLE = {
    Severity.BLOCKER: "bold red",
    Severity.DEGRADED: "yellow",
    Severity.INFO: "cyan",
}
_SEVERITY_LABEL = {
    Severity.BLOCKER: "BLOCKER",
    Severity.DEGRADED: "DEGRADED",
    Severity.INFO: "INFO",
}


def render(report: DoctorReport, *, console: Console | None = None) -> None:
    console = console or Console()
    use_unicode = console.is_terminal
    glyphs = _UNICODE if use_unicode else _ASCII

    target = report.target
    console.print(
        f"\n Pre-flight · {target.qualified_name} · snapshot "
        f"{target.snapshot_id} · Iceberg v{report.format_version}\n"
    )

    for result in report.results:
        _render_result(console, result, glyphs)

    console.print(f"\n {_footer(report)}\n")

    paragraph = _skip_paragraph(report)
    if paragraph:
        console.print(f" [dim]{paragraph}[/dim]\n")


def _render_result(console: Console, result: RuleResult, glyphs: _Glyphs) -> None:
    if result.status is RuleStatus.SKIPPED:
        by = ", ".join(result.skipped_by) or "—"
        console.print(
            f"  [dim]{glyphs.skipped} {result.rule_id:<20} SKIPPED     "
            f"skipped by {by}[/dim]"
        )
        return
    if result.status is RuleStatus.ERROR:
        console.print(
            f"  [bold magenta]{glyphs.error} {result.rule_id:<20} ERROR       "
            f"{result.error or ''}[/bold magenta]"
        )
        return
    if result.status is RuleStatus.PASSED:
        console.print(f"  [green]{glyphs.passed} {result.rule_id:<20} OK[/green]")
        return
    # FAILED: one block per finding.
    for finding in result.findings:
        style = _SEVERITY_STYLE[finding.severity]
        glyph = _glyph_for(finding.severity, glyphs)
        label = _SEVERITY_LABEL[finding.severity]
        console.print(
            f"  [{style}]{glyph} {result.rule_id:<20} {label:<11} "
            f"{finding.subject}[/{style}]"
        )
        for line in textwrap.wrap(finding.detail, _WRAP):
            console.print(f"      {line}")
        console.print(f"      [dim]→ {finding.remediation}[/dim]")


def _glyph_for(severity: Severity, glyphs: _Glyphs) -> str:
    return {
        Severity.BLOCKER: glyphs.blocker,
        Severity.DEGRADED: glyphs.degraded,
        Severity.INFO: glyphs.info,
    }[severity]


def _footer(report: DoctorReport) -> str:
    summary = report.summary()
    parts = [
        f"{summary['blocker']} blocker",
        f"{summary['degraded']} degraded",
        f"{summary['info']} info",
    ]
    skipped = summary["rules_skipped"]
    if skipped:
        parts.append(f"{skipped} skipped")
    errored = summary["rules_errored"]
    if errored:
        parts.append(f"{errored} error(s)")
    parts.append(f"exit {report.exit_code()}")
    return " · ".join(parts)


def _skip_paragraph(report: DoctorReport) -> str:
    """Mandatory paragraph whenever a rule was skipped.

    Without it, "N skipped" reads as "N fewer things to worry about". The text
    states the consequence: `status: skipped` is not a pass.
    """
    skipped = [r for r in report.results if r.status is RuleStatus.SKIPPED]
    if not skipped:
        return ""
    gates: list[str] = []
    for result in skipped:
        for gate in result.skipped_by:
            if gate not in gates:
                gates.append(gate)
    if "inlined-data" in gates:
        return (
            f"{len(skipped)} rules were not evaluated: with inlined data, the "
            "catalog file tables are empty and any verdict about them would be "
            "false. Run `iceduck checkpoint` and repeat the pre-flight check."
        )
    return (
        f"{len(skipped)} rules were not evaluated (skipped by "
        f"{', '.join(gates)}); 'skipped' is not a pass. Resolve the gate and "
        "repeat the pre-flight check."
    )
