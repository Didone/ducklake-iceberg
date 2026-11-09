from __future__ import annotations

import dataclasses
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from rich.console import Console
from rich.table import Table

from ducklake_iceberg.errors import ExitCode
from ducklake_iceberg.obs.redaction import scrub_mapping

ENVELOPE_VERSION: int = 1
#: versions accepted by ``--json-version``. When ``ENVELOPE_VERSION`` is
#: bumped, the previous one stays here and in ``DEPRECATED_ENVELOPE_VERSIONS``
#: for at least one *minor* release.
SUPPORTED_ENVELOPE_VERSIONS: frozenset[int] = frozenset({ENVELOPE_VERSION})
#: versions still accepted but that emit a ``warning``-level ``problem`` with
#: ``code="envelope_version_deprecated"``
DEPRECATED_ENVELOPE_VERSIONS: frozenset[int] = frozenset()

Status = Literal["ok", "error", "blocked", "diverged", "nothing-to-do"]

STATUS_BY_EXIT: dict[ExitCode, Status] = {
    ExitCode.SUCCESS: "ok",
    ExitCode.ERROR: "error",
    ExitCode.BLOCKED: "blocked",
    ExitCode.DIVERGED: "diverged",
    ExitCode.NOTHING_TO_DO: "nothing-to-do",
}

#: Increasing severity for --all-tables: 0 < 4 < 3 < 2 < 1.
#: An error dominates; failing that, a blocker; `4` only prevails if
#: everything is "nothing to do".
_SEVERITY: dict[ExitCode, int] = {
    ExitCode.SUCCESS: 0,
    ExitCode.NOTHING_TO_DO: 1,
    ExitCode.DIVERGED: 2,
    ExitCode.BLOCKED: 3,
    ExitCode.ERROR: 4,
}


def worse_exit(a: ExitCode, b: ExitCode) -> ExitCode:
    """Return the more severe of two exit codes."""
    return a if _SEVERITY[a] >= _SEVERITY[b] else b


ProblemLevel = Literal["error", "warning"]


@dataclass(slots=True)
class Problem:
    code: str  # "table_not_found", "profile_not_found", ...
    message: str
    hint: str | None = None
    table: str | None = None
    #: severity (optional envelope v1 key): ``warning`` is not a failure
    #: (e.g. ``workers_capped``); ``None`` omits the key and counts as an error.
    level: ProblemLevel | None = None

    def to_json(self) -> dict[str, Any]:
        payload = dataclasses.asdict(self)
        if self.level is None:
            del payload["level"]
        return payload


@dataclass(slots=True)
class Envelope:
    command: str
    dry_run: bool
    version: int = ENVELOPE_VERSION
    status: Status = "ok"
    exit_code: int = int(ExitCode.SUCCESS)
    started_at: datetime = field(default_factory=lambda: datetime.now().astimezone())
    duration_ms: int = 0
    config: dict[str, Any] | None = None
    results: list[dict[str, Any]] = field(default_factory=list)
    problems: list[Problem] = field(default_factory=list)
    #: phases and counters (``DUCKLAKE_ICEBERG_TIMINGS=1``); absent from the JSON
    #: when None
    timings: dict[str, Any] | None = None

    def set_exit(self, code: ExitCode) -> None:
        """Set `exit_code` and the derived `status` consistently."""
        self.exit_code = int(code)
        self.status = STATUS_BY_EXIT[code]

    def to_json(self) -> str:
        payload = scrub_mapping(
            {
                "version": self.version,
                "command": self.command,
                "status": self.status,
                "exit_code": self.exit_code,
                "dry_run": self.dry_run,
                "started_at": self.started_at.isoformat(),
                "duration_ms": self.duration_ms,
                "config": self.config,
                "results": self.results,
                "problems": [p.to_json() for p in self.problems],
            }
        )
        if self.timings is not None:
            payload["timings"] = self.timings
        return json.dumps(payload, ensure_ascii=False, default=str)


def emit(envelope: Envelope, *, json_out: bool) -> None:
    """Print the envelope to stdout (JSON) or delegate to the rich renderer."""
    if json_out:
        # stdout carries ONLY the envelope.
        sys.stdout.write(envelope.to_json() + "\n")
        return
    _render(envelope)


def _render(envelope: Envelope) -> None:
    console = Console()
    envelope.results = scrub_mapping(envelope.results)
    if envelope.command == "doctor" and envelope.results:
        if len(envelope.results) == 1:
            render_doctor(envelope.results[0])
        else:
            _render_doctor_table(envelope.results)
    elif envelope.command == "checkpoint" and envelope.results:
        from ducklake_iceberg.checkpoint.render import render_checkpoint

        render_checkpoint(envelope.results[0])
    elif envelope.command == "sync" and envelope.results:
        render_sync(envelope, console=console)
    for problem in envelope.problems:
        _render_problem(console, problem)


def _render_problem(console: Console, problem: Problem) -> None:
    where = f" ({problem.table})" if problem.table else ""
    safe_message = scrub_mapping(problem.message)
    if problem.level == "warning":
        console.print(f"[yellow]warning{where}:[/yellow] {safe_message}")
    else:
        console.print(f"[red]error{where}:[/red] {safe_message}")
    if problem.hint:
        console.print(f"  [dim]{scrub_mapping(problem.hint)}[/dim]")


def render_doctor(result: dict[str, Any]) -> None:
    """`rich` table on stdout (human mode) for a single table."""
    console = Console()
    identity = result.get("identity", {})
    snapshot = result.get("snapshot", {})
    env = result.get("environment", {})
    aws = result.get("aws", {})
    table_name = result.get("table", "?")

    grid = Table(
        title=f"ducklake-iceberg doctor · {table_name}",
        show_header=False,
        title_justify="left",
    )
    grid.add_column(style="bold")
    grid.add_column()

    grid.add_row("[cyan]Identity[/cyan]", "")
    grid.add_row("  table_id", str(identity.get("table_id", "-")))
    grid.add_row("  table_uuid", str(identity.get("table_uuid", "-")))
    grid.add_row(
        "  schema_id / ver",
        f"{identity.get('schema_id', '-')} / {identity.get('schema_version', '-')}",
    )
    grid.add_row("[cyan]Snapshot[/cyan]", "")
    grid.add_row("  snapshot_id", str(snapshot.get("snapshot_id", "-")))
    grid.add_row("  snapshot_time", str(snapshot.get("snapshot_time", "-")))
    grid.add_row("  schema_version", str(snapshot.get("schema_version", "-")))
    grid.add_row("[cyan]Environment[/cyan]", "")
    ro = "read_only" if env.get("read_only") else "read_write"
    grid.add_row("  duckdb", f"{env.get('duckdb_version', '-')}  ({ro})")
    exts = ", ".join(k for k, v in env.get("extensions", {}).items() if v)
    grid.add_row("  extensions", exts or "-")
    grid.add_row("  attached", ", ".join(env.get("attached", [])) or "-")
    grid.add_row("[cyan]AWS[/cyan]", "")
    grid.add_row(
        "  region / profile",
        f"{aws.get('region', '-')} / {aws.get('profile') or '(default)'}",
    )
    grid.add_row("  glue database", _glue_line(aws))
    grid.add_row("  data_path", _s3_line(aws))
    grid.add_row("[cyan]Inlined rows[/cyan]", str(result.get("inlined_rows", "-")))

    console.print(grid)
    _render_rules(console, result.get("rules"))


#: glyph and style per severity/state, mirroring the rich output.
_RULE_GLYPH: dict[str, tuple[str, str]] = {
    "blocker": ("✗", "bold red"),
    "degraded": ("!", "yellow"),
    "info": ("i", "cyan"),
    "passed": ("✓", "green"),
    "skipped": ("⊘", "dim"),
    "error": ("⚠", "bold magenta"),
}


def _render_rules(console: Console, rules_report: Any) -> None:
    """Render the preflight rules report below the diagnostics."""
    if not rules_report:
        return
    rules = rules_report.get("rules", [])
    summary = rules_report.get("summary", {})

    console.print("\n[bold]Compatibility preflight (Iceberg v2)[/bold]")
    for rule in rules:
        _render_one_rule(console, rule)

    parts = [
        f"{summary.get('blocker', 0)} blocker",
        f"{summary.get('degraded', 0)} degraded",
        f"{summary.get('info', 0)} info",
    ]
    if summary.get("rules_skipped"):
        parts.append(f"{summary['rules_skipped']} skipped")
    if summary.get("rules_errored"):
        parts.append(f"{summary['rules_errored']} error(s)")
    parts.append(f"exit {rules_report.get('exit_code', 0)}")
    console.print(f"  [dim]{' · '.join(parts)}[/dim]")

    if summary.get("rules_skipped"):
        gates = ", ".join(summary.get("short_circuited_by", [])) or "—"
        console.print(
            f"  [dim]{summary['rules_skipped']} rule(s) not evaluated "
            f"(skipped by {gates}); 'skipped' is not a pass. "
            f"Resolve the gate and rerun the preflight.[/dim]"
        )


def _render_one_rule(console: Console, rule: dict[str, Any]) -> None:
    rule_id = rule.get("id", "?")
    status = rule.get("status", "passed")
    findings = rule.get("findings", [])

    if status == "skipped":
        by = ", ".join(rule.get("skipped_by", [])) or "—"
        glyph, style = _RULE_GLYPH["skipped"]
        console.print(
            f"  [{style}]{glyph} {rule_id:<20} SKIPPED skipped by {by}[/{style}]"
        )
        return
    if status == "error":
        glyph, style = _RULE_GLYPH["error"]
        error_text = rule.get("error", "")
        console.print(
            f"  [{style}]{glyph} {rule_id:<20} ERROR   "
            f"{scrub_mapping(error_text)}[/{style}]"
        )
        return
    if status == "passed":
        glyph, style = _RULE_GLYPH["passed"]
        console.print(f"  [{style}]{glyph} {rule_id:<20} OK[/{style}]")
        return
    # failed: one block per finding
    for finding in findings:
        severity = finding.get("severity", "info")
        glyph, style = _RULE_GLYPH.get(severity, ("•", ""))
        label = severity.upper()
        console.print(
            f"  [{style}]{glyph} {rule_id:<20} {label:<10} "
            f"{scrub_mapping(finding.get('subject', ''))}[/{style}]"
        )
        console.print(f"      {scrub_mapping(finding.get('detail', ''))}")
        console.print(
            f"      [dim]→ {scrub_mapping(finding.get('remediation', ''))}[/dim]"
        )


def _glue_line(aws: dict[str, Any]) -> str:
    db = aws.get("glue_database", "-")
    if aws.get("glue_database_exists"):
        return f"{db}  (exists)"
    if aws.get("glue_reachable"):
        return f"{db}  (does not exist - will be created on export)"
    return f"{db}  (unreachable)"


def _s3_line(aws: dict[str, Any]) -> str:
    readable = aws.get("data_path_readable")
    if readable is True:
        return "✓ readable"
    if readable is False:
        return "✗ unreachable"
    return "(local - not checked)"


def _render_doctor_table(results: list[dict[str, Any]]) -> None:
    console = Console()
    table = Table(title="ducklake-iceberg doctor · --all-tables", title_justify="left")
    for col in (
        "table",
        "table_id",
        "snapshot_id",
        "schema_version",
        "inlined",
        "preflight",
    ):
        table.add_column(col)
    for result in results:
        identity = result.get("identity", {})
        snapshot = result.get("snapshot", {})
        table.add_row(
            str(result.get("table", "-")),
            str(identity.get("table_id", "-")),
            str(snapshot.get("snapshot_id", "-")),
            str(identity.get("schema_version", "-")),
            str(result.get("inlined_rows", "-")),
            _preflight_verdict(result.get("rules")),
        )
    console.print(table)


def _preflight_verdict(rules_report: Any) -> str:
    """One-line preflight summary for the `--all-tables` table."""
    if not rules_report:
        return "-"
    code = rules_report.get("exit_code", 0)
    summary = rules_report.get("summary", {})
    if code == 1:
        return "[bold magenta]error[/bold magenta]"
    if code == 2:
        return f"[bold red]{summary.get('blocker', 0)} blocker[/bold red]"
    degraded = summary.get("degraded", 0)
    if degraded:
        return f"[yellow]{degraded} degraded[/yellow]"
    return "[green]ok[/green]"


# --------------------------------------------------------------------------
# sync: consolidated report
# --------------------------------------------------------------------------
#: label of the ``status`` column and of the footer by exit code (singular, plural)
_SYNC_STATUS: dict[int, tuple[str, str, str, str]] = {
    int(ExitCode.SUCCESS): ("ok", "green", "ok", "ok"),
    int(ExitCode.NOTHING_TO_DO): ("nothing", "dim", "nothing to do", "nothing to do"),
    int(ExitCode.DIVERGED): ("diverged", "magenta", "diverged", "diverged"),
    int(ExitCode.BLOCKED): ("blocked", "yellow", "blocked", "blocked"),
    int(ExitCode.ERROR): ("error", "bold red", "error", "errors"),
}
#: order of the counters in the footer
_SYNC_FOOTER_ORDER: tuple[int, ...] = (0, 4, 3, 2, 1)


def render_sync(envelope: Envelope, *, console: Console | None = None) -> None:
    """One line per table, sorted by name, and the mandatory footer."""
    console = console or Console()
    results = sorted(envelope.results, key=lambda item: str(item.get("table", "")))
    table = Table(title="ducklake-iceberg sync", title_justify="left")
    table.add_column("table")
    table.add_column("status")
    for name in ("exit", "files", "bytes", "time"):
        table.add_column(name, justify="right")
    counts: dict[int, int] = {}
    for result in results:
        code = int(result.get("exit_code", ExitCode.ERROR))
        counts[code] = counts.get(code, 0) + 1
        label, style, _, _ = _SYNC_STATUS.get(code, (str(code), "", "", ""))
        table.add_row(
            str(result.get("table", "-")),
            f"[{style}]{label}[/{style}]" if style else label,
            str(code),
            _sync_files(result, code),
            _human_bytes(result.get("bytes_referenced"))
            if code == int(ExitCode.SUCCESS)
            else "—",
            _human_seconds(result.get("duration_ms")),
        )
    console.print(table)
    noun = "table" if len(results) == 1 else "tables"
    parts = [f"{len(results)} {noun}"]
    for code in _SYNC_FOOTER_ORDER:
        if counts.get(code):
            _, _, singular, plural = _SYNC_STATUS[code]
            parts.append(f"{counts[code]} {singular if counts[code] == 1 else plural}")
    parts.append(f"total {_human_seconds(envelope.duration_ms)}")
    parts.append(f"exit {envelope.exit_code}")
    console.print(" · ".join(parts))


def _sync_files(result: dict[str, Any], code: int) -> str:
    """``+N`` (and ``-M`` on removals); ``0`` for no change; ``—`` on failure."""
    added = int(result.get("files_added") or 0)
    removed = int(result.get("files_removed") or 0)
    if code == int(ExitCode.NOTHING_TO_DO):
        return "0"
    if code != int(ExitCode.SUCCESS) and not (added or removed):
        return "—"
    text = f"+{added}"
    return f"{text} -{removed}" if removed else text


def _human_bytes(value: Any) -> str:
    """Bytes in binary units (``1.2 GB``)."""
    if not isinstance(value, int) or value <= 0:
        return "—"
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            break
        size /= 1024
    if unit == "B":
        return f"{int(size)} B"
    return f"{size:.1f} {unit}"


def _human_seconds(value: Any) -> str:
    """Milliseconds as ``4.2 s``."""
    if not isinstance(value, int | float):
        return "—"
    return f"{value / 1000:.1f} s"
