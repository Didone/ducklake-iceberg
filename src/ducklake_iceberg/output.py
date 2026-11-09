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
    if envelope.command == "sync" and envelope.results:
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
