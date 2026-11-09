"""Report rendering for the status command.

Supports rich (terminal) output and structured JSON.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from rich.console import Console
from rich.markup import escape
from rich.table import Table

from iceduck import __version__
from iceduck.obs.redaction import scrub
from iceduck.status import DIVERGENT_STATUSES, Severity, StatusCode

if TYPE_CHECKING:
    from iceduck.status import Pending, TableStatus

# JSON schema version (bump on incompatible changes)
JSON_SCHEMA_VERSION = 1


def render_rich(statuses: list[TableStatus], *, console: Console | None = None) -> None:
    """Render status in rich format for the terminal.

    Args:
        statuses: List of table statuses.
        console: Rich console (default: creates a new one).
    """
    if console is None:
        console = Console()

    if not statuses:
        console.print("[yellow]No tables found.[/yellow]")
        return

    # One table per status (single mode)
    if len(statuses) == 1:
        _render_single_table(statuses[0], console)
    else:
        _render_multiple_tables(statuses, console)


def _render_single_table(status: TableStatus, console: Console) -> None:
    """Render the status of a single table."""
    # Header
    severity_color = _severity_color(status.severity)
    console.print(
        f"\n {status.ducklake_schema}.{status.ducklake_table} → "
        f"glue:{status.glue_database}.{status.glue_table}    "
        f"[{severity_color}]{status.status.upper()}[/{severity_color}]\n"
    )

    # Quantities table
    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
    table.add_column("Quantity", style="cyan")
    table.add_column("Value")

    # D
    d_str = _value(status.d_snapshot_id, "(absent)")
    table.add_row("D  DuckLake current", d_str)

    # P
    p_str = _value(status.p_snapshot_id, "(absent)")
    table.add_row("P  ducklake.snapshot-id", p_str)

    # I
    i_str = _value(status.i_snapshot_id, "(absent)")
    table.add_row("I  Iceberg current-snapshot-id", i_str)

    # Î
    i_hat_str = _value(status.i_hat_snapshot_id, "(absent)")
    match_marker = ""
    if status.i_snapshot_id is not None and status.i_hat_snapshot_id is not None:
        if status.i_snapshot_id == status.i_hat_snapshot_id:
            match_marker = " [green]✓ equal to I[/green]"
        else:
            match_marker = " [red]✗ differs from I[/red]"
    table.add_row("Î  iceduck.iceberg-snapshot-id", i_hat_str + match_marker)

    # Schema version
    schema_str = (
        f"DuckLake {status.d_schema_version} / mirror {status.p_schema_version}"
    )
    schema_match = ""
    if status.d_schema_version is not None and status.p_schema_version is not None:
        if status.d_schema_version == status.p_schema_version:
            schema_match = " [green]✓[/green]"
        else:
            schema_match = " [yellow]✗[/yellow]"
    table.add_row("   schema-version", schema_str + schema_match)

    console.print(table)

    # Pending
    if status.pending is not None and status.pending.has_changes:
        console.print()
        added = f"+{status.pending.added_files} files"
        removed = (
            f"-{status.pending.removed_files} files"
            if status.pending.removed_files > 0
            else ""
        )
        records_str = f"+{status.pending.added_records:,} rows"
        bytes_mib = status.pending.added_bytes / (1024 * 1024)
        bytes_str = f"{bytes_mib:.1f} MiB"

        parts = [added]
        if removed:
            parts.append(removed)
        parts.append(f"({records_str}, {bytes_str})")
        deletes = (
            status.pending.added_delete_files + status.pending.removed_delete_files
        )
        if deletes:
            parts.append(
                f"delete files +{status.pending.added_delete_files}"
                f"/-{status.pending.removed_delete_files}"
            )

        console.print(
            f" Pending: {status.pending.snapshots} snapshots · {' · '.join(parts)}"
        )

    # Findings
    if status.findings:
        console.print()
        for finding in status.findings:
            if finding.message:
                color = _severity_color(finding.severity)
                console.print(f" [{color}]▸ {escape(scrub(finding.message))}[/{color}]")

    # Remediation
    console.print()
    if status.findings and status.findings[0].remediation:
        remediation = escape(scrub(status.findings[0].remediation))
        console.print(f" [bold]▸ Run:[/bold] {remediation}")
    elif status.status == StatusCode.IN_SYNC:
        console.print(" [green]Nothing to do.[/green]")

    console.print()


def _render_multiple_tables(statuses: list[TableStatus], console: Console) -> None:
    """Render a summary of multiple tables."""
    # Summary table
    table = Table(show_header=True, header_style="bold")
    table.add_column("Table", style="cyan")
    table.add_column("Status")
    table.add_column("D")
    table.add_column("P")
    table.add_column("Pending")

    for status in statuses:
        severity_color = _severity_color(status.severity)
        status_str = f"[{severity_color}]{status.status}[/{severity_color}]"

        d_str = _value(status.d_snapshot_id, "-")
        p_str = _value(status.p_snapshot_id, "-")

        pending_str = "-"
        if status.pending is not None and status.pending.has_changes:
            pending_str = (
                f"{status.pending.snapshots} snapshots, "
                f"+{status.pending.added_files} files"
            )

        table.add_row(
            f"{status.ducklake_schema}.{status.ducklake_table}",
            status_str,
            d_str,
            p_str,
            pending_str,
        )

    console.print(table)

    # Footer with a count per status
    counts: dict[str, int] = {}
    for status in statuses:
        counts[status.status] = counts.get(status.status, 0) + 1

    console.print()
    console.print(f"Total: {len(statuses)} tables")
    for code, count in sorted(counts.items()):
        console.print(f"  {code}: {count}")
    console.print()


def _value(value: int | None, absent: str) -> str:
    return absent if value is None else str(value)


def _severity_color(severity: Severity) -> str:
    """Return the rich color for a severity."""
    match severity:
        case Severity.OK:
            return "green"
        case Severity.INFO:
            return "blue"
        case Severity.WARN:
            return "yellow"
        case Severity.ERROR:
            return "red"


def _iso(value: datetime | None) -> str | None:
    """ISO-8601 in UTC with a ``Z`` suffix; explicit ``None``."""
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _pending_json(
    pending: Pending | None, *, synced_at: datetime | None
) -> dict[str, int | None] | None:
    if pending is None:
        return None
    age = (
        max(0, int((datetime.now(UTC) - synced_at.astimezone(UTC)).total_seconds()))
        if pending.has_changes and synced_at is not None
        else None
    )
    return {
        "snapshots": pending.snapshots,
        "added_files": pending.added_files,
        "removed_files": pending.removed_files,
        "added_records": pending.added_records,
        "removed_records": pending.removed_records,
        "added_bytes": pending.added_bytes,
        "added_delete_files": pending.added_delete_files,
        "removed_delete_files": pending.removed_delete_files,
        "unpublished_changes": (
            pending.added_files
            + pending.removed_files
            + pending.added_delete_files
            + pending.removed_delete_files
        ),
        "unpublished_age_s": age,
    }


#: statuses where Glue was moved outside of iceduck
_EXTERNAL_WRITE_STATUSES: frozenset[StatusCode] = frozenset(
    {
        StatusCode.EXTERNAL_WRITE,
        StatusCode.HISTORY_REWRITTEN,
        StatusCode.DIVERGED_BOTH,
    }
)


def table_json(status: TableStatus) -> dict[str, object]:
    """Document for one table; null fields are emitted explicitly.

    ``table``, ``ducklake_snapshot_id``, ``iceberg_snapshot_id``, ``in_sync`` and
    ``external_write_suspected`` are the minimum keys (a flat summary of the
    ``ducklake``/``iceberg``/``status`` blocks).
    """
    return {
        "table": f"{status.ducklake_schema}.{status.ducklake_table}",
        "ducklake_snapshot_id": status.d_snapshot_id,
        "iceberg_snapshot_id": status.i_snapshot_id,
        "in_sync": status.status is StatusCode.IN_SYNC,
        "external_write_suspected": status.status in _EXTERNAL_WRITE_STATUSES,
        "ducklake": {
            "schema": status.ducklake_schema,
            "table": status.ducklake_table,
            "table_id": status.ducklake_table_id,
            "table_uuid": status.ducklake_table_uuid,
            "snapshot_id": status.d_snapshot_id,
            "snapshot_time": _iso(status.d_snapshot_time),
            "schema_version": status.d_schema_version,
            "oldest_retained_snapshot": status.oldest_retained_snapshot,
        },
        "iceberg": {
            "database": status.glue_database,
            "table": status.glue_table,
            "metadata_location": status.metadata_location,
            "current_snapshot_id": status.i_snapshot_id,
            "sequence_number": status.sequence_number,
            "current_schema_id": status.current_schema_id,
            "total_data_files": status.total_data_files,
            "total_records": status.total_records,
        },
        "state": {
            "state_version": status.state_version,
            "ducklake_snapshot_id": status.p_snapshot_id,
            "ducklake_schema_version": status.p_schema_version,
            "iceberg_snapshot_id": status.i_hat_snapshot_id,
            "synced_at": _iso(status.synced_at),
            "sync_mode": status.sync_mode,
        },
        "status": str(status.status),
        "severity": str(status.severity),
        "exit_code": status.exit_code,
        "pending": _pending_json(status.pending, synced_at=status.synced_at),
        "findings": [
            {
                "code": str(f.code),
                "severity": str(f.severity),
                "message": f.message,
                "detail": f.detail,
                "remediation": f.remediation,
            }
            for f in status.findings
        ],
        "checked_at": _iso(status.checked_at),
    }


def render_json(statuses: list[TableStatus]) -> dict[str, object]:
    """Versioned ``--json`` document.

    Args:
        statuses: List of table statuses.

    Returns:
        JSON document (dict) with ``version``, ``tables`` and ``summary``.
    """
    counts: dict[StatusCode, int] = {}
    for status in statuses:
        counts[status.status] = counts.get(status.status, 0) + 1

    worst = Severity.OK
    for status in statuses:
        if _severity_rank(status.severity) > _severity_rank(worst):
            worst = status.severity

    return {
        "version": JSON_SCHEMA_VERSION,
        "generated_at": _iso(datetime.now(UTC)),
        "iceduck_version": __version__,
        "tables": [table_json(s) for s in statuses],
        "summary": {
            "tables": len(statuses),
            "in_sync": counts.get(StatusCode.IN_SYNC, 0),
            "behind": counts.get(StatusCode.BEHIND, 0),
            "diverged": sum(counts.get(c, 0) for c in DIVERGENT_STATUSES),
            "errors": sum(
                n for code, n in counts.items() if _exit_of_status(code) == 1
            ),
            "by_status": {str(code): n for code, n in sorted(counts.items())},
            "worst_severity": str(worst),
            "exit_code": aggregate_exit_code(statuses),
        },
    }


def _exit_of_status(code: StatusCode) -> int:
    return (
        1 if code is StatusCode.UNREADABLE else (4 if code is StatusCode.IN_SYNC else 3)
    )


def _severity_rank(severity: Severity) -> int:
    """Return the numeric severity rank (higher = worse)."""
    match severity:
        case Severity.OK:
            return 0
        case Severity.INFO:
            return 1
        case Severity.WARN:
            return 2
        case Severity.ERROR:
            return 3


def aggregate_exit_code(statuses: list[TableStatus]) -> int:
    """Aggregate exit codes across multiple tables.

    Args:
        statuses: List of table statuses.

    Returns:
        Aggregated exit code (worst severity).
    """
    if not statuses:
        return 4  # nothing to do

    # Priority order: 1 (error) > 3 (divergence) > 4 (ok)
    has_error = any(s.exit_code == 1 for s in statuses)
    if has_error:
        return 1

    has_divergence = any(s.exit_code == 3 for s in statuses)
    if has_divergence:
        return 3

    # All in_sync
    return 4


def print_json(doc: dict[str, object]) -> None:
    """Print a JSON document with indentation."""
    print(json.dumps(doc, indent=2, ensure_ascii=False))
