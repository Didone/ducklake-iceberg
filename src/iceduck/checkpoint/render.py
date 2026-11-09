"""`rich` rendering of the checkpoint report.

Consumes the dict from `CheckpointRunReport.to_json()` (the same one behind
`--json`), so the human and machine outputs never diverge.
"""

from __future__ import annotations

from typing import Any

from rich.console import Console
from rich.table import Table


def _files(step: dict[str, Any] | None) -> str:
    if not step or not step.get("files_processed"):
        return "-"
    return f"{step['files_processed']} → {step.get('files_created', 0)}"


def _situation(row: dict[str, Any]) -> str:
    error = row.get("error")
    if error:
        if error.get("code") == "E-VARIANT-NESTED":
            return "[bold red]nested VARIANT[/bold red]"
        phase = error.get("phase") or "?"
        return f"[bold red]error[/bold red] ({phase})"
    if row.get("changed"):
        return "[green]checkpoint[/green]"
    return "[dim]nothing to do[/dim]"


def render_checkpoint(report: dict[str, Any]) -> None:
    """`rich` table on stdout (human mode)."""
    console = Console()
    dry_run = bool(report.get("dry_run"))
    catalog = report.get("catalog", {})
    before = report.get("snapshot_before", "?")
    after = report.get("snapshot_after")
    arrow = f"{before} -> {after}" if after not in (None, before) else str(before)
    title = "iceduck checkpoint --dry-run" if dry_run else "iceduck checkpoint"
    console.print(
        f"{title} · {catalog.get('alias', '?')} "
        f"({catalog.get('metadata_catalog', '?')}) · snapshot {arrow}"
    )

    if dry_run:
        _render_dry_run(console, report)
        return

    table = Table(show_header=True, header_style="bold")
    for column in (
        "Table",
        "table_id",
        "flush",
        "expire",
        "merge",
        "rewrite",
        "snapshot",
    ):
        table.add_column(column)
    table.add_column("Status")
    for row in report.get("tables", []):
        rows_flushed = row.get("rows_flushed", 0)
        expired = row.get("snapshots_expired") or []
        table.add_row(
            f"{row.get('schema_name')}.{row.get('table_name')}",
            str(row.get("table_id", "-")),
            str(rows_flushed) if rows_flushed else "-",
            str(len(expired)) if expired else "-",
            _files(row.get("merge")),
            _files(row.get("rewrite")),
            str(row.get("snapshot_after", "-")),
            _situation(row),
        )
    console.print(table)

    totals = report.get("totals", {})
    console.print(
        f"{totals.get('tables_changed', 0)} table(s) checkpointed: "
        f"{totals.get('rows_flushed', 0)} row(s) materialized, "
        f"{totals.get('snapshots_expired', 0)} snapshot(s) expired in the "
        f"catalog, {totals.get('files_processed', 0)} file(s) → "
        f"{totals.get('files_created', 0)}."
    )
    if report.get("exit_code") == 4:
        console.print(
            "[dim]nothing to do: checkpoint had no effect in scope. "
            "Proceed to `iceduck export`.[/dim]"
        )
    for warning in report.get("warnings", []):
        console.print(f"  [yellow]warning:[/yellow] {warning}")

    for row in report.get("tables", []):
        _render_error(console, row)

    for row in report.get("tables", []):
        _render_snapshots(console, row)

    for row in report.get("tables", []):
        _render_revalidation(console, row)

    for row in report.get("tables", []):
        distribution = row.get("file_size_distribution")
        if not distribution:
            continue
        qualified = f"{row.get('schema_name')}.{row.get('table_name')}"
        if distribution["file_count"]:
            stats = (
                f"{distribution['file_count']} files · "
                f"min {distribution['min_bytes']} B · "
                f"p25 {distribution['p25_bytes']:.0f} B · "
                f"median {distribution['median_bytes']:.0f} B · "
                f"p75 {distribution['p75_bytes']:.0f} B · "
                f"max {distribution['max_bytes']} B"
            )
        else:
            stats = "no live data files"
        target = distribution.get("target_file_size_bytes")
        if target is not None:
            stats += (
                f" · target {target} B · "
                f"{distribution['below_half_target']} below 50% of target"
            )
        console.print(f"{qualified} · post-checkpoint distribution: {stats}")
        _render_partitions(console, row)


#: rows of the per-partition table in human mode; `--json` has all of them
_MAX_PARTITION_ROWS = 20


def _render_partitions(console: Console, row: dict[str, Any]) -> None:
    """Per-partition distribution and the criterion on closed partitions."""
    report = row.get("partition_distribution")
    if not report or not report.get("partitions"):
        return
    qualified = f"{row.get('schema_name')}.{row.get('table_name')}"
    closed = report.get("closed_partitions", 0)
    criterion = report.get("closed_criterion_met")
    verdict = {
        True: "[green]ok[/green]",
        False: "[bold red]not met[/bold red]",
    }.get(criterion, "-")
    console.print(
        f"{qualified} · {report.get('partition_count', 0)} partition(s): "
        f"{closed} closed (no write for {report.get('closed_after_seconds')} s), "
        f"{report.get('open_partitions', 0)} open · "
        f"{report.get('closed_partitions_meeting_target', 0)}/{closed} closed "
        f"with median ≥ 50% of target · criterion: {verdict}"
    )
    table = Table(show_header=True, header_style="bold")
    for column in ("Partition", "State", "Files", "Median (B)", "Total (B)"):
        table.add_column(column)
    table.add_column("Target")
    partitions = report["partitions"]
    for part in partitions[-_MAX_PARTITION_ROWS:]:
        dist = part.get("file_size_distribution") or {}
        median = dist.get("median_bytes")
        meets = part.get("meets_target")
        table.add_row(
            str(part.get("partition")),
            "closed" if part.get("state") == "closed" else "open",
            str(dist.get("file_count", 0)),
            f"{median:.0f}" if median is not None else "-",
            str(dist.get("total_bytes", 0)),
            {True: "ok", False: "below"}.get(meets, "-"),
        )
    console.print(table)
    if len(partitions) > _MAX_PARTITION_ROWS:
        console.print(
            f"  [dim]{len(partitions) - _MAX_PARTITION_ROWS} earlier "
            "partition(s) omitted; use --json for the full list.[/dim]"
        )


_ATTRIBUTION_NOTE: dict[str, str] = {
    "exact": "exact attribution",
    "ambiguous": "ambiguous attribution",
    "absent": "snapshots not attributed",
    "no_op": "no new snapshot",
}


def _render_snapshots(console: Console, row: dict[str, Any]) -> None:
    """Report on the snapshots created by the checkpoint."""
    snap = row.get("snapshots")
    if not snap:
        return
    qualified = f"{row.get('schema_name')}.{row.get('table_name')}"
    before = snap.get("snapshot_before")
    target = snap.get("export_target_snapshot")
    attribution = str(snap.get("attribution", "no_op"))
    note = _ATTRIBUTION_NOTE.get(attribution, attribution)
    console.print(f"\n{qualified}  snapshot {before} -> {target}  ({note})")
    if snap.get("flush_snapshot_id") is not None:
        console.print(f"  flush ........ snapshot {snap['flush_snapshot_id']}")
    if snap.get("compaction_snapshot_id") is not None:
        console.print(f"  compaction ... snapshot {snap['compaction_snapshot_id']}")
    hint = snap.get("iceberg_operation_hint", "none")
    console.print(
        f"  export target ............. {target}  (Iceberg operation: {hint})"
    )
    if snap.get("foreign_snapshots"):
        console.print(
            "  [yellow]warning:[/yellow] another writer committed to the catalog "
            "during the checkpoint"
        )
    for warning in snap.get("warnings", []):
        console.print(f"  [yellow]warning:[/yellow] {warning}")


def _dry_run_situation(row: dict[str, Any]) -> str:
    if row.get("blocked_by"):
        return "[bold red]blocked (VARIANT)[/bold red]"
    flush = bool(row.get("would_flush"))
    compact = bool(row.get("would_compact"))
    if flush and compact:
        return "[green]flush + compaction[/green]"
    if flush:
        return "[green]flush[/green]"
    if compact:
        return "[green]compaction[/green]"
    return "[dim]nothing to do[/dim]"


def _render_dry_run(console: Console, report: dict[str, Any]) -> None:
    """`--dry-run` layout: Data/Deletes/Status, without writing."""
    table = Table(show_header=True, header_style="bold")
    for column in (
        "Table",
        "table_id",
        "Data",
        "Deletes",
        "Target (bytes)",
        "Status",
    ):
        table.add_column(column)
    for row in report.get("tables", []):
        table.add_row(
            f"{row.get('schema_name')}.{row.get('table_name')}",
            str(row.get("table_id", "-")),
            str(row.get("data_rows", 0)),
            str(row.get("delete_rows", 0)),
            str(row.get("target_file_size_bytes") or "-"),
            _dry_run_situation(row),
        )
    console.print(table)

    totals = report.get("totals", {})
    rows_pending = totals.get("rows_pending", 0)
    with_pending = totals.get("tables_with_pending", 0)
    console.print(
        f"{rows_pending} inlined row(s) in {with_pending} table(s) at "
        f"snapshot {report.get('snapshot_id', '?')}."
    )
    console.print("Nothing was written: the catalog stayed READ_ONLY.")
    console.print("Run without --dry-run to perform the checkpoint.")
    for warning in report.get("warnings", []):
        console.print(f"  [yellow]warning:[/yellow] {warning}")
    for row in report.get("tables", []):
        _render_partitions(console, row)
    for row in report.get("tables", []):
        if row.get("blocked_by"):
            console.print(
                f"  [bold red]{row.get('schema_name')}.{row.get('table_name')}:"
                f"[/bold red] blocked by nested VARIANT; the checkpoint would "
                "fail. Fix the type at the source before running."
            )


def _render_error(console: Console, row: dict[str, Any]) -> None:
    """Actionable error message for a table (including nested VARIANT)."""
    error = row.get("error")
    if not error:
        return
    qualified = f"{row.get('schema_name')}.{row.get('table_name')}"
    if error.get("code") == "E-VARIANT-NESTED":
        console.print(
            f"\n[bold red]Error: checkpoint blocked by nested VARIANT[/bold red] "
            f"· {qualified}"
        )
        for col in error.get("columns", []):
            parent = (
                f"column_id {col.get('parent_column_id')} ({col.get('parent_type')})"
                if col.get("parent_column_id") is not None
                else "-"
            )
            console.print(
                f"  column {col.get('path')}  ·  column_id {col.get('column_id')}"
                f"  ·  nested in {parent}"
            )
        console.print(
            "  DuckDB cannot write nested VARIANT to Parquet, and the checkpoint "
            "is a Parquet write."
        )
        if error.get("snapshot_created") is not None:
            console.print(
                f"  [yellow]Warning:[/yellow] the checkpoint failed but created "
                f"snapshot {error['snapshot_created']} in DuckLake. Run "
                f"`iceduck doctor --table {qualified}` before exporting."
            )
        else:
            console.print(
                "  Nothing was materialized and nothing was written to the catalog."
            )
        console.print(f"  [dim]→ {error.get('remediation')}[/dim]")
        console.print(
            "  [dim]Iceberg v2 has no variant type: the export stays blocked by "
            "the type rules as long as a variant column exists.[/dim]"
        )
    else:
        console.print(f"\n[bold red]{qualified}:[/bold red] {error.get('message')}")
        if error.get("snapshot_created") is not None:
            console.print(
                f"  [yellow]Warning:[/yellow] the checkpoint failed but created "
                f"snapshot {error['snapshot_created']} in DuckLake."
            )
        if error.get("hint"):
            console.print(f"  [dim]{error['hint']}[/dim]")


_SEVERITY_LABEL: dict[str, str] = {
    "blocker": "[bold red]blocker[/bold red]",
    "degraded": "[yellow]degraded[/yellow]",
    "info": "[cyan]info[/cyan]",
}

_ORIGIN_LABEL: dict[str, str] = {
    "introduced": "this window",
    "pre_existing": "earlier",
    "unknown": "-",
}


def _render_revalidation(console: Console, row: dict[str, Any]) -> None:
    """Post-checkpoint revalidation. The checkpoint result was already printed
    above, before any blocker."""
    qualified = f"{row.get('schema_name')}.{row.get('table_name')}"
    error = row.get("revalidation_error")
    if error:
        console.print(
            f"\n[bold magenta]{qualified}:[/bold magenta] checkpoint completed "
            f"(snapshot {row.get('snapshot_after')}); {error}"
        )
        return
    rev = row.get("revalidation")
    if not rev:
        return
    run, skipped = len(rev.get("rules_run", [])), len(rev.get("rules_skipped", []))
    console.print(
        f"\n{qualified} · revalidation at snapshot {rev.get('snapshot_after')} "
        f"({run} rules rerun, {skipped} skipped) · export target: "
        f"{rev.get('export_target_snapshot')}"
    )
    findings = [f for f in rev.get("findings", []) if f.get("severity") != "info"]
    if findings:
        table = Table(show_header=True, header_style="bold")
        for column in ("Rule", "Severity", "Origin", "Detail"):
            table.add_column(column)
        for f in findings:
            table.add_row(
                str(f.get("rule_id")),
                _SEVERITY_LABEL.get(str(f.get("severity")), str(f.get("severity"))),
                _ORIGIN_LABEL.get(str(f.get("origin")), "-"),
                str(f.get("detail")),
            )
        console.print(table)
        for f in findings:
            console.print(f"  [dim]→ {f.get('rule_id')}: {f.get('remediation')}[/dim]")
    pending = rev.get("still_pending_rows", 0)
    code = rev.get("exit_code", 0)
    if pending:
        console.print(
            f"  [bold red]inlined rows still pending after the checkpoint: "
            f"{pending}[/bold red] -- possible concurrent write; rerun "
            "`iceduck checkpoint`."
        )
    elif code == 2:
        console.print(
            "  The checkpoint completed and is not reversible. Export of this "
            "table remains blocked from the current state; follow the actions "
            "above."
        )
    elif code == 0:
        console.print("  [green]revalidation ok: table ready for export.[/green]")
