"""Rule `unsupported-type`.

Scans `ducklake_column` (live at the snapshot), builds the qualified name of
nested fields via `parent_column`, and delegates the verdict to
`resolve_type`. Blocks caused by parameters (`decimal(40,2)`) or by an unknown
name only show up in `resolve_type`, not in the SQL, which is why the
classification runs over all live columns.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from ducklake_iceberg.doctor.model import DoctorContext, Finding, Severity
from ducklake_iceberg.doctor.registry import register
from ducklake_iceberg.doctor.sql import MAX_FINDINGS, as_int
from ducklake_iceberg.mapping.types import TypeStatus, resolve_type

MAX_DEPTH: int = 64


class DepthExceededError(RuntimeError):
    """Nesting depth above MAX_DEPTH (suspected cycle)."""


def _sql(ctx: DoctorContext) -> str:
    column = ctx.meta("ducklake_column")
    return f"""\
WITH RECURSIVE live_column AS (
    SELECT
        c.column_id, c.parent_column, c.column_order,
        c.column_name, c.column_type, c.nulls_allowed
    FROM {column} c
    WHERE c.table_id = $table_id
      AND $snapshot_id >= c.begin_snapshot
      AND (c.end_snapshot IS NULL OR $snapshot_id < c.end_snapshot)
),
qualified(column_id, parent_column, depth, qualified_name,
          column_type, column_order) AS (
    SELECT column_id, parent_column, 0, column_name, column_type, column_order
    FROM live_column
    WHERE parent_column IS NULL
    UNION ALL
    SELECT
        c.column_id, c.parent_column, q.depth + 1,
        q.qualified_name || '.' || c.column_name,
        c.column_type, c.column_order
    FROM live_column c
    JOIN qualified q ON c.parent_column = q.column_id
)
SELECT column_id, qualified_name, column_type, depth
FROM qualified
ORDER BY depth, column_order, column_id"""


def classify(table: pa.Table, ctx: DoctorContext, rule_id: str) -> list[Finding]:
    blockers: list[Finding] = []
    notes: dict[str, int] = {}
    total_blocked = 0
    for row in table.to_pylist():
        depth = int(str(row["depth"]))
        if depth > MAX_DEPTH:
            raise DepthExceededError(f"column tree with depth > {MAX_DEPTH}")
        column_type = row["column_type"]
        raw = "" if column_type is None else str(column_type)
        res = resolve_type(raw)
        if res.status is TypeStatus.NESTED:
            continue
        if res.blocked:
            total_blocked += 1
            if len(blockers) < MAX_FINDINGS:
                blockers.append(_blocker(row, res.reason, ctx, rule_id))
        elif res.note:
            notes[res.note] = notes.get(res.note, 0) + 1

    findings = list(blockers)
    if total_blocked > MAX_FINDINGS:
        remaining = total_blocked - MAX_FINDINGS
        findings.append(
            Finding(
                rule_id=rule_id,
                severity=Severity.BLOCKER,
                subject=ctx.target.qualified_name,
                detail=f"... and {remaining} more columns with a blocked type",
                remediation="ALTER TABLE at the source for each column",
                data={"remaining": remaining, "total": total_blocked},
            )
        )
    for note, count in notes.items():
        findings.append(
            Finding(
                rule_id=rule_id,
                severity=Severity.INFO,
                subject=ctx.target.qualified_name,
                detail=f"{count} column(s): {note}",
                remediation="take into account during validation",
                data={"note": note, "columns": count},
            )
        )
    return findings


def _blocker(
    row: dict[str, object], reason: str, ctx: DoctorContext, rule_id: str
) -> Finding:
    name = str(row["qualified_name"])
    column_id = as_int(row["column_id"])
    column_type = "" if row["column_type"] is None else str(row["column_type"])
    subject = f"column {name} (column_id={column_id})"
    return Finding(
        rule_id=rule_id,
        severity=Severity.BLOCKER,
        subject=subject,
        detail=(f"type {column_type!r} has no Iceberg v2 equivalent: {reason}"),
        remediation=(
            f"ALTER TABLE {ctx.target.qualified_name} ALTER COLUMN {name} TYPE ..."
        ),
        data={
            "column_id": column_id,
            "qualified_name": name,
            "column_type": column_type,
        },
    )


@dataclass(frozen=True, slots=True)
class UnsupportedTypeRule:
    id: str = "unsupported-type"
    title: str = "Type without a v2 equivalent"
    severity: Severity = Severity.BLOCKER
    remediation: str = "ALTER TABLE at the source"
    requires: tuple[str, ...] = ()

    def check(self, ctx: DoctorContext) -> Sequence[Finding]:
        table = ctx.query(_sql(ctx), ctx.bind)
        return classify(table, ctx, self.id)


RULE = register(UnsupportedTypeRule())
