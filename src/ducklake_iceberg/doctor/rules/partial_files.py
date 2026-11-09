"""Rule `partial-files`.

Blocks if and only if `partial_max > S`. Files with `partial_max <= S` are
routine (every inlined flush that spans more than one transaction produces
one) and map 1:1 to Iceberg data files, so they become a single `INFO`
finding.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from ducklake_iceberg.doctor.model import DoctorContext, Finding, Severity
from ducklake_iceberg.doctor.registry import register
from ducklake_iceberg.doctor.sql import MAX_FINDINGS


def _sql(ctx: DoctorContext) -> str:
    data_file = ctx.meta("ducklake_data_file")
    delete_file = ctx.meta("ducklake_delete_file")
    return f"""\
WITH partial_files AS (
    SELECT
        'data'              AS kind,
        df.data_file_id     AS file_id,
        df.path             AS path,
        df.path_is_relative AS path_is_relative,
        df.partial_max      AS partial_max,
        df.begin_snapshot   AS begin_snapshot,
        df.record_count     AS row_count
    FROM {data_file} df
    WHERE df.table_id = $table_id
      AND df.partial_max IS NOT NULL
      AND $snapshot_id >= df.begin_snapshot
      AND (df.end_snapshot IS NULL OR $snapshot_id < df.end_snapshot)
    UNION ALL
    SELECT
        'delete',
        dl.delete_file_id,
        dl.path,
        dl.path_is_relative,
        dl.partial_max,
        dl.begin_snapshot,
        dl.delete_count
    FROM {delete_file} dl
    WHERE dl.table_id = $table_id
      AND dl.partial_max IS NOT NULL
      AND $snapshot_id >= dl.begin_snapshot
      AND (dl.end_snapshot IS NULL OR $snapshot_id < dl.end_snapshot)
)
SELECT
    kind, file_id, path, path_is_relative, partial_max,
    begin_snapshot, row_count,
    (partial_max > $snapshot_id) AS beyond_target
FROM partial_files
ORDER BY beyond_target DESC, kind, file_id"""


def classify(table: pa.Table, ctx: DoctorContext, rule_id: str) -> list[Finding]:
    rows = table.to_pylist()
    beyond = [r for r in rows if r["beyond_target"]]
    routine = [r for r in rows if not r["beyond_target"]]

    findings: list[Finding] = []
    for row in beyond[:MAX_FINDINGS]:
        path = str(row["path"])
        findings.append(
            Finding(
                rule_id=rule_id,
                severity=Severity.BLOCKER,
                subject=path,
                detail=(
                    f"{path}: partial_max={row['partial_max']} > target snapshot "
                    f"{ctx.target.snapshot_id}; the file contains rows "
                    f"inserted after {ctx.target.snapshot_id}, which would "
                    f"appear in the Iceberg table"
                ),
                remediation=(
                    "export the current snapshot, or compact the table in "
                    "DuckLake to materialize the history"
                ),
                data={
                    "kind": str(row["kind"]),
                    "path": path,
                    "path_is_relative": bool(row["path_is_relative"]),
                    "partial_max": int(row["partial_max"]),
                    "begin_snapshot": int(row["begin_snapshot"]),
                },
            )
        )
    if len(beyond) > MAX_FINDINGS:
        findings.append(
            Finding(
                rule_id=rule_id,
                severity=Severity.BLOCKER,
                subject=ctx.target.qualified_name,
                detail=(
                    f"... and {len(beyond) - MAX_FINDINGS} more partial files "
                    f"beyond the target snapshot"
                ),
                remediation="export the current snapshot",
                data={"remaining": len(beyond) - MAX_FINDINGS, "total": len(beyond)},
            )
        )
    if routine:
        findings.append(
            Finding(
                rule_id=rule_id,
                severity=Severity.INFO,
                subject=ctx.target.qualified_name,
                detail=(
                    f"{len(routine)} partial files with partial_max <= "
                    f"{ctx.target.snapshot_id}: all rows are visible at the "
                    f"target snapshot; they map 1:1 to Iceberg data files"
                ),
                remediation="none",
                data={"partial_files": len(routine)},
            )
        )
    return findings


@dataclass(frozen=True, slots=True)
class PartialFilesRule:
    id: str = "partial-files"
    title: str = "Partial data files"
    severity: Severity = Severity.BLOCKER
    remediation: str = "export the current snapshot or compact in DuckLake"
    requires: tuple[str, ...] = ("inlined-data",)

    def check(self, ctx: DoctorContext) -> Sequence[Finding]:
        table = ctx.query(_sql(ctx), ctx.bind)
        return classify(table, ctx, self.id)


RULE = register(PartialFilesRule())
