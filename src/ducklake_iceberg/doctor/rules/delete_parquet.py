"""Rule `delete-parquet`.

DuckLake delete files other than `puffin` (mostly `parquet`) use their own
positional schema, not Iceberg's position delete schema. Without the
`position-deletes` capability, exporting would republish deleted rows: a
blocker. With it, the files are merely rewritten: a degradation. An unknown
format is always a blocker.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from ducklake_iceberg.doctor.model import Capability, DoctorContext, Finding, Severity
from ducklake_iceberg.doctor.registry import register
from ducklake_iceberg.doctor.sql import as_int


def _sql(ctx: DoctorContext) -> str:
    delete_file = ctx.meta("ducklake_delete_file")
    return f"""\
SELECT
    dl.delete_file_id,
    dl.data_file_id,
    dl.path,
    dl.path_is_relative,
    lower(dl.format)  AS format,
    dl.delete_count,
    dl.file_size_bytes,
    dl.begin_snapshot
FROM {delete_file} dl
WHERE dl.table_id = $table_id
  AND lower(dl.format) <> 'puffin'
  AND $snapshot_id >= dl.begin_snapshot
  AND (dl.end_snapshot IS NULL OR $snapshot_id < dl.end_snapshot)
ORDER BY dl.delete_file_id"""


def classify(table: pa.Table, ctx: DoctorContext, rule_id: str) -> list[Finding]:
    rows = table.to_pylist()
    if not rows:
        return []

    has_deletes = Capability.POSITION_DELETES in ctx.capabilities
    # aggregate by format
    by_format: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        by_format.setdefault(str(row["format"]), []).append(row)

    findings: list[Finding] = []
    for fmt, group in sorted(by_format.items()):
        total_deletes = sum(as_int(r["delete_count"]) for r in group)
        if fmt == "parquet":
            severity = Severity.DEGRADED if has_deletes else Severity.BLOCKER
            detail = (
                f"{len(group)} delete files in 'parquet' format covering "
                f"{total_deletes} deletions; the DuckLake positional schema "
                f"is not the Iceberg position delete schema"
            )
            remediation = (
                "none: export rewrites them as Iceberg position deletes"
                if has_deletes
                else "wait for position-delete support (export rewrites them), or "
                "compact the table in DuckLake to materialize the deletes"
            )
        else:
            severity = Severity.BLOCKER
            detail = (
                f"unsupported delete file format: {fmt!r} "
                f"({len(group)} files, {total_deletes} deletions); there is no "
                f"way to guarantee the deletions would be preserved"
            )
            remediation = "open an issue with the observed value"
        findings.append(
            Finding(
                rule_id=rule_id,
                severity=severity,
                subject=ctx.target.qualified_name,
                detail=detail,
                remediation=remediation,
                data={
                    "format": fmt,
                    "delete_files": len(group),
                    "delete_count": total_deletes,
                },
            )
        )
    return findings


@dataclass(frozen=True, slots=True)
class DeleteParquetRule:
    id: str = "delete-parquet"
    title: str = "Parquet delete files"
    severity: Severity = Severity.BLOCKER
    remediation: str = "export rewrites them as Iceberg position deletes"
    requires: tuple[str, ...] = ("inlined-data",)

    def check(self, ctx: DoctorContext) -> Sequence[Finding]:
        table = ctx.query(_sql(ctx), ctx.bind)
        return classify(table, ctx, self.id)


RULE = register(DeleteParquetRule())
