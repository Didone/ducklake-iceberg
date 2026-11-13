"""Classify source delete formats before Iceberg v2 publication.

Parquet deletes can be converted to position deletes. Puffin vectors and
unknown formats remain blockers. Both rules share the same snapshot read.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

from ducklake_iceberg.doctor.model import Capability, DoctorContext, Finding, Severity
from ducklake_iceberg.doctor.registry import register
from ducklake_iceberg.doctor.sql import as_int, live_delete_files_sql, one_row


def _sql_parquet(ctx: DoctorContext) -> str:
    return live_delete_files_sql(ctx)


def classify_parquet(
    table: pa.Table, ctx: DoctorContext, rule_id: str
) -> list[Finding]:
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
        deletes = ctx.query_cached(_sql_parquet(ctx))
        table = deletes.filter(pc.not_equal(deletes["format"], "puffin"))
        return classify_parquet(table, ctx, self.id)


PARQUET_RULE = register(DeleteParquetRule())


def _sql_puffin(ctx: DoctorContext) -> str:
    return live_delete_files_sql(ctx)


def _sql_puffin_option(ctx: DoctorContext) -> str:
    metadata = ctx.meta("ducklake_metadata")
    return f"""\
SELECT lower(value) AS value, scope
FROM {metadata}
WHERE key = 'write_deletion_vectors'
  AND (scope IS NULL OR (scope = 'table' AND scope_id = $table_id))
ORDER BY scope NULLS LAST
LIMIT 1"""


def classify_puffin(
    puffin: pa.Table, option: pa.Table, ctx: DoctorContext, rule_id: str
) -> list[Finding]:
    rows = puffin.to_pylist()
    if rows:
        total_deletes = sum(int(r["delete_count"]) for r in rows)
        return [
            Finding(
                rule_id=rule_id,
                severity=Severity.BLOCKER,
                subject=ctx.target.qualified_name,
                detail=(
                    f"{len(rows)} Puffin deletion vectors covering "
                    f"{total_deletes} deletions; Athena cannot read deletion "
                    f"vectors in Iceberg v2 tables, and the rows would reappear "
                    f"in queries"
                ),
                remediation=(
                    "rewrite as a v2 position delete file, or compact the "
                    "table in DuckLake"
                ),
                data={"puffin_files": len(rows), "delete_count": total_deletes},
            )
        ]

    option_value = one_row(option, "value")
    if option_value == "true":
        return [
            Finding(
                rule_id=rule_id,
                severity=Severity.INFO,
                subject=ctx.target.qualified_name,
                detail=(
                    "write_deletion_vectors=true in the catalog: the next "
                    "deletions will be written as Puffin and will block the "
                    "next sync"
                ),
                remediation="consider disabling the option while the target is v2",
                data={"write_deletion_vectors": True},
            )
        ]
    return []


@dataclass(frozen=True, slots=True)
class DeletePuffinRule:
    id: str = "delete-puffin"
    title: str = "Puffin delete files (deletion vectors)"
    severity: Severity = Severity.BLOCKER
    remediation: str = "rewrite as position deletes or compact"
    requires: tuple[str, ...] = ("inlined-data",)

    def check(self, ctx: DoctorContext) -> Sequence[Finding]:
        deletes = ctx.query_cached(_sql_puffin(ctx))
        puffin = deletes.filter(pc.equal(deletes["format"], "puffin"))
        option = ctx.query(_sql_puffin_option(ctx), ctx.bind)
        return classify_puffin(puffin, option, ctx, self.id)


PUFFIN_RULE = register(DeletePuffinRule())
