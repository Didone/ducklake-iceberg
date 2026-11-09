"""Rule `delete-puffin`.

In v2, Puffin deletion vectors are a blocker: Athena cannot read them, so the
deletions would not be applied. It also emits a preventive `INFO` when
`write_deletion_vectors=true` in the catalog, even without a live DV.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from ducklake_iceberg.doctor.model import DoctorContext, Finding, Severity
from ducklake_iceberg.doctor.registry import register
from ducklake_iceberg.doctor.sql import one_row


def _sql_puffin(ctx: DoctorContext) -> str:
    delete_file = ctx.meta("ducklake_delete_file")
    return f"""\
SELECT
    dl.delete_file_id,
    dl.data_file_id,
    dl.path,
    dl.path_is_relative,
    dl.delete_count,
    dl.file_size_bytes,
    dl.footer_size,
    dl.begin_snapshot
FROM {delete_file} dl
WHERE dl.table_id = $table_id
  AND lower(dl.format) = 'puffin'
  AND $snapshot_id >= dl.begin_snapshot
  AND (dl.end_snapshot IS NULL OR $snapshot_id < dl.end_snapshot)
ORDER BY dl.delete_file_id"""


def _sql_option(ctx: DoctorContext) -> str:
    metadata = ctx.meta("ducklake_metadata")
    return f"""\
SELECT lower(value) AS value, scope
FROM {metadata}
WHERE key = 'write_deletion_vectors'
  AND (scope IS NULL OR (scope = 'table' AND scope_id = $table_id))
ORDER BY scope NULLS LAST
LIMIT 1"""


def classify(
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
        puffin = ctx.query(_sql_puffin(ctx), ctx.bind)
        option = ctx.query(_sql_option(ctx), ctx.bind)
        return classify(puffin, option, ctx, self.id)


RULE = register(DeletePuffinRule())
