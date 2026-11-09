"""Rule `inlined-data`.

Gate for all rules that read files. While inlined data exists,
`ducklake_data_file` and `ducklake_list_files` return empty (not an error):
exporting in that state produces a syntactically valid Iceberg table with no
rows. This rule runs first and blocks the rest.

The rule **does not repeat the SQL**: it delegates to `detect_inlined`, the
single source of detection, shared with the checkpoint command and the
post-flush revalidation. The rule only translates the `InlinedStatus` into a
`Finding`.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from iceduck.checkpoint.detect import detect_inlined
from iceduck.checkpoint.model import TableRef
from iceduck.doctor.model import DoctorContext, Finding, Severity
from iceduck.doctor.registry import register


@dataclass(frozen=True, slots=True)
class InlinedDataRule:
    id: str = "inlined-data"  # fixed identifier (part of the --json contract)
    title: str = "Pending inlined data"
    severity: Severity = Severity.BLOCKER
    remediation: str = "iceduck checkpoint --table <schema>.<table>"
    requires: tuple[str, ...] = ()  # it is a gate; it depends on nothing

    def check(self, ctx: DoctorContext) -> Sequence[Finding]:
        status = detect_inlined(
            ctx.con,
            ctx.target.metadata_catalog,
            TableRef(
                ctx.target.schema_name,
                ctx.target.table_name,
                ctx.target.table_id,
            ),
            ctx.target.snapshot_id,
        )
        if not status.has_pending:
            return ()
        return (
            Finding(
                rule_id=self.id,
                severity=self.severity,
                subject=ctx.target.qualified_name,
                detail=(
                    f"{status.pending_rows} inlined row(s) visible at "
                    f"snapshot {status.snapshot_id} in "
                    f"{len(status.counts)} physical table(s); they do not "
                    f"exist in any Parquet file and would vanish from the "
                    f"Iceberg table without an error"
                ),
                remediation=f"iceduck checkpoint --table {ctx.target.qualified_name}",
                data={
                    "pending_rows": status.pending_rows,
                    "pending_data_rows": status.pending_data_rows,
                    "pending_delete_rows": status.pending_delete_rows,
                    "sources": [c.to_json() for c in status.counts],
                    "warnings": list(status.warnings),
                },
            ),
        )


RULE = register(InlinedDataRule())
