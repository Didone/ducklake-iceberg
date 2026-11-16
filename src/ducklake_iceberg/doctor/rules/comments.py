"""Snapshot-scoped documentation compatibility, checked before any flush."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from ducklake_iceberg.catalog.reader import read_comments
from ducklake_iceberg.doctor.model import DoctorContext, Finding, Severity
from ducklake_iceberg.doctor.registry import register
from ducklake_iceberg.mapping.schema import comment_problem


@dataclass(frozen=True, slots=True)
class CommentsRule:
    id: str = "comments"
    title: str = "Catalog comments"
    severity: Severity = Severity.BLOCKER
    remediation: str = "use comments representable in the target catalog"
    requires: tuple[str, ...] = ()

    def check(self, ctx: DoctorContext) -> Sequence[Finding]:
        state = read_comments(
            ctx.con,
            ctx.target.metadata_catalog,
            ctx.target.table_id,
            ctx.target.snapshot_id,
        )
        values = [
            ("table", state.table, True, False),
            *[
                (f"column {name} (id={cid})", doc, False, parent is not None)
                for cid, parent, name, doc in state.columns
            ],
        ]
        return [
            Finding(self.id, self.severity, subject, problem, self.remediation)
            for subject, text, table, nested in values
            if (problem := comment_problem(text, table=table, nested=nested))
        ]


RULE = register(CommentsRule())
