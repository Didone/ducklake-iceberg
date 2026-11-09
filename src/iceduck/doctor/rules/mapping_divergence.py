"""Rule `mapping-divergence`.

`ducklake_data_file.mapping_id` is per file; Iceberg's
`schema.name-mapping.default` is per table. More than one distinct mapping
among live files cannot be expressed in Iceberg. A single mapping can be
expressed, but export does not emit it yet, so it is a blocker without the
`name-mapping` capability.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from iceduck.doctor.model import Capability, DoctorContext, Finding, JsonValue, Severity
from iceduck.doctor.registry import register
from iceduck.doctor.sql import one_row


def _sql_distribution(ctx: DoctorContext) -> str:
    data_file = ctx.meta("ducklake_data_file")
    return f"""\
SELECT
    count(*)                                          AS live_files,
    count(*) FILTER (WHERE df.mapping_id IS NOT NULL) AS mapped_files,
    count(DISTINCT df.mapping_id)                     AS distinct_mappings,
    list_sort(
        array_agg(DISTINCT df.mapping_id)
            FILTER (WHERE df.mapping_id IS NOT NULL)
    )                                                 AS mapping_ids
FROM {data_file} df
WHERE df.table_id = $table_id
  AND $snapshot_id >= df.begin_snapshot
  AND (df.end_snapshot IS NULL OR $snapshot_id < df.end_snapshot)"""


def _sql_types(ctx: DoctorContext) -> str:
    column_mapping = ctx.meta("ducklake_column_mapping")
    return f"""\
SELECT cm.mapping_id, lower(cm."type") AS mapping_type
FROM {column_mapping} cm
WHERE cm.table_id = $table_id
ORDER BY cm.mapping_id"""


def classify(
    distribution: pa.Table,
    types: pa.Table,
    ctx: DoctorContext,
    rule_id: str,
) -> list[Finding]:
    distinct = int(one_row(distribution, "distinct_mappings", 0) or 0)
    if distinct == 0:
        return []

    live = int(one_row(distribution, "live_files", 0) or 0)
    mapped = int(one_row(distribution, "mapped_files", 0) or 0)
    ids_raw = one_row(distribution, "mapping_ids", []) or []
    ids: list[int] = [int(x) for x in ids_raw]
    ids_json: list[JsonValue] = list(ids)

    findings: list[Finding] = []

    # an unknown mapping type is always a blocker
    unknown = [
        row
        for row in types.to_pylist()
        if row["mapping_id"] in ids and str(row["mapping_type"]) != "map_by_name"
    ]
    for row in unknown:
        findings.append(
            Finding(
                rule_id=rule_id,
                severity=Severity.BLOCKER,
                subject=ctx.target.qualified_name,
                detail=(
                    f"unknown column mapping type: "
                    f"{str(row['mapping_type'])!r} (mapping_id={row['mapping_id']})"
                ),
                remediation="open an issue with the observed value",
                data={
                    "mapping_id": int(row["mapping_id"]),
                    "mapping_type": str(row["mapping_type"]),
                },
            )
        )

    if distinct > 1:
        findings.append(
            Finding(
                rule_id=rule_id,
                severity=Severity.BLOCKER,
                subject=ctx.target.qualified_name,
                detail=(
                    f"{live} live files use {distinct} distinct mapping_ids "
                    f"({', '.join(map(str, ids))}); Iceberg has a single "
                    f"schema.name-mapping.default per table and cannot describe "
                    f"files with different mappings"
                ),
                remediation=(
                    "rewrite the divergent files in DuckLake (compaction) so "
                    "they use the native field IDs"
                ),
                data={
                    "distinct_mappings": distinct,
                    "mapping_ids": ids_json,
                    "live_files": live,
                    "mapped_files": mapped,
                },
            )
        )
    elif distinct == 1 and not unknown:
        has_cap = Capability.NAME_MAPPING in ctx.capabilities
        severity = Severity.DEGRADED if has_cap else Severity.BLOCKER
        mixed = mapped < live
        detail = (
            f"files use mapping_id {ids[0]}; "
            + ("some resolve by ID and some by name; " if mixed else "")
            + "name mapping is not yet emitted by export"
        )
        findings.append(
            Finding(
                rule_id=rule_id,
                severity=severity,
                subject=ctx.target.qualified_name,
                detail=detail,
                remediation=("compact in DuckLake, or wait for name mapping support"),
                data={
                    "distinct_mappings": 1,
                    "mapping_ids": ids_json,
                    "live_files": live,
                    "mapped_files": mapped,
                },
            )
        )
    return findings


@dataclass(frozen=True, slots=True)
class MappingDivergenceRule:
    id: str = "mapping-divergence"
    title: str = "mapping_id differs between files"
    severity: Severity = Severity.BLOCKER
    remediation: str = "compact in DuckLake"
    requires: tuple[str, ...] = ("inlined-data",)

    def check(self, ctx: DoctorContext) -> Sequence[Finding]:
        distribution = ctx.query(_sql_distribution(ctx), ctx.bind)
        types = ctx.query(_sql_types(ctx), ctx.bind)
        return classify(distribution, types, ctx, self.id)


RULE = register(MappingDivergenceRule())
