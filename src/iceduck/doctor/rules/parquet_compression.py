"""Recommends ZSTD to reduce Parquet byte amplification."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from iceduck.doctor.model import DoctorContext, Finding, Severity
from iceduck.doctor.registry import register


def _sql(ctx: DoctorContext) -> str:
    metadata = ctx.meta("ducklake_metadata")
    table = ctx.meta("ducklake_table")
    schema = ctx.meta("ducklake_schema")
    return f"""\
WITH target AS (
    SELECT t.table_id, t.schema_id
    FROM {table} t
    JOIN {schema} s ON s.schema_id = t.schema_id
    WHERE t.table_id = $table_id
)
SELECT m.value
FROM {metadata} m
CROSS JOIN target t
WHERE m.key = $key
  AND (
      m.scope IS NULL
      OR (m.scope = 'schema' AND m.scope_id = t.schema_id)
      OR (m.scope = 'table' AND m.scope_id = t.table_id)
  )
ORDER BY CASE m.scope
    WHEN 'table' THEN 0
    WHEN 'schema' THEN 1
    ELSE 2
END
LIMIT 1"""


def classify(table: pa.Table, ctx: DoctorContext, rule_id: str) -> list[Finding]:
    values = table.column("value").to_pylist()
    configured = str(values[0]).strip() if values else None
    if configured is not None and configured.lower() == "zstd":
        return []

    setting = configured if configured is not None else "not explicitly configured"
    return [
        Finding(
            rule_id=rule_id,
            severity=Severity.DEGRADED,
            subject=ctx.target.qualified_name,
            detail=(
                f"parquet_compression={setting}; ZSTD reduces the bytes written "
                "and read in DuckLake Parquet files (about half the data file "
                "bytes compared to SNAPPY in our measurements)"
            ),
            remediation=(
                "set parquet_compression = 'zstd' in DuckLake; it applies to "
                "every new file (insert, inlining flush, merge, and rewrite), "
                "and existing files only change compression when rewritten"
            ),
            data={
                "configured_compression": configured,
                "recommended_compression": "zstd",
            },
        )
    ]


@dataclass(frozen=True, slots=True)
class ParquetCompressionRule:
    id: str = "parquet-compression"
    title: str = "DuckLake Parquet compression"
    severity: Severity = Severity.DEGRADED
    remediation: str = "set parquet_compression = 'zstd' in DuckLake"
    requires: tuple[str, ...] = ()

    def check(self, ctx: DoctorContext) -> Sequence[Finding]:
        table = ctx.query(
            _sql(ctx),
            {**ctx.bind, "key": "parquet_compression"},
        )
        return classify(table, ctx, self.id)


RULE = register(ParquetCompressionRule())
