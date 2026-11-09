from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any, Literal

from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
)

from iceduck.concurrency import worker_cursor
from iceduck.retry import call_with_retry

GLUE_STATS_BATCH = 25
GlueStatsType = Literal[
    "BOOLEAN", "DATE", "DECIMAL", "DOUBLE", "LONG", "STRING", "BINARY"
]
NdvMode = Literal["off", "approx"]


@dataclass(frozen=True, slots=True)
class ColumnStatsPlan:
    column_name: str
    duckdb_expr: str
    glue_type: GlueStatsType | None
    skip_reason: str | None = None
    column_type: str | None = None


@dataclass(frozen=True, slots=True)
class ColumnStat:
    column_name: str
    glue_type: GlueStatsType
    number_of_nulls: int
    number_of_distinct_values: int = 0
    minimum: Any | None = None
    maximum: Any | None = None
    maximum_length: int | None = None
    average_length: float | None = None
    number_of_trues: int | None = None
    number_of_falses: int | None = None
    rows_scanned: int = 0
    column_type: str | None = None

    def to_glue(self, analyzed_at: datetime) -> dict[str, Any]:
        kind: dict[str, Any] = {}
        if self.glue_type == "BOOLEAN":
            kind.update(
                NumberOfTrues=self.number_of_trues or 0,
                NumberOfFalses=self.number_of_falses or 0,
            )
        elif self.glue_type in ("STRING", "BINARY"):
            kind.update(
                MaximumLength=self.maximum_length or 0,
                AverageLength=self.average_length or 0.0,
            )
        if self.minimum is not None and self.glue_type not in ("DECIMAL",):
            kind["MinimumValue"] = self.minimum
        if self.maximum is not None and self.glue_type not in ("DECIMAL",):
            kind["MaximumValue"] = self.maximum
        if self.glue_type not in ("BOOLEAN", "BINARY"):
            kind["NumberOfDistinctValues"] = self.number_of_distinct_values
        return {
            "ColumnName": self.column_name,
            "ColumnType": self.column_type or self.glue_type.lower(),
            "AnalyzedTime": analyzed_at,
            "StatisticsData": {
                "Type": self.glue_type,
                f"{self.glue_type.title()}ColumnStatisticsData": {
                    **kind,
                    "NumberOfNulls": self.number_of_nulls,
                },
            },
        }


@dataclass(slots=True)
class GlueStatsReport:
    table: str
    columns_published: int = 0
    columns_skipped: list[tuple[str, str]] = field(default_factory=list)
    batches: int = 0
    api_calls: int = 0
    errors: list[dict[str, str]] = field(default_factory=list)
    duration_ms: int = 0
    rows_scanned: int = 0
    sampled_percent: float | None = None
    ndv_estimated: bool = True


def plan_column_statistics(
    iceberg_schema: Any, *, ndv: NdvMode = "approx"
) -> list[ColumnStatsPlan]:
    result: list[ColumnStatsPlan] = []
    for schema_field in iceberg_schema.fields:
        column_name = schema_field.name
        expr = '"' + column_name.replace('"', '""') + '"'
        itype = schema_field.field_type
        glue_type: GlueStatsType | None = None
        column_type: str | None = None
        reason = "glue_type_unsupported"
        if isinstance(itype, BooleanType):
            glue_type = "BOOLEAN"
            column_type = "boolean"
        elif isinstance(itype, DateType):
            glue_type = "DATE"
            column_type = "date"
        elif isinstance(itype, DecimalType):
            glue_type = "DECIMAL"
            column_type = f"decimal({itype.precision},{itype.scale})"
        elif isinstance(itype, DoubleType):
            glue_type = "DOUBLE"
            column_type = "double"
        elif isinstance(itype, FloatType):
            glue_type = "DOUBLE"
            column_type = "float"
        elif isinstance(itype, IntegerType):
            glue_type = "LONG"
            column_type = "int"
        elif isinstance(itype, LongType):
            glue_type = "LONG"
            column_type = "bigint"
        elif isinstance(itype, StringType):
            glue_type = "STRING"
            column_type = "string"
        elif isinstance(itype, (BinaryType, FixedType)):
            glue_type = "BINARY"
            column_type = "binary"
        elif not getattr(itype, "is_primitive", False):
            reason = "nested_type"
        if (
            ndv == "off"
            and glue_type is not None
            and glue_type
            not in (
                "BOOLEAN",
                "BINARY",
            )
        ):
            glue_type = None
            reason = "ndv_disabled"
        result.append(
            ColumnStatsPlan(
                column_name,
                expr,
                glue_type,
                skip_reason=None if glue_type is not None else reason,
                column_type=column_type,
            )
        )
    return result


def collect_column_statistics(
    con: Any,
    *,
    table_ref: str,
    snapshot_version: int,
    plans: list[ColumnStatsPlan],
    sample_percent: float | None = None,
) -> list[ColumnStat]:
    if snapshot_version < 0:
        raise ValueError("snapshot_version must be >= 0")
    if sample_percent is not None and not 0 < sample_percent <= 100:
        raise ValueError("sample_percent must be between 0 and 100")
    publishable = [plan for plan in plans if plan.glue_type is not None]
    if not publishable:
        return []
    relation = _qualified_table(table_ref)
    columns = ["count(*) AS row_count"]
    for index, plan in enumerate(publishable):
        col = plan.duckdb_expr
        kind = plan.glue_type
        columns.append(f"count(*) FILTER (WHERE {col} IS NULL) AS c{index}_nulls")
        if kind == "BOOLEAN":
            columns.extend(
                [
                    f"count(*) FILTER (WHERE {col}) AS c{index}_true",
                    f"count(*) FILTER (WHERE NOT {col}) AS c{index}_false",
                ]
            )
        elif kind in ("STRING", "BINARY"):
            length_expr = (
                f"strlen({col})" if kind == "STRING" else f"octet_length({col})"
            )
            columns.extend(
                [
                    f"max({length_expr}) AS c{index}_maxlen",
                    f"avg({length_expr}) AS c{index}_avglen",
                ]
            )
            if kind == "STRING":
                columns.append(f"approx_count_distinct({col}) AS c{index}_ndv")
        else:
            columns.extend(
                [
                    f"approx_count_distinct({col}) AS c{index}_ndv",
                    f"min({col}) AS c{index}_min",
                    f"max({col}) AS c{index}_max",
                ]
            )
    sample = (
        f" USING SAMPLE {sample_percent:g} PERCENT (bernoulli, 42)"
        if sample_percent is not None
        else ""
    )
    sql = (
        f"SELECT {', '.join(columns)} FROM {relation} "  # noqa: S608
        f"AT (VERSION => {int(snapshot_version)}){sample}"
    )
    raw = getattr(con, "raw", con)
    with worker_cursor(raw) as cursor:
        result = cursor.execute(sql)
        row = result.fetchone()
        if row is None:
            return []
        data = dict(zip([entry[0] for entry in result.description], row, strict=True))
    scale = 100.0 / sample_percent if sample_percent is not None else 1.0
    rows_scanned = int(data["row_count"] or 0)
    stats: list[ColumnStat] = []
    for index, plan in enumerate(publishable):
        kind = plan.glue_type
        if kind is None:
            continue
        nulls = int(data[f"c{index}_nulls"] or 0)
        if sample_percent is not None:
            nulls = round(nulls * scale)
        ndv = int(data.get(f"c{index}_ndv") or 0)
        minimum, maximum = data.get(f"c{index}_min"), data.get(f"c{index}_max")
        if kind == "DATE":
            minimum = minimum.date() if isinstance(minimum, datetime) else minimum
            maximum = maximum.date() if isinstance(maximum, datetime) else maximum
            minimum = (
                datetime.combine(minimum, datetime.min.time(), UTC)
                if isinstance(minimum, date)
                else minimum
            )
            maximum = (
                datetime.combine(maximum, datetime.min.time(), UTC)
                if isinstance(maximum, date)
                else maximum
            )
        if kind == "DECIMAL":
            minimum = maximum = None
        stats.append(
            ColumnStat(
                column_name=plan.column_name,
                glue_type=kind,
                number_of_nulls=nulls,
                number_of_distinct_values=ndv,
                minimum=minimum,
                maximum=maximum,
                maximum_length=int(data.get(f"c{index}_maxlen") or 0),
                average_length=float(data.get(f"c{index}_avglen") or 0.0),
                number_of_trues=int(data.get(f"c{index}_true") or 0),
                number_of_falses=int(data.get(f"c{index}_false") or 0),
                rows_scanned=rows_scanned,
                column_type=plan.column_type,
            )
        )
    return stats


def publish_column_statistics(
    glue: Any,
    *,
    database: str,
    table: str,
    stats: list[ColumnStat],
    catalog_id: str | None = None,
    analyzed_at: datetime | None = None,
) -> GlueStatsReport:
    started = time.perf_counter_ns()
    report = GlueStatsReport(table=table)
    report.rows_scanned = stats[0].rows_scanned if stats else 0
    report.ndv_estimated = any(
        stat.glue_type not in ("BOOLEAN", "BINARY") for stat in stats
    )
    analyzed_at = analyzed_at or datetime.now(UTC)
    for offset in range(0, len(stats), GLUE_STATS_BATCH):
        batch = stats[offset : offset + GLUE_STATS_BATCH]
        kwargs: dict[str, Any] = {
            "DatabaseName": database,
            "TableName": table,
            "ColumnStatisticsList": [stat.to_glue(analyzed_at) for stat in batch],
        }
        if catalog_id is not None:
            kwargs["CatalogId"] = catalog_id
        attempts = 0

        def call(kwargs: dict[str, Any] = kwargs) -> Any:
            nonlocal attempts
            attempts += 1
            return glue.update_column_statistics_for_table(**kwargs)

        # the call overwrites the statistics: resending is idempotent
        response = call_with_retry(call, phase="glue_stats")
        report.batches += 1
        report.api_calls += attempts
        errors = response.get("Errors") or []
        report.errors.extend(_column_error(error) for error in errors)
        failed = {error["column"] for error in report.errors}
        report.columns_published += sum(
            stat.column_name not in failed for stat in batch
        )
    report.duration_ms = max(0, (time.perf_counter_ns() - started) // 1_000_000)
    return report


def _column_error(error: dict[str, Any]) -> dict[str, str]:
    """Flattened ``Errors[]`` item (``ColumnStatisticsError``).

    In the botocore model the column name comes in
    ``ColumnStatistics.ColumnName`` (the failed item), not at the top level.
    """
    stats = error.get("ColumnStatistics") or {}
    detail = error.get("Error") or {}
    return {
        "column": str(stats.get("ColumnName", "")),
        "code": str(detail.get("ErrorCode", "")),
        "message": str(detail.get("ErrorMessage", "")),
    }


def _qualified_table(table_ref: str) -> str:
    parts = table_ref.split(".")
    if len(parts) != 3 or any(not part.replace("_", "a").isalnum() for part in parts):
        raise ValueError("table_ref must be catalog.schema.table")
    return ".".join(f'"{part}"' for part in parts)
