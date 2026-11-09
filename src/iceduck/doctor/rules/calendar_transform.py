"""Rule `calendar-transform`.

DuckLake's `year`/`month`/`day`/`hour` extract a calendar component; the
same-named Iceberg transforms count from 1970. Declaring one as the other
produces incomplete pruning and results, with no error. The safe fallback is
to export unpartitioned: a degradation, not a blocker. A historical spec
becomes `INFO` (it only affects incremental republishing of old specs).
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

import pyarrow as pa

from iceduck.doctor.model import DoctorContext, Finding, Severity
from iceduck.doctor.registry import register
from iceduck.doctor.sql import as_int

CALENDAR_TRANSFORMS: Final[frozenset[str]] = frozenset({"year", "month", "day", "hour"})
MAPPABLE_TRANSFORMS: Final[dict[str, str]] = {
    "identity": "identity",
    "epoch_year": "year",
    "epoch_month": "month",
    "epoch_day": "day",
    "epoch_hour": "hour",
}
_BUCKET_RE: Final[re.Pattern[str]] = re.compile(
    r"\Abucket\s*[(\[]\s*(?P<n>\d+)\s*[)\]]\Z"
)


def _sql(ctx: DoctorContext) -> str:
    partition_info = ctx.meta("ducklake_partition_info")
    partition_column = ctx.meta("ducklake_partition_column")
    column = ctx.meta("ducklake_column")
    return f"""\
WITH spec AS (
    SELECT
        pi.partition_id,
        pi.begin_snapshot,
        pi.end_snapshot,
        ($snapshot_id >= pi.begin_snapshot
         AND (pi.end_snapshot IS NULL
              OR $snapshot_id < pi.end_snapshot)) AS is_current
    FROM {partition_info} pi
    WHERE pi.table_id = $table_id
)
SELECT
    s.partition_id,
    s.is_current,
    pc.partition_key_index,
    pc.column_id,
    c.column_name,
    c.column_type,
    lower(trim(pc.transform)) AS transform
FROM spec s
JOIN {partition_column} pc
  ON pc.partition_id = s.partition_id
 AND pc.table_id     = $table_id
LEFT JOIN {column} c
  ON c.table_id  = $table_id
 AND c.column_id = pc.column_id
 AND $snapshot_id >= c.begin_snapshot
 AND (c.end_snapshot IS NULL OR $snapshot_id < c.end_snapshot)
ORDER BY s.is_current DESC, s.partition_id, pc.partition_key_index"""


def classify(table: pa.Table, ctx: DoctorContext, rule_id: str) -> list[Finding]:
    findings: list[Finding] = []
    for row in table.to_pylist():
        transform = str(row["transform"])
        if transform in MAPPABLE_TRANSFORMS:
            continue
        if _BUCKET_RE.match(transform):
            findings.append(_bucket_info(row, transform, ctx, rule_id))
            continue
        # calendar or unknown
        is_current = bool(row["is_current"])
        findings.append(_problem(row, transform, is_current, ctx, rule_id))
    return findings


def _bucket_info(
    row: dict[str, object], transform: str, ctx: DoctorContext, rule_id: str
) -> Finding:
    column_name = _col(row)
    return Finding(
        rule_id=rule_id,
        severity=Severity.INFO,
        subject=_subject(row),
        detail=(
            f"transform {transform!r} on {column_name}: the bucket formula is "
            f"identical in both formats; validate by sampling"
        ),
        remediation="validate the pruning by sampling",
        data={"transform": transform, "column_id": _cid(row)},
    )


def _problem(
    row: dict[str, object],
    transform: str,
    is_current: bool,
    ctx: DoctorContext,
    rule_id: str,
) -> Finding:
    is_calendar = transform in CALENDAR_TRANSFORMS
    if is_current:
        severity = Severity.DEGRADED
        if is_calendar:
            detail = (
                f"DuckLake transform {transform!r} extracts the calendar "
                f"component; the same-named Iceberg transform counts from 1970. "
                f"Declaring one as the other would silently produce incorrect "
                f"pruning and results"
            )
        else:
            detail = (
                f"transform {transform!r} cannot be mapped to Iceberg; the table "
                f"will be exported as unpartitioned"
            )
        remediation = (
            "the table will be exported as unpartitioned (correct, but without "
            "partition pruning); to keep pruning, repartition at the source "
            "with epoch_*"
        )
    else:
        severity = Severity.INFO
        detail = (
            f"historical spec uses transform {transform!r}; relevant to the "
            f"incremental sync, which republishes historical specs"
        )
        remediation = "none needed for now"
    return Finding(
        rule_id=rule_id,
        severity=severity,
        subject=_subject(row),
        detail=detail,
        remediation=remediation,
        data={
            "transform": transform,
            "column_id": _cid(row),
            "is_current": is_current,
        },
    )


def _col(row: dict[str, object]) -> str:
    name = row["column_name"]
    return str(name) if name is not None else f"column_id={_cid(row)}"


def _cid(row: dict[str, object]) -> int:
    return as_int(row["column_id"])


def _subject(row: dict[str, object]) -> str:
    name = row["column_name"]
    if name is None:
        return f"column column_id={_cid(row)}"
    return f"column {name} (column_id={_cid(row)})"


@dataclass(frozen=True, slots=True)
class CalendarTransformRule:
    id: str = "calendar-transform"
    title: str = "Calendar transform"
    severity: Severity = Severity.DEGRADED
    remediation: str = "export unpartitioned or repartition with epoch_*"
    requires: tuple[str, ...] = ()

    def check(self, ctx: DoctorContext) -> Sequence[Finding]:
        table = ctx.query(_sql(ctx), ctx.bind)
        return classify(table, ctx, self.id)


RULE = register(CalendarTransformRule())
