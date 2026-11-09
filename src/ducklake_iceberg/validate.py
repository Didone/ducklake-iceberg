"""Data reconciliation between DuckLake and Iceberg.

Both sides read the **same** Parquet files: any divergence is a metadata defect
or an external write. The DuckLake side is always read at the published snapshot
(``AT (VERSION => S_pub)``) and the Iceberg side is registered on the **same**
DuckDB connection, so the same SQL aggregates both.
"""

from __future__ import annotations

import hashlib
import math
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal, cast
from uuid import UUID

from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    BooleanExpression,
    EqualTo,
    GreaterThanOrEqual,
    In,
    LessThan,
    LessThanOrEqual,
)
from rich.console import Console
from rich.table import Table as RichTable

from ducklake_iceberg.catalog.reader import snapshot_at, table_schema_version
from ducklake_iceberg.doctor.model import quote_ident
from ducklake_iceberg.errors import DivergenceError, ExitCode, IdentityError
from ducklake_iceberg.mapping.types import base_type_name, resolve_type
from ducklake_iceberg.sync.state import PROP_SCHEMA_VERSION

if TYPE_CHECKING:
    from pyiceberg.table import Table as IcebergTable

    from ducklake_iceberg.catalog.connection import DuckLakeConnection


class CheckId(StrEnum):
    ROW_COUNT = "row_count"
    COLUMN_AGGREGATES = "column_aggregates"
    PARTITION_PREDICATE = "partition_predicate"
    BOUNDS_PREDICATE = "bounds_predicate"
    ROW_SAMPLE = "row_sample"


class CheckStatus(StrEnum):
    PASS = "pass"
    DIVERGENT = "divergent"
    SKIPPED = "skipped"
    ERROR = "error"


DivergenceKind = Literal["scalar", "row_missing", "row_extra", "field", "column"]
PredicateKind = Literal["frequent", "rare", "absent", "range"]


@dataclass(frozen=True, slots=True)
class Tolerances:
    """Every applied tolerance is reported; none is silent."""

    float_rtol: float = 1e-9
    float_atol: float = 0.0
    timestamp_precision: Literal["us", "ms"] = "us"


@dataclass(frozen=True, slots=True)
class ValidateOptions:
    deep_count: bool = False
    sample_rows: int = 1_000
    max_in_values: int = 1_000
    max_columns: int = 64
    max_predicate_checks: int = 2
    materialize_limit_rows: int = 5_000_000
    strict: bool = False
    tolerances: Tolerances = Tolerances()


@dataclass(frozen=True, slots=True)
class ComparedColumn:
    """Live top-level scalar column in the published snapshot."""

    name: str
    kind: str  # base DuckLake type name (``int32``, ``date``, ...)
    order: int


@dataclass(frozen=True, slots=True)
class PredicateChoice:
    """Predicate value chosen deterministically on the DuckLake side.

    ``range`` is the interval ``[value, upper)`` (``[value, upper]`` when
    ``upper_inclusive``). ``sql_literal`` is for display only: queries use
    bound parameters.
    """

    column: str
    iceberg_type: str
    value: Any
    kind: PredicateKind
    expected_rows: int
    sql_literal: str
    upper: Any = None
    upper_inclusive: bool = False


@dataclass(frozen=True, slots=True)
class Divergence:
    kind: DivergenceKind
    column: str | None
    lake_value: Any
    iceberg_value: Any
    key: dict[str, Any] | None = None


@dataclass(slots=True)
class CheckResult:
    id: CheckId
    status: CheckStatus
    duration_ms: int
    details: dict[str, Any]
    divergences: list[Divergence] = field(default_factory=list)
    skip_reason: str | None = None
    warnings: list[str] = field(default_factory=list)

    @property
    def message(self) -> str:
        return _check_message(self)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id.value,
            "status": self.status.value,
            "duration_ms": self.duration_ms,
            "message": self.message,
            "details": self.details,
            "divergences": [asdict(item) for item in self.divergences],
            "warnings": self.warnings,
            "skip_reason": self.skip_reason,
        }


@dataclass(slots=True)
class ValidationReport:
    schema_name: str
    table_name: str
    glue_identifier: tuple[str, str]
    ducklake_snapshot_id: int
    iceberg_snapshot_id: int
    checks: list[CheckResult]
    tolerances_applied: list[str]
    external_write: bool = False
    published_iceberg_snapshot_id: int | None = None
    excluded_columns: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def exit_code(self) -> ExitCode:
        """0 ok · 3 divergence · 1 error · 4 nothing to validate.

        Error wins over divergence; divergence wins over warning. An external
        write is always a divergence, even when the checks pass.
        """
        if any(result.status is CheckStatus.ERROR for result in self.checks):
            return ExitCode.ERROR
        if self.external_write or any(
            result.status is CheckStatus.DIVERGENT for result in self.checks
        ):
            return ExitCode.DIVERGED
        row_count = next(
            (check for check in self.checks if check.id is CheckId.ROW_COUNT), None
        )
        if (
            row_count is not None
            and row_count.details.get("ducklake_rows") == 0
            and row_count.details.get("iceberg_metadata_rows") == 0
        ):
            return ExitCode.NOTHING_TO_DO
        if all(result.status is CheckStatus.SKIPPED for result in self.checks):
            return ExitCode.NOTHING_TO_DO
        return ExitCode.SUCCESS

    @property
    def message(self) -> str:
        """Summary line."""
        code = self.exit_code
        if self.external_write:
            return (
                f"external write detected: Iceberg snapshot "
                f"{self.iceberg_snapshot_id} ≠ published "
                f"{self.published_iceberg_snapshot_id}"
            )
        if code is ExitCode.NOTHING_TO_DO:
            return "validate: table is empty on both sides; nothing to validate"
        passed = sum(1 for check in self.checks if check.status is CheckStatus.PASS)
        divergent = [
            check.id.value
            for check in self.checks
            if check.status is CheckStatus.DIVERGENT
        ]
        if divergent:
            return (
                f"validate: {len(divergent)}/{len(self.checks)} checks "
                f"divergent ({', '.join(divergent)}) "
                f"(snapshot {self.ducklake_snapshot_id})"
            )
        return (
            f"validate: {passed}/{len(self.checks)} checks ok "
            f"(snapshot {self.ducklake_snapshot_id})"
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "table": f"{self.schema_name}.{self.table_name}",
            "ducklake_snapshot_id": self.ducklake_snapshot_id,
            "iceberg_snapshot_id": self.iceberg_snapshot_id,
            "published_iceberg_snapshot_id": self.published_iceberg_snapshot_id,
            "tolerances_applied": self.tolerances_applied,
            "external_write": self.external_write,
            "excluded_columns": self.excluded_columns,
            "warnings": self.warnings,
            "message": self.message,
            "checks": [check.as_dict() for check in self.checks],
            "exit_code": int(self.exit_code),
        }


def empty_validation_result(table: str) -> dict[str, Any]:
    """``results[]`` item for a table that is empty on both sides (exit 4)."""
    return {
        "table": table,
        "ducklake_snapshot_id": None,
        "iceberg_snapshot_id": None,
        "published_iceberg_snapshot_id": None,
        "tolerances_applied": [],
        "external_write": False,
        "excluded_columns": [],
        "warnings": [],
        "message": "validate: table is empty on both sides; nothing to validate",
        "checks": [],
        "exit_code": int(ExitCode.NOTHING_TO_DO),
    }


def lake_row_count(con: DuckLakeConnection, schema_name: str, table_name: str) -> int:
    """Rows of the table in the current snapshot (includes inlined data)."""
    relation = f"{_quote(con.alias)}.{_quote(schema_name)}.{_quote(table_name)}"
    return int(_scalar(con, f"SELECT count(*) FROM {relation}") or 0)


_INTEGER_TYPES = frozenset(
    {"int8", "int16", "int32", "int64", "uint8", "uint16", "uint32"}
)
_FLOAT_TYPES = frozenset({"float32", "float64"})
_NUMERIC_TYPES = _INTEGER_TYPES | _FLOAT_TYPES | frozenset({"decimal"})
_TIME_TYPES = frozenset({"date", "timestamp", "timestamptz"})
_TIMESTAMP_TYPES = frozenset({"timestamp", "timestamptz"})
_ORDERABLE_TYPES = _NUMERIC_TYPES | _TIME_TYPES | frozenset({"varchar", "uuid"})
_RANGE_TYPES = _NUMERIC_TYPES | _TIME_TYPES
_AGGREGATABLE_TYPES = _ORDERABLE_TYPES | frozenset({"boolean"})
_EXCLUDED_TYPES = frozenset({"blob", "json", "struct", "list", "map", "variant"})
_MAX_DIVERGENCES = 50
_PARTITION_KINDS: tuple[PredicateKind, ...] = ("frequent", "rare", "absent")
_BOUNDS_KINDS: tuple[PredicateKind, ...] = ("rare", "range", "absent")


def resolve_published_snapshot(table: IcebergTable) -> int:
    """Read ``ducklake.snapshot-id`` from the table properties."""
    value = table.properties.get("ducklake.snapshot-id")
    if value is None:
        raise IdentityError(
            "table has no ducklake.snapshot-id",
            hint="publish again with 'ducklake-iceberg sync'",
        )
    try:
        return int(value)
    except ValueError as exc:
        raise IdentityError(
            f"invalid ducklake.snapshot-id property: {value!r}",
            hint="publish again with 'ducklake-iceberg sync'",
        ) from exc


def _column_metadata(
    con: DuckLakeConnection, *, table_id: int, snapshot_id: int
) -> tuple[list[ComparedColumn], list[str]]:
    rows = con.query_arrow(
        f"SELECT column_name, column_type, column_order, parent_column "
        f"FROM {quote_ident(con.meta)}.ducklake_column "
        "WHERE table_id = $table_id AND $snapshot_id >= begin_snapshot "
        "AND (end_snapshot IS NULL OR $snapshot_id < end_snapshot) "
        "ORDER BY column_order",
        {"table_id": table_id, "snapshot_id": snapshot_id},
    ).to_pylist()
    columns: list[ComparedColumn] = []
    excluded: list[str] = []
    for row in rows:
        name = str(row["column_name"])
        raw_type = str(row["column_type"])
        kind = base_type_name(raw_type)
        if row["parent_column"] is not None:
            continue
        if (
            kind in _EXCLUDED_TYPES
            or not kind
            or resolve_type(raw_type).iceberg is None
        ):
            excluded.append(name)
            continue
        columns.append(ComparedColumn(name, kind, int(row["column_order"])))
    return columns, excluded


def _partitioned_columns(
    con: DuckLakeConnection, *, table_id: int, snapshot_id: int
) -> dict[str, str]:
    """Column → transform of the partitions live in the snapshot."""
    rows = con.query_arrow(
        f"SELECT DISTINCT c.column_name, pc.transform "
        f"FROM {quote_ident(con.meta)}.ducklake_partition_info pi "
        f"JOIN {quote_ident(con.meta)}.ducklake_partition_column pc "
        "ON pc.partition_id = pi.partition_id AND pc.table_id = pi.table_id "
        f"JOIN {quote_ident(con.meta)}.ducklake_column c "
        "ON c.table_id = pi.table_id AND c.column_id = pc.column_id "
        "AND $snapshot_id >= c.begin_snapshot "
        "AND (c.end_snapshot IS NULL OR $snapshot_id < c.end_snapshot) "
        "WHERE pi.table_id = $table_id AND $snapshot_id >= pi.begin_snapshot "
        "AND (pi.end_snapshot IS NULL OR $snapshot_id < pi.end_snapshot)",
        {"table_id": table_id, "snapshot_id": snapshot_id},
    ).to_pylist()
    return {str(row["column_name"]): str(row["transform"]) for row in rows}


def _duration(started: float) -> int:
    return round((time.perf_counter() - started) * 1000)


def _check(
    check_id: CheckId,
    started: float,
    *,
    details: dict[str, Any],
    divergences: list[Divergence] | None = None,
    skip_reason: str | None = None,
    warnings: list[str] | None = None,
) -> CheckResult:
    diffs = divergences or []
    status = (
        CheckStatus.DIVERGENT
        if diffs
        else CheckStatus.SKIPPED
        if skip_reason
        else CheckStatus.PASS
    )
    return CheckResult(
        check_id,
        status,
        _duration(started),
        details,
        diffs[:_MAX_DIVERGENCES],
        skip_reason,
        list(warnings or []),
    )


def _equal(left: Any, right: Any, tolerances: Tolerances, *, floating: bool) -> bool:
    if left is None or right is None:
        return left is right
    if (
        isinstance(left, datetime)
        and isinstance(right, datetime)
        and tolerances.timestamp_precision == "ms"
    ):
        return left.isoformat(timespec="milliseconds") == right.isoformat(
            timespec="milliseconds"
        )
    if floating:
        a, b = float(left), float(right)
        if math.isnan(a) or math.isnan(b):
            return math.isnan(a) and math.isnan(b)
        return abs(a - b) <= tolerances.float_atol + tolerances.float_rtol * abs(b)
    return bool(left == right)


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _scalar(con: DuckLakeConnection, sql: str, params: list[Any] | None = None) -> Any:
    row = _fetchone(con, sql, params)
    return None if row is None else row[0]


def _fetchone(
    con: DuckLakeConnection, sql: str, params: list[Any] | None = None
) -> tuple[Any, ...] | None:
    rows = con.raw.execute(sql, params or []).fetchall()
    return rows[0] if rows else None


def _display(value: Any) -> str:
    if isinstance(value, str | UUID | date):
        return f"'{value}'"
    return str(value)


def register_iceberg_scan(
    con: DuckLakeConnection,
    table: IcebergTable,
    *,
    name: str,
    row_filter: BooleanExpression,
    selected_fields: tuple[str, ...],
    streaming: bool,
) -> str:
    """Register the Iceberg scan on the **same** connection and return its name.

    ``streaming`` registers a ``RecordBatchReader`` (consumable only once,
    without materializing the table); otherwise ``to_arrow()`` is used.
    """
    scan = table.scan(row_filter=row_filter, selected_fields=selected_fields)
    source: Any = scan.to_arrow_batch_reader() if streaming else scan.to_arrow()
    con.raw.register(name, source)
    return name


def pick_predicate_values(
    con: DuckLakeConnection,
    *,
    relation: str,
    columns: Sequence[ComparedColumn],
    kind: PredicateKind,
    limit: int,
) -> list[PredicateChoice]:
    """Pick predicate values deterministically.

    Tie-breaking by ``md5(CAST(v AS VARCHAR))`` makes the choice stable across
    runs. ``absent`` is a value guaranteed to be missing (negative control);
    ``range`` is the narrow band ``[q50, q51)`` via ``quantile_disc``, exact
    and deterministic.
    """
    choices: list[PredicateChoice] = []
    for column in columns[:limit]:
        ident = _quote(column.name)
        choice: PredicateChoice | None = None
        if kind in {"frequent", "rare"}:
            order = "DESC" if kind == "frequent" else "ASC"
            row = _fetchone(
                con,
                f"SELECT {ident} AS v, count(*) AS n FROM {relation} "
                f"WHERE {ident} IS NOT NULL GROUP BY 1 "
                f"ORDER BY n {order}, md5(CAST({ident} AS VARCHAR)) LIMIT 1",
            )
            if row is not None:
                choice = PredicateChoice(
                    column.name,
                    column.kind,
                    row[0],
                    kind,
                    int(row[1]),
                    f"{ident} = {_display(row[0])}",
                )
        elif kind == "absent":
            maximum = _scalar(con, f"SELECT max({ident}) FROM {relation}")
            absent = None if maximum is None else _absent_value(column.kind, maximum)
            if absent is not None:
                count_sql = f"SELECT count(*) FROM {relation} WHERE {ident} = ?"
                while int(_scalar(con, count_sql, [absent]) or 0):
                    if not isinstance(absent, str):
                        absent = None
                        break
                    absent = f"{absent}_"
            if absent is not None:
                choice = PredicateChoice(
                    column.name,
                    column.kind,
                    absent,
                    "absent",
                    0,
                    f"{ident} = {_display(absent)}",
                )
        elif column.kind in _RANGE_TYPES:
            bounds = _fetchone(
                con,
                f"SELECT quantile_disc({ident}, 0.50), quantile_disc({ident}, 0.51) "
                f"FROM {relation} WHERE {ident} IS NOT NULL",
            )
            if bounds is not None and bounds[0] is not None:
                lo, hi = bounds
                inclusive = lo == hi
                op = "<=" if inclusive else "<"
                rows = int(
                    _scalar(
                        con,
                        f"SELECT count(*) FROM {relation} "
                        f"WHERE {ident} >= ? AND {ident} {op} ?",
                        [lo, hi],
                    )
                    or 0
                )
                choice = PredicateChoice(
                    column.name,
                    column.kind,
                    lo,
                    "range",
                    rows,
                    f"{ident} >= {_display(lo)} AND {ident} {op} {_display(hi)}",
                    upper=hi,
                    upper_inclusive=inclusive,
                )
        if choice is not None:
            choices.append(choice)
    return choices


def _iceberg_filter(choice: PredicateChoice) -> BooleanExpression:
    if choice.kind != "range":
        return EqualTo(term=choice.column, value=choice.value)
    upper: BooleanExpression = (
        LessThanOrEqual(term=choice.column, value=choice.upper)
        if choice.upper_inclusive
        else LessThan(term=choice.column, value=choice.upper)
    )
    return And(GreaterThanOrEqual(term=choice.column, value=choice.value), upper)


def _iceberg_rows(
    table: IcebergTable, choice: PredicateChoice, *, plan: bool = False
) -> tuple[int, int | None]:
    """Rows actually read (never ``scan.count()``) and planned files."""
    scan = table.scan(
        row_filter=_iceberg_filter(choice), selected_fields=(choice.column,)
    )
    rows = sum(batch.num_rows for batch in scan.to_arrow_batch_reader())
    planned = len(list(scan.plan_files())) if plan else None
    return rows, planned


def _choice_detail(
    choice: PredicateChoice, iceberg_rows: int, **extra: Any
) -> dict[str, Any]:
    return {
        "column": choice.column,
        "kind": choice.kind,
        "value": choice.value,
        "upper": choice.upper,
        "predicate": choice.sql_literal,
        "ducklake_rows": choice.expected_rows,
        "iceberg_rows": iceberg_rows,
        **extra,
    }


def _row_count_hints(
    lake_rows: int, metadata_rows: int, summary_rows: int, deep_rows: int | None
) -> list[str]:
    """Diagnostic hints: which side is probably wrong."""
    hints: list[str] = []
    if metadata_rows != summary_rows:
        hints.append("manifest and snapshot summary disagree: writer defect")
    if lake_rows > 0 and metadata_rows == 0:
        hints.append(
            "pending inlined data? run 'ducklake-iceberg checkpoint' and then "
            "'ducklake-iceberg sync'"
        )
    elif lake_rows != metadata_rows and deep_rows is None:
        hints.append(
            "rerun with --deep to check that the files exist and have the rows"
        )
    if deep_rows is not None and deep_rows < metadata_rows:
        hints.append(
            "file removed by DuckLake maintenance? run "
            "'ducklake-iceberg validate --only retention --check-s3 full'"
        )
    return hints


def validate_table(
    con: DuckLakeConnection,
    table: IcebergTable,
    *,
    lake_alias: str,
    meta_catalog: str,
    schema_name: str,
    table_name: str,
    table_id: int,
    iceberg_identifier: tuple[str, str],
    options: ValidateOptions,
) -> ValidationReport:
    published_snapshot = resolve_published_snapshot(table)
    try:
        lake_snapshot = snapshot_at(con, published_snapshot)
    except IdentityError as exc:
        raise DivergenceError(
            f"snapshot {published_snapshot} no longer exists in DuckLake",
            hint="review the snapshot retention and run 'ducklake-iceberg sync'",
        ) from exc
    iceberg_snapshot = table.current_snapshot()
    if iceberg_snapshot is None:
        raise IdentityError("Iceberg table has no current snapshot")
    iceberg_snapshot_id = int(iceberg_snapshot.snapshot_id)
    # Same criterion as `status`: current snapshot != the published one.
    raw_published_iceberg = table.properties.get("ducklake_iceberg.iceberg-snapshot-id")
    published_iceberg_id = (
        int(raw_published_iceberg) if raw_published_iceberg is not None else None
    )
    external_write = (
        published_iceberg_id is not None and published_iceberg_id != iceberg_snapshot_id
    )

    # DuckDB does not bind identifiers; each component goes through _quote.
    relation = (
        f"{_quote(lake_alias)}.{_quote(schema_name)}.{_quote(table_name)} "
        f"AT (VERSION => {published_snapshot})"
    )
    con.raw.execute(
        f"CREATE OR REPLACE TEMP VIEW lake_side AS SELECT * FROM {relation}"
    )
    columns, excluded = _column_metadata(
        con, table_id=table_id, snapshot_id=published_snapshot
    )
    report_warnings: list[str] = []

    # Column intersection: a column only in DuckLake becomes a `column`
    # divergence in (b); a column only in Iceberg is a warning (schema changed
    # externally).
    iceberg_names = {f.name for f in table.schema().fields}
    lake_names = {c.name for c in columns} | set(excluded)
    lake_only = [c for c in columns if c.name not in iceberg_names]
    iceberg_only = sorted(iceberg_names - lake_names)
    columns = [c for c in columns if c.name in iceberg_names]
    comparable = columns[: options.max_columns]
    excluded.extend(column.name for column in columns[options.max_columns :])
    if iceberg_only:
        report_warnings.append(f"columns only in Iceberg: {', '.join(iceberg_only)}")
    published_schema = table.properties.get(PROP_SCHEMA_VERSION)
    lake_schema = table_schema_version(con, table_id, lake_snapshot)
    schema_drift = published_schema is not None and str(lake_schema) != str(
        published_schema
    )
    if schema_drift:
        report_warnings.append(
            f"schema_drift: schema_version DuckLake {lake_schema} vs "
            f"{PROP_SCHEMA_VERSION} {published_schema}"
        )
    partitioned = _partitioned_columns(
        con, table_id=table_id, snapshot_id=published_snapshot
    )
    tolerance_list = (
        [f"timestamp_precision={options.tolerances.timestamp_precision}"]
        if options.tolerances.timestamp_precision == "ms"
        else []
    )
    checks: list[CheckResult] = []

    # (a) Count: metadata kept separate from the physical read (--deep).
    started = time.perf_counter()
    lake_rows = int(_scalar(con, "SELECT count(*) FROM lake_side") or 0)
    scan = table.scan()
    metadata_rows = int(scan.count())
    summary = cast(dict[str, str] | None, iceberg_snapshot.summary)
    summary_total = int((summary or {}).get("total-records", str(metadata_rows)) or 0)
    # `total-records` counts the rows of the data files, before deletes; with
    # position deletes (only visible positions) the live count is the difference.
    summary_deletes = int((summary or {}).get("total-position-deletes", "0") or 0)
    summary_rows = summary_total - summary_deletes
    deep_rows: int | None = None
    if options.deep_count:
        deep_rows = sum(batch.num_rows for batch in scan.to_arrow_batch_reader())
    count_values = [lake_rows, metadata_rows, summary_rows]
    if deep_rows is not None:
        count_values.append(deep_rows)
    checks.append(
        _check(
            CheckId.ROW_COUNT,
            started,
            details={
                "ducklake_rows": lake_rows,
                "iceberg_metadata_rows": metadata_rows,
                "iceberg_summary_total_records": summary_total,
                "iceberg_summary_position_deletes": summary_deletes,
                "iceberg_summary_live_records": summary_rows,
                "iceberg_deep_rows": deep_rows,
                "external_write": external_write,
                "hints": _row_count_hints(
                    lake_rows, metadata_rows, summary_rows, deep_rows
                ),
            },
            divergences=[
                Divergence(
                    "scalar",
                    None,
                    lake_rows,
                    deep_rows if deep_rows is not None else metadata_rows,
                )
            ]
            if len(set(count_values)) > 1
            else [],
        ),
    )

    # (b) Per-column aggregates: the same SQL on both sides, in the same DuckDB.
    started = time.perf_counter()
    aggregatable = [c for c in comparable if c.kind in _AGGREGATABLE_TYPES]
    agg_details: dict[str, Any] = {}
    agg_diffs: list[Divergence] = [
        Divergence("column", column.name, "present in DuckLake", None)
        for column in lake_only
    ]
    agg_warnings = list(report_warnings)
    if options.strict and schema_drift:
        agg_diffs.append(Divergence("column", None, lake_schema, published_schema))
    if options.strict and iceberg_only:
        agg_diffs.extend(
            Divergence("column", name, None, "present in Iceberg")
            for name in iceberg_only
        )
    if aggregatable:
        expressions, aliases = _aggregate_expressions(aggregatable, options)
        select = ", ".join(expressions)
        lake_values = _fetchone(con, f"SELECT {select} FROM lake_side")
        ice_rel = register_iceberg_scan(
            con,
            table,
            name="ice_aggregate_side",
            row_filter=AlwaysTrue(),
            selected_fields=tuple(c.name for c in aggregatable),
            streaming=True,
        )
        iceberg_values = _fetchone(con, f"SELECT {select} FROM {ice_rel}")
        if lake_values is None or iceberg_values is None:
            raise RuntimeError("aggregates query returned no row")
        for index, (label, floating) in enumerate(aliases):
            lake_value, iceberg_value = lake_values[index], iceberg_values[index]
            agg_details[label] = {"ducklake": lake_value, "iceberg": iceberg_value}
            if not _equal(
                lake_value, iceberg_value, options.tolerances, floating=floating
            ):
                agg_diffs.append(Divergence("scalar", label, lake_value, iceberg_value))
    checks.append(
        _check(
            CheckId.COLUMN_AGGREGATES,
            started,
            details={
                "aggregates": agg_details,
                "excluded_columns": excluded,
                "lake_only_columns": [c.name for c in lake_only],
                "schema_drift": schema_drift,
            },
            divergences=agg_diffs,
            skip_reason="no comparable scalar column" if not aggregatable else None,
            warnings=agg_warnings,
        )
    )

    # (c) Predicates on partitioned columns: frequent, rare, and absent.
    started = time.perf_counter()
    pred_details: list[dict[str, Any]] = []
    pred_diffs: list[Divergence] = []
    partition_columns = [
        c for c in comparable if c.name in partitioned and c.kind in _ORDERABLE_TYPES
    ]
    if lake_rows > 0:
        for kind in _PARTITION_KINDS:
            for choice in pick_predicate_values(
                con,
                relation="lake_side",
                columns=partition_columns,
                kind=kind,
                limit=options.max_predicate_checks,
            ):
                observed, _ = _iceberg_rows(table, choice)
                pred_details.append(
                    _choice_detail(
                        choice, observed, transform=partitioned[choice.column]
                    )
                )
                if observed != choice.expected_rows:
                    pred_diffs.append(
                        Divergence(
                            "scalar", choice.column, choice.expected_rows, observed
                        )
                    )
    checks.append(
        _check(
            CheckId.PARTITION_PREDICATE,
            started,
            details={"predicates": pred_details},
            divergences=pred_diffs,
            skip_reason="empty table"
            if lake_rows == 0
            else "no comparable partitioned column"
            if not partition_columns
            else None,
        )
    )

    # (d) Predicates on unpartitioned columns exercise the bounds.
    started = time.perf_counter()
    bound_details: list[dict[str, Any]] = []
    bound_diffs: list[Divergence] = []
    bound_warnings: list[str] = []
    unpartitioned = [
        c
        for c in comparable
        if c.name not in partitioned and c.kind in _ORDERABLE_TYPES
    ]
    live_files = int(
        _scalar(
            con,
            f"SELECT count(*) FROM {quote_ident(meta_catalog)}.ducklake_data_file "
            "WHERE table_id = ? AND ? >= begin_snapshot "
            "AND (end_snapshot IS NULL OR ? < end_snapshot)",
            [table_id, published_snapshot, published_snapshot],
        )
        or 0
    )
    if lake_rows > 0:
        for kind in _BOUNDS_KINDS:
            for choice in pick_predicate_values(
                con,
                relation="lake_side",
                columns=unpartitioned,
                kind=kind,
                limit=options.max_predicate_checks,
            ):
                observed, planned = _iceberg_rows(table, choice, plan=True)
                no_pruning = (
                    choice.kind == "rare" and live_files > 1 and planned == live_files
                )
                bound_details.append(
                    _choice_detail(
                        choice,
                        observed,
                        planned_files=planned,
                        live_files=live_files,
                    )
                )
                if observed != choice.expected_rows or (
                    planned == 0 and choice.expected_rows > 0
                ):
                    bound_diffs.append(
                        Divergence(
                            "scalar", choice.column, choice.expected_rows, observed
                        )
                    )
                elif no_pruning:
                    warning = f"no pruning on {choice.column}; bounds missing"
                    bound_warnings.append(warning)
                    if options.strict:
                        bound_diffs.append(
                            Divergence("column", choice.column, "pruning", "none")
                        )
    checks.append(
        _check(
            CheckId.BOUNDS_PREDICATE,
            started,
            details={"predicates": bound_details, "live_files": live_files},
            divergences=bound_diffs,
            skip_reason="empty table"
            if lake_rows == 0
            else "no suitable unpartitioned column"
            if not unpartitioned
            else None,
            warnings=bound_warnings,
        )
    )

    # (e) md5 bucket sample: the same DuckDB computes the hash on both sides.
    checks.append(_row_sample(con, table, comparable, lake_rows, options))

    if external_write:
        for check in checks:
            check.details["external_write"] = True

    return ValidationReport(
        schema_name=schema_name,
        table_name=table_name,
        glue_identifier=iceberg_identifier,
        ducklake_snapshot_id=published_snapshot,
        iceberg_snapshot_id=iceberg_snapshot_id,
        checks=checks,
        tolerances_applied=tolerance_list,
        external_write=external_write,
        published_iceberg_snapshot_id=published_iceberg_id,
        excluded_columns=excluded,
        warnings=report_warnings,
    )


def _aggregate_expressions(
    columns: Sequence[ComparedColumn], options: ValidateOptions
) -> tuple[list[str], list[tuple[str, bool]]]:
    """Aggregate expressions and, for each, its label and whether it is a float sum."""
    expressions: list[str] = []
    labels: list[tuple[str, bool]] = []

    def add(expression: str, label: str, *, floating: bool = False) -> None:
        expressions.append(f"{expression} AS a{len(expressions)}")
        labels.append((label, floating))

    truncate_ms = options.tolerances.timestamp_precision == "ms"
    for column in columns:
        ident = _quote(column.name)
        add(f"count(*) FILTER (WHERE {ident} IS NULL)", f"{column.name}.nulls")
        if column.kind == "boolean":
            add(f"count(*) FILTER (WHERE {ident})", f"{column.name}.true")
            add(f"count(*) FILTER (WHERE NOT {ident})", f"{column.name}.false")
            continue
        if column.kind in _NUMERIC_TYPES:
            if column.kind == "decimal":
                add(f"sum({ident})", f"{column.name}.sum")
            elif column.kind in _FLOAT_TYPES:
                add(
                    f"sum(CAST({ident} AS DOUBLE))", f"{column.name}.sum", floating=True
                )
            else:
                add(f"sum(CAST({ident} AS HUGEINT))", f"{column.name}.sum")
        value = (
            f"date_trunc('milliseconds', {ident})"
            if truncate_ms and column.kind in _TIMESTAMP_TYPES
            else ident
        )
        add(f"min({value})", f"{column.name}.min")
        add(f"max({value})", f"{column.name}.max")
    return expressions, labels


def _row_sample(
    con: DuckLakeConnection,
    table: IcebergTable,
    comparable: Sequence[ComparedColumn],
    lake_rows: int,
    options: ValidateOptions,
) -> CheckResult:
    started = time.perf_counter()
    sample_columns = list(comparable[:4])
    if not sample_columns or lake_rows == 0:
        return _check(
            CheckId.ROW_SAMPLE,
            started,
            details={"sample_rows": options.sample_rows},
            skip_reason="empty table or no comparable scalar column",
        )
    names = tuple(column.name for column in comparable)
    key_expr = ", ".join(
        f"CAST({_quote(column.name)} AS VARCHAR)" for column in sample_columns
    )
    bucket_expr = f"substr(md5(concat_ws('|', {key_expr})), 1, 2)"
    prefix_count = min(256, max(1, math.ceil(256 * options.sample_rows / lake_rows)))
    prefixes = [f"{i:02x}" for i in range(prefix_count)]
    quoted_prefixes = ", ".join("'" + prefix + "'" for prefix in prefixes)
    sample_where = f"{bucket_expr} IN ({quoted_prefixes})"
    projection = ", ".join(_quote(name) for name in names)
    lake_sample = con.raw.execute(
        f"SELECT {projection} FROM lake_side "
        f"WHERE {sample_where} ORDER BY {projection} LIMIT ?",
        [options.sample_rows],
    ).fetchall()
    first = sample_columns[0]
    sample_keys = list(dict.fromkeys(row[0] for row in lake_sample))
    sample_filter: BooleanExpression = AlwaysTrue()
    filter_kind = "always_true"
    # The md5 cannot be pushed down to the scan: the filter only reduces the read.
    if (
        lake_rows > options.materialize_limit_rows
        and sample_keys
        and None not in sample_keys
        and first.kind in _ORDERABLE_TYPES
    ):
        if len(sample_columns) == 1 and len(sample_keys) <= options.max_in_values:
            sample_filter = In(term=first.name, literals=sample_keys)
            filter_kind = "in"
        else:
            sample_filter = And(
                GreaterThanOrEqual(term=first.name, value=min(sample_keys)),
                LessThanOrEqual(term=first.name, value=max(sample_keys)),
            )
            filter_kind = "range"
    ice_rel = register_iceberg_scan(
        con,
        table,
        name="ice_sample_side",
        row_filter=sample_filter,
        selected_fields=names,
        streaming=True,
    )
    iceberg_sample = con.raw.execute(
        f"SELECT {projection} FROM {ice_rel} "
        f"WHERE {sample_where} ORDER BY {projection} LIMIT ?",
        [options.sample_rows],
    ).fetchall()
    diffs = _compare_samples(
        lake_sample,
        iceberg_sample,
        names,
        options.tolerances,
        key_width=len(sample_columns),
    )
    digest = hashlib.sha256(repr(lake_sample).encode("utf-8")).hexdigest()[:16]
    return _check(
        CheckId.ROW_SAMPLE,
        started,
        details={
            "sample_rows": options.sample_rows,
            "compared_rows": len(lake_sample),
            "iceberg_rows": len(iceberg_sample),
            "key_columns": [column.name for column in sample_columns],
            "prefixes": prefix_count,
            "iceberg_filter": filter_kind,
            "sample_digest": digest,
        },
        divergences=diffs,
    )


def _absent_value(kind: str, maximum: Any) -> Any | None:
    """Value above the maximum, hence guaranteed absent (negative control)."""
    if kind in _INTEGER_TYPES or kind == "decimal":
        return maximum + 1
    if kind in _FLOAT_TYPES:
        return float(maximum) + max(1.0, abs(float(maximum)))
    if kind == "date" and isinstance(maximum, date):
        try:
            return maximum + timedelta(days=1)
        except OverflowError:
            return None
    if kind in _TIMESTAMP_TYPES and isinstance(maximum, datetime):
        try:
            return maximum + timedelta(days=1)
        except OverflowError:
            return None
    if kind == "uuid":
        try:
            parsed = maximum if isinstance(maximum, UUID) else UUID(str(maximum))
        except ValueError:
            return None
        return UUID(int=(parsed.int + 1) % (1 << 128))
    if kind == "varchar":
        return f"{maximum}~ducklake-iceberg-absent"
    return None


def _compare_samples(
    lake_rows: list[tuple[Any, ...]],
    iceberg_rows: list[tuple[Any, ...]],
    columns: tuple[str, ...],
    tolerances: Tolerances,
    *,
    key_width: int = 4,
) -> list[Divergence]:
    """Classify rows as ``row_missing``, ``row_extra``, or ``field``."""
    width = max(1, min(key_width, len(columns)))

    def floating(left: Any, right: Any) -> bool:
        return isinstance(left, float) or isinstance(right, float)

    def key_of(row: tuple[Any, ...]) -> dict[str, Any]:
        return dict(zip(columns[:width], row[:width], strict=True))

    diffs: list[Divergence] = []
    remaining = list(iceberg_rows)
    for lake_row in lake_rows:
        matched = next(
            (
                index
                for index, ice_row in enumerate(remaining)
                if all(
                    _equal(
                        lake_row[i],
                        ice_row[i],
                        tolerances,
                        floating=floating(lake_row[i], ice_row[i]),
                    )
                    for i in range(width)
                )
            ),
            None,
        )
        if matched is None:
            diffs.append(
                Divergence(
                    "row_missing",
                    None,
                    dict(zip(columns, lake_row, strict=True)),
                    None,
                    key_of(lake_row),
                )
            )
            continue
        ice_row = remaining.pop(matched)
        for index, (lake_value, ice_value) in enumerate(
            zip(lake_row, ice_row, strict=True)
        ):
            if not _equal(
                lake_value,
                ice_value,
                tolerances,
                floating=floating(lake_value, ice_value),
            ):
                diffs.append(
                    Divergence(
                        "field", columns[index], lake_value, ice_value, key_of(lake_row)
                    )
                )
    for ice_row in remaining:
        diffs.append(
            Divergence(
                "row_extra",
                None,
                None,
                dict(zip(columns, ice_row, strict=True)),
                key_of(ice_row),
            )
        )
    return diffs[:_MAX_DIVERGENCES]


def _check_message(check: CheckResult) -> str:
    """Message for a single check."""
    name = check.id.value
    if check.status is CheckStatus.SKIPPED:
        return f"{name}: skipped ({check.skip_reason})"
    details = check.details
    if check.id is CheckId.ROW_COUNT:
        iceberg = details.get("iceberg_deep_rows")
        if iceberg is None:
            iceberg = details.get("iceberg_metadata_rows")
        text = f"{name}: DuckLake {details.get('ducklake_rows')} vs Iceberg {iceberg}"
        hints = details.get("hints") or []
        return f"{text} — {hints[0]}" if hints and check.divergences else text
    if check.status is CheckStatus.PASS:
        base = f"{name}: ok"
        return f"{base} (warning: {check.warnings[0]})" if check.warnings else base
    first = check.divergences[0]
    if check.id is CheckId.COLUMN_AGGREGATES:
        if first.kind == "column":
            return f"{name}: column {first.column} on one side only"
        return f"{name}: {first.column} {first.lake_value} vs {first.iceberg_value}"
    if check.id in {CheckId.PARTITION_PREDICATE, CheckId.BOUNDS_PREDICATE}:
        predicates = details.get("predicates") or []
        item = next(
            (
                p
                for p in predicates
                if p["column"] == first.column
                and p["ducklake_rows"] == first.lake_value
                and p["iceberg_rows"] == first.iceberg_value
            ),
            None,
        )
        if item is None:
            return f"{name}: {first.column} {first.lake_value} vs {first.iceberg_value}"
        text = (
            f"{name}: {item['predicate']} → {item['ducklake_rows']} vs "
            f"{item['iceberg_rows']}"
        )
        if item.get("planned_files") is not None:
            text += f" ({item['planned_files']} planned files)"
        return text
    compared = details.get("compared_rows", 0)
    return f"{name}: {len(check.divergences)} divergences in {compared} rows"


_STATUS_STYLE = {
    "pass": "green",
    "divergent": "red",
    "skipped": "dim",
    "error": "red",
}


def render_validation(results: list[dict[str, Any]]) -> None:
    console = Console()
    for result in results:
        if not result.get("checks"):
            console.print(result.get("message", ""))
            continue
        table = RichTable(
            title=f"ducklake-iceberg validate · {result['table']}",
            title_justify="left",
        )
        table.add_column("check")
        table.add_column("status")
        table.add_column("duration", justify="right")
        table.add_column("details")
        for check in result["checks"]:
            style = _STATUS_STYLE.get(check["status"], "")
            table.add_row(
                check["id"],
                f"[{style}]{check['status']}[/{style}]" if style else check["status"],
                f"{check['duration_ms']} ms",
                check.get("message") or check.get("skip_reason") or "",
            )
        console.print(table)
        for warning in result.get("warnings") or []:
            console.print(f"[yellow]warning:[/yellow] {warning}")
        tolerances = result.get("tolerances_applied") or []
        if tolerances:
            console.print(f"[dim]tolerances: {', '.join(tolerances)}[/dim]")
        console.print(result.get("message", ""))
        console.print(
            f"[dim]snapshots DuckLake/Iceberg: "
            f"{result['ducklake_snapshot_id']}/{result['iceberg_snapshot_id']} · "
            f"exit {result['exit_code']}[/dim]"
        )
