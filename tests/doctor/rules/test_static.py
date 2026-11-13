"""Static tests over the set of doctor rules.

They check design invariants that do not depend on a catalog: unique ids,
declared severity as a ceiling, and SQL discipline (snapshot predicate only on
versioned tables; interpolation only of `ctx.meta`).
"""

from __future__ import annotations

import re

import pytest

from ducklake_iceberg.doctor.model import DoctorContext, DoctorTarget, Severity
from ducklake_iceberg.doctor.plan import PLAN_ORDER, execution_plan
from ducklake_iceberg.doctor.rules import (
    RULES,
    calendar_transform,
    delete_files,
    encryption_key,
    inlined_data,
    mapping_divergence,
    parquet_compression,
    partial_files,
    unsupported_type,
)

# modules with a public _sql(ctx) -> str function for static inspection
_SQL_BUILDERS = {
    "partial-files": [partial_files._sql],
    "delete-parquet": [delete_files._sql_parquet],
    "delete-puffin": [delete_files._sql_puffin, delete_files._sql_puffin_option],
    "unsupported-type": [unsupported_type._sql],
    "calendar-transform": [calendar_transform._sql],
    "parquet-compression": [parquet_compression._sql],
    "mapping-divergence": [
        mapping_divergence._sql_distribution,
        mapping_divergence._sql_types,
    ],
    "encryption-key": [encryption_key._sql],
}

# NON-versioned tables: applying the snapshot predicate to them is an SQL error
_UNVERSIONED = (
    "ducklake_metadata",
    "ducklake_inlined_data_tables",
    "ducklake_partition_column",
    "ducklake_column_mapping",
    "ducklake_file_partition_value",
    "ducklake_file_column_stats",
)


def _ctx() -> DoctorContext:
    target = DoctorTarget(
        catalog="lake",
        metadata_catalog="lake_meta",
        schema_name="sales",
        table_name="orders",
        table_id=7,
        snapshot_id=42,
        schema_version=1,
    )
    return DoctorContext(con=None, target=target)  # type: ignore[arg-type]


def _all_sql() -> list[str]:
    ctx = _ctx()
    return [builder(ctx) for builders in _SQL_BUILDERS.values() for builder in builders]


# unique ids and correspondence with PLAN_ORDER
def test_rule_ids_unique_and_in_plan() -> None:
    ids = [r.id for r in RULES]
    assert len(ids) == len(set(ids))
    assert set(ids) == set(PLAN_ORDER)
    # inlined-data is the gate of the five physical dependents
    assert inlined_data.RULE.requires == ()


# the declared severity is the ceiling of each rule
def test_declared_severity_is_ceiling() -> None:
    expected = {
        "inlined-data": Severity.BLOCKER,
        "partial-files": Severity.BLOCKER,
        "delete-parquet": Severity.BLOCKER,
        "delete-puffin": Severity.BLOCKER,
        "unsupported-type": Severity.BLOCKER,
        "calendar-transform": Severity.DEGRADED,
        "parquet-compression": Severity.DEGRADED,
        "mapping-divergence": Severity.BLOCKER,
        "encryption-key": Severity.BLOCKER,
    }
    for rule in RULES:
        assert rule.severity is expected[rule.id]


# the snapshot predicate is never bound to the alias of an unversioned table
@pytest.mark.parametrize("unversioned", _UNVERSIONED)
def test_no_snapshot_predicate_on_unversioned(unversioned: str) -> None:
    for sql in _all_sql():
        for alias in _aliases_of(sql, unversioned):
            assert f"{alias}.begin_snapshot" not in sql, (
                f"snapshot predicate bound to alias {alias!r} of the "
                f"unversioned table {unversioned}"
            )
            assert f"{alias}.end_snapshot" not in sql


def _aliases_of(sql: str, table: str) -> list[str]:
    """Aliases given to `table` in FROM/JOIN clauses (`... <table> <alias>`)."""
    # the reference comes quoted: "lake_meta"."table" alias OR "lake_meta".table alias
    pattern = re.compile(
        re.escape(f'"lake_meta".{table}') + r"\s+(?P<alias>[A-Za-z_][A-Za-z0-9_]*)"
    )
    aliases = [m.group("alias") for m in pattern.finditer(sql)]
    # ignore keywords that are not aliases
    return [a for a in aliases if a.lower() not in {"on", "where", "as"}]


# SQL only interpolates ctx.meta; values go through named parameters
def test_sql_only_binds_params_and_meta() -> None:
    for sql in _all_sql():
        # the only accepted numeric value is via $table_id / $snapshot_id
        assert "$table_id" in sql or "$snapshot_id" in sql or "duckdb_tables" in sql
        # no literal id/snapshot value interpolated directly
        assert "= 7" not in sql
        assert "= 42" not in sql
        # the metadata catalog appears quoted (quote_ident) via ctx.meta
        assert '"lake_meta".' in sql


# the inlined-data rule builds dynamic SQL - covered via the plan
def test_execution_plan_builds_for_real_rules() -> None:
    plan = execution_plan()
    assert [r.id for r in plan.order] == list(PLAN_ORDER)
    assert plan.dependents_of("inlined-data") == (
        "partial-files",
        "delete-parquet",
        "delete-puffin",
        "mapping-divergence",
        "encryption-key",
    )
