"""Integration test of the doctor -> flush -> doctor revalidation cycle.

Uses a real DuckLake catalog (SQLite) and the real rules. Shows that the flush
**introduces** conditions detected by `partial-files` and by the delete rules,
which is why the doctor must be revalidated after a flush.
"""

from __future__ import annotations

import pytest

from ducklake_iceberg.catalog.connection import open_catalog
from ducklake_iceberg.catalog.reader import (
    current_snapshot,
    parse_table_ref,
    resolve_table,
)
from ducklake_iceberg.doctor import rules as real_rules
from ducklake_iceberg.doctor.model import DoctorContext, DoctorTarget
from ducklake_iceberg.doctor.report import DoctorReport, Revalidation
from ducklake_iceberg.doctor.runner import run
from tests.integration.conftest import CatalogHarness

pytestmark = pytest.mark.integration


def _doctor(
    harness: CatalogHarness,
    table: str,
    *,
    snapshot_id: int | None = None,
    revalidation: Revalidation | None = None,
) -> DoctorReport:
    settings = harness.settings()
    with open_catalog(settings, read_only=True) as con:
        snapshot = current_snapshot(con)
        identity = resolve_table(con, parse_table_ref(table), snapshot)
        target = DoctorTarget(
            catalog=con.alias,
            metadata_catalog=con.meta,
            schema_name=identity.ref.schema_name,
            table_name=identity.ref.table_name,
            table_id=identity.table_id,
            snapshot_id=(
                snapshot_id if snapshot_id is not None else snapshot.snapshot_id
            ),
            schema_version=identity.schema_version,
        )
        ctx = DoctorContext(con=con.raw, target=target)
        return run(ctx, list(real_rules.RULES), revalidation=revalidation)


def _flush(harness: CatalogHarness, table: str) -> None:
    schema, name = table.split(".")
    harness.write(
        f"CALL ducklake_flush_inlined_data('lake', schema_name => '{schema}', "
        f"table_name => '{name}')"
    )


def _seed_inlined_with_delete(catalog: CatalogHarness) -> None:
    catalog.write("CREATE SCHEMA IF NOT EXISTS lake.main")
    catalog.write("CREATE TABLE lake.main.pedidos (id INTEGER, uf VARCHAR)")
    catalog.write("INSERT INTO lake.main.pedidos VALUES (1, 'SP')")  # snapshot
    catalog.write("INSERT INTO lake.main.pedidos VALUES (2, 'MG')")  # snapshot
    catalog.write("DELETE FROM lake.main.pedidos WHERE id = 1")  # delete inlined


def _ids(report: DoctorReport, *, status: str) -> set[str]:
    return {r.rule_id for r in report.results if str(r.status) == status}


# --------------------------------------------------------------------------
# full cycle: flush introduces conditions
# --------------------------------------------------------------------------
def test_cycle_flush_introduces_conditions(catalog: CatalogHarness) -> None:
    _seed_inlined_with_delete(catalog)

    # doctor(S): only inlined-data fires; the 5 file rules are SKIPPED
    before = _doctor(catalog, "main.pedidos")
    if before.exit_code() != 2:
        pytest.skip(
            f"inlining did not occur in this environment (exit={before.exit_code()}); "
            "DATA_INLINING_ROW_LIMIT default differs"
        )
    assert "inlined-data" in _ids(before, status="failed")
    skipped_before = _ids(before, status="skipped")
    assert {
        "partial-files",
        "delete-parquet",
        "delete-puffin",
        "mapping-divergence",
        "encryption-key",
    } <= skipped_before
    assert before.summary()["revalidation_required"] is True

    prev_snapshot = before.target.snapshot_id
    _flush(catalog, "main.pedidos")

    # doctor(S'): inlined-data passes; partial-files and/or a delete rule are
    # now EVALUATED (no longer SKIPPED) and fire.
    after = _doctor(
        catalog,
        "main.pedidos",
        revalidation=Revalidation(
            after="flush",
            previous_snapshot_id=prev_snapshot,
            previous_exit_code=before.exit_code(),
        ),
    )
    assert "inlined-data" not in _ids(after, status="failed") or _only_info(
        after, "inlined-data"
    )
    # no file rule remains skipped
    assert _ids(after, status="skipped") == set()
    # the flush introduced a condition: partial-files and/or delete-* fire
    fired = _ids(after, status="failed")
    assert fired & {"partial-files", "delete-parquet", "delete-puffin"}


def _only_info(report: DoctorReport, rule_id: str) -> bool:
    """True if the rule only emitted INFO findings (residue), without blocking."""
    for result in report.results:
        if result.rule_id == rule_id:
            return all(f.severity.name == "INFO" for f in result.findings)
    return False


# --------------------------------------------------------------------------
# revalidation in --json
# --------------------------------------------------------------------------
def test_revalidation_in_json(catalog: CatalogHarness) -> None:
    _seed_inlined_with_delete(catalog)

    before = _doctor(catalog, "main.pedidos")
    if before.exit_code() != 2:
        pytest.skip("inlining did not occur in this environment; default differs")
    assert before.to_json()["revalidation"] is None

    prev = before.target.snapshot_id
    _flush(catalog, "main.pedidos")
    after = _doctor(
        catalog,
        "main.pedidos",
        revalidation=Revalidation(
            after="flush",
            previous_snapshot_id=prev,
            previous_exit_code=2,
        ),
    )
    reval = after.to_json()["revalidation"]
    assert reval == {
        "after": "flush",
        "previous_snapshot_id": prev,
        "previous_exit_code": 2,
    }
