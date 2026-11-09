"""Unit tests for the regex, classification, and reconciliation.

They prove the guarantees that do not depend on a real catalog: the pattern is
anchored, names outside the pattern or with a mismatched `table_id` are never
interpolated and become warnings, and the 4 reconciliation cases are classified
correctly.
"""

from __future__ import annotations

from ducklake_iceberg.checkpoint.detect import (
    _PHYSICAL_NAME,
    InlinedCount,
    InlinedPhysicalTable,
    InlinedStatus,
    _classify,
    _reconcile,
)
from ducklake_iceberg.checkpoint.model import TableRef

# -- the regex is anchored and does not match the DuckLake spec tables -------
_SPEC_TABLES = [
    "ducklake_snapshot",
    "ducklake_table",
    "ducklake_data_file",
    "ducklake_inlined_data_tables",  # registry, not a physical table
    "ducklake_delete_file",
    "ducklake_column",
]


def test_regex_rejects_spec_tables() -> None:
    for name in _SPEC_TABLES:
        assert _PHYSICAL_NAME.match(name) is None, name


def test_regex_accepts_physical_tables() -> None:
    assert _PHYSICAL_NAME.match("ducklake_inlined_data_5_0") is not None
    assert _PHYSICAL_NAME.match("ducklake_inlined_data_6_12") is not None
    assert _PHYSICAL_NAME.match("ducklake_inlined_delete_5") is not None


def test_regex_rejects_non_numeric_suffix() -> None:
    # 'ducklake_inlined_data_x_y' does not match the pattern
    assert _PHYSICAL_NAME.match("ducklake_inlined_data_x_y") is None
    assert _PHYSICAL_NAME.match("ducklake_inlined_data_5_0_extra") is None


# -- classification ---------------------------------------------------------
def test_classify_data_table() -> None:
    src = _classify("ducklake_inlined_data_5_0", 5, registered=True, exists=True)
    assert src is not None
    assert src.kind == "data"
    assert src.schema_version == 0
    assert src.registered
    assert src.exists


def test_classify_delete_table() -> None:
    src = _classify("ducklake_inlined_delete_5", 5, registered=False, exists=True)
    assert src is not None
    assert src.kind == "delete"
    assert src.schema_version is None


def test_classify_rejects_mismatched_table_id() -> None:
    # name embeds table_id=9, but the queried table is 5 -> None
    src = _classify("ducklake_inlined_data_9_0", 5, registered=False, exists=True)
    assert src is None


def test_classify_rejects_bad_name() -> None:
    src = _classify("ducklake_inlined_data_x_y", 5, registered=True, exists=True)
    assert src is None


# -- reconciliation of the 4 cases ------------------------------------------
def test_reconcile_registered_and_exists() -> None:
    warnings: list[str] = []
    sources = _reconcile(
        {"ducklake_inlined_data_5_0": 0}, {"ducklake_inlined_data_5_0"}, 5, warnings
    )
    assert len(sources) == 1
    assert warnings == []


def test_reconcile_orphan_registry_warns() -> None:
    warnings: list[str] = []
    sources = _reconcile({"ducklake_inlined_data_5_0": 0}, set(), 5, warnings)
    assert len(sources) == 1  # still listed, but the count will be 0
    assert any("without a physical table" in w for w in warnings)


def test_reconcile_physical_without_registry() -> None:
    # normal case for delete tables: physical without registry, no orphan warning
    warnings: list[str] = []
    sources = _reconcile(set(), {"ducklake_inlined_delete_5"}, 5, warnings)  # type: ignore[arg-type]
    assert len(sources) == 1
    assert sources[0].kind == "delete"
    assert all("without a physical table" not in w for w in warnings)


def test_reconcile_bad_name_warns_no_interpolation() -> None:
    warnings: list[str] = []
    sources = _reconcile(
        {"ducklake_inlined_data_9_0": 0}, set(), 5, warnings
    )  # mismatched table_id
    assert sources == []
    assert any("outside the inlining pattern" in w for w in warnings)


# -- InlinedStatus aggregations ---------------------------------------------
def _src(kind: str, name: str) -> InlinedPhysicalTable:
    return InlinedPhysicalTable(
        physical_name=name,
        kind=kind,  # type: ignore[arg-type]
        table_id=5,
        schema_version=0 if kind == "data" else None,
        registered=True,
        exists=True,
    )


def test_status_aggregates_data_and_delete() -> None:
    status = InlinedStatus(
        table=TableRef("vendas", "pedidos", 5),
        snapshot_id=41,
        counts=(
            InlinedCount(_src("data", "ducklake_inlined_data_5_0"), 3, 5, True),
            InlinedCount(_src("delete", "ducklake_inlined_delete_5"), 1, 1, False),
        ),
    )
    assert status.pending_rows == 4
    assert status.pending_data_rows == 3
    assert status.pending_delete_rows == 1
    assert status.has_pending
    payload = status.to_json()
    assert payload["pending_rows"] == 4
    assert payload["sources"][0]["physical_name"] == "ducklake_inlined_data_5_0"
    assert payload["sources"][1]["visibility_filtered"] is False


def test_reconcile_ignores_other_tables_inlined_names_silently() -> None:
    """Catalog-wide introspection sees every table; foreign names are not noise."""
    warnings: list[str] = []
    sources = _reconcile(
        {},
        {
            "ducklake_inlined_data_2_2",
            "ducklake_inlined_delete_3",
            "ducklake_inlined_data_5_7",
        },
        5,
        warnings,
    )
    assert [s.physical_name for s in sources] == ["ducklake_inlined_data_5_7"]
    assert warnings == []
