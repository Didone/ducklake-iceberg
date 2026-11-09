"""Unit tests for nested VARIANT detection/translation."""

from __future__ import annotations

import duckdb

from ducklake_iceberg.checkpoint.model import NestedVariantError, TableRef
from ducklake_iceberg.checkpoint.variant import (
    NESTED_VARIANT_PATTERNS,
    VariantColumn,
    find_nested_variants,
    nested_variant_error,
    translate_checkpoint_error,
)
from ducklake_iceberg.errors import ExitCode

T = TableRef("vendas", "pedidos", 5)

#: Raw text captured at runtime (DuckDB 2.0).
RAW_NESTED = (
    "Not implemented Error: ColumnWriter of type 'VARIANT' requires a "
    "transform, but is not a root column, this isn't supported currently"
)


def _col(path: str, depth: int, parent_type: str | None = "struct") -> VariantColumn:
    return VariantColumn(
        column_id=7,
        path=path,
        column_name=path.split(".")[-1].rstrip("[]"),
        depth=depth,
        parent_column_id=5 if depth else None,
        parent_type=parent_type if depth else None,
        root_column_id=5,
    )


def test_raw_nested_variant_message_matches_patterns() -> None:
    assert any(p.search(RAW_NESTED) for p in NESTED_VARIANT_PATTERNS)


def test_is_nested() -> None:
    assert not _col("top", 0, None).is_nested
    assert _col("payload.attrs", 1).is_nested


def test_find_nested_variants_filters_top_level() -> None:
    cols = (_col("top", 0, None), _col("payload.attrs", 1), _col("tags[].v", 2))
    nested = find_nested_variants(cols)
    assert {c.path for c in nested} == {"payload.attrs", "tags[].v"}


def test_nested_variant_error_names_column_and_id() -> None:
    err = nested_variant_error(T, (_col("payload.attrs", 1),))
    assert isinstance(err, NestedVariantError)
    assert err.exit_code == ExitCode.BLOCKED
    assert err.detected == "pre_check"
    assert "payload.attrs" in err.message
    assert "column_id 7" in err.message
    payload = err.to_json()
    assert payload["code"] == "E-VARIANT-NESTED"
    assert payload["columns"][0]["path"] == "payload.attrs"
    assert payload["remediation"]


def test_nested_variant_error_multiple_columns_sorted_source() -> None:
    cols = (_col("a.x", 1), _col("b.y", 1))
    err = nested_variant_error(T, cols)
    assert err.message.index("a.x") < err.message.index("b.y")


# --- translate_checkpoint_error ---------------------------------------------
def _exc() -> duckdb.Error:
    return duckdb.Error(RAW_NESTED)


def test_translate_precheck_found_nested() -> None:
    # the pre-check already found a nested variant
    err = translate_checkpoint_error(
        _exc(), table=T, phase="flush", variants=(_col("payload.attrs", 1),)
    )
    assert isinstance(err, NestedVariantError)
    assert err.detected == "duckdb_error"
    assert err.raw == RAW_NESTED


def test_translate_text_matches_top_level_variant() -> None:
    # text matches and a variant exists (even only at top level)
    err = translate_checkpoint_error(
        _exc(), table=T, phase="flush", variants=(_col("top", 0, None),)
    )
    assert isinstance(err, NestedVariantError)
    assert err.raw == RAW_NESTED


def test_translate_text_matches_no_variant_column() -> None:
    # text matches 'variant' but the catalog has no variant column
    err = translate_checkpoint_error(
        duckdb.Error("something about variant failed"),
        table=T,
        phase="flush",
        variants=(),
    )
    assert not isinstance(err, NestedVariantError)
    assert err.exit_code == ExitCode.ERROR
    assert err.code == "E-CHECKPOINT-UNKNOWN"
    assert "records no variant column" in (err.hint or "")
    assert err.raw == "something about variant failed"


def test_translate_unrecognized() -> None:
    err = translate_checkpoint_error(
        duckdb.Error("disk full"), table=T, phase="flush", variants=()
    )
    assert err.code == "E-CHECKPOINT-UNKNOWN"
    assert err.exit_code == ExitCode.ERROR
    assert err.phase == "flush"
    assert err.raw == "disk full"


def test_translate_preserves_snapshot_created() -> None:
    err = translate_checkpoint_error(
        _exc(),
        table=T,
        phase="flush",
        variants=(_col("payload.attrs", 1),),
        snapshot_created=42,
    )
    assert err.snapshot_created == 42


def test_translate_always_keeps_raw() -> None:
    err = translate_checkpoint_error(
        duckdb.Error("anything"),
        table=T,
        phase="flush",
        variants=(),
    )
    assert err.raw == "anything"
