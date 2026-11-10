"""Detection and translation of the nested `VARIANT` error.

Flush and compaction write Parquet, and DuckDB refuses to write a `VARIANT`
column that is not a root: *"ColumnWriter of type 'VARIANT' requires a
transform, but is not a root column, this isn't supported currently"*
(verified at runtime, DuckDB 2.0). The failure is deterministic from the
catalog, so detection is **up-front**: before opening the write connection, it
walks the `ducklake_column` tree via `parent_column` and points at the
offending column by path and `column_id`.

When, despite the pre-check, the failure comes from the `CALL` (column created
between the read and the write, different extension version),
`translate_checkpoint_error` classifies the raw error -- which is **never**
discarded, it stays in `raw`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Final, cast

import duckdb

from ducklake_iceberg.catalog.query import query_arrow
from ducklake_iceberg.checkpoint.model import (
    CheckpointError,
    NestedVariantError,
    Phase,
    TableRef,
)
from ducklake_iceberg.doctor.model import quote_ident

#: Maximum depth walked -- defense against a cycle in `parent_column`.
_MAX_DEPTH: Final[int] = 64

#: Broad patterns on the DuckDB text. Correct attribution does not depend on
#: them: the primary source is the catalog (pre-check); the text is a backstop
#: for a column that appeared between the read and the `CALL`.
NESTED_VARIANT_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"columnwriter of type 'variant'", re.I),
    re.compile(r"\bnested\b.*\bvariant\b", re.I),
    re.compile(r"\bvariant\b.*\b(struct|list|map)\b", re.I),
    re.compile(r"\bvariant\b", re.I),
)

_NESTED_REMEDIATION: Final[str] = (
    "change the column type at the source to `json` or `varchar` and reinsert "
    "the rows, or drop the column"
)


@dataclass(frozen=True, slots=True)
class VariantColumn:
    column_id: int
    path: str  # 'payload.attrs' or 'tags[].v'
    column_name: str
    depth: int  # 0 = top level
    parent_column_id: int | None
    parent_type: str | None  # 'struct' | 'list' | 'map' | None
    root_column_id: int

    @property
    def is_nested(self) -> bool:
        return self.depth > 0

    def to_json(self) -> dict[str, Any]:
        return {
            "column_id": self.column_id,
            "path": self.path,
            "column_name": self.column_name,
            "depth": self.depth,
            "parent_column_id": self.parent_column_id,
            "parent_type": self.parent_type,
        }


def _tree_sql(meta: str) -> str:
    col = f"{quote_ident(meta)}.ducklake_column"
    return f"""\
WITH RECURSIVE cols AS (
    SELECT column_id, column_name, column_type, parent_column
    FROM {col}
    WHERE table_id = $table_id
      AND $snapshot_id >= begin_snapshot
      AND (end_snapshot IS NULL OR $snapshot_id < end_snapshot)
),
tree AS (
    SELECT c.column_id, c.column_name, c.column_type, c.parent_column,
           coalesce(c.column_name, '?')       AS path,
           0                                  AS depth,
           c.column_id                        AS root_column_id,
           CAST(NULL AS VARCHAR)              AS parent_type
    FROM cols AS c
    WHERE c.parent_column IS NULL
    UNION ALL
    SELECT c.column_id, c.column_name, c.column_type, c.parent_column,
           t.path || CASE
               WHEN lower(t.column_type) = 'list' THEN '[]'
               WHEN c.column_name IS NULL THEN '.?'
               ELSE '.' || c.column_name
           END                                AS path,
           t.depth + 1                        AS depth,
           t.root_column_id,
           lower(t.column_type)               AS parent_type
    FROM cols AS c
    JOIN tree AS t ON c.parent_column = t.column_id
    WHERE t.depth < {_MAX_DEPTH}
)
SELECT column_id, path, column_name, depth,
       parent_column AS parent_column_id, parent_type, root_column_id
FROM tree
WHERE lower(column_type) = 'variant'
ORDER BY path"""


def find_variant_columns(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    table: TableRef,
    snapshot_id: int,
) -> tuple[VariantColumn, ...]:
    """All `variant` columns of the table in the snapshot, with their path."""
    rows = query_arrow(
        con,
        _tree_sql(meta),
        {"table_id": table.table_id, "snapshot_id": snapshot_id},
    ).to_pylist()
    return tuple(
        VariantColumn(
            column_id=int(r["column_id"]),
            path=str(r["path"]),
            column_name=str(r["column_name"]) if r["column_name"] is not None else "?",
            depth=int(r["depth"]),
            parent_column_id=(
                int(r["parent_column_id"])
                if r["parent_column_id"] is not None
                else None
            ),
            parent_type=(
                str(r["parent_type"]) if r["parent_type"] is not None else None
            ),
            root_column_id=int(r["root_column_id"]),
        )
        for r in rows
    )


def find_nested_variants(
    columns: tuple[VariantColumn, ...],
) -> tuple[VariantColumn, ...]:
    """Subset with `is_nested`. Empty => the checkpoint can proceed."""
    return tuple(c for c in columns if c.is_nested)


def _nested_detail(columns: tuple[VariantColumn, ...]) -> str:
    parts = [
        f"{c.path} (column_id {c.column_id})"
        + (
            f" inside column_id {c.parent_column_id} ({c.parent_type})"
            if c.parent_column_id is not None
            else ""
        )
        for c in columns
    ]
    return "; ".join(parts)


def nested_variant_error(
    table: TableRef,
    nested: tuple[VariantColumn, ...],
    *,
    detected: str = "pre_check",
    raw: str | None = None,
    snapshot_created: int | None = None,
) -> NestedVariantError:
    """Build the nested VARIANT error (exit 2) pointing at the offending column."""
    detail = _nested_detail(nested)
    message = f"checkpoint blocked by nested VARIANT: {detail}"
    hint = (
        "DuckDB cannot write nested VARIANT to Parquet; Iceberg v2 has no "
        "variant type and the export stays blocked by the type rules"
    )
    return NestedVariantError(
        message,
        table=table,
        hint=hint,
        raw=raw,
        columns=nested,
        snapshot_created=snapshot_created,
        detected=cast("Any", detected),
        remediation=_NESTED_REMEDIATION,
    )


def translate_checkpoint_error(
    exc: duckdb.Error,
    *,
    table: TableRef,
    phase: Phase,
    variants: tuple[VariantColumn, ...],
    snapshot_created: int | None = None,
) -> CheckpointError:
    """Classify the `CALL` failure and return an actionable error.

    `variants` are **all** the table's `variant` columns (nested or not), read
    in the pre-check; nesting is derived here.
    """
    raw = str(exc)
    nested = find_nested_variants(variants)

    # case 1: the pre-check had already found a nested one
    if nested:
        return nested_variant_error(
            table,
            nested,
            detected="duckdb_error",
            raw=raw,
            snapshot_created=snapshot_created,
        )

    matched = any(p.search(raw) for p in NESTED_VARIANT_PATTERNS)
    # case 2: the text matches and a variant exists (even a top-level one)
    if matched and variants:
        return nested_variant_error(
            table,
            variants,
            detected="duckdb_error",
            raw=raw,
            snapshot_created=snapshot_created,
        )

    # cases 3 and 4: generic error, raw always preserved
    hint = (
        "the catalog records no variant column in this table; check the "
        "DuckLake extension version and open an issue with the raw text"
        if matched
        else "rerun with --log-level debug; the checkpoint is idempotent"
    )
    return CheckpointError(
        f"checkpoint of {table.qualified} failed in phase {phase}: {raw}",
        table=table,
        phase=phase,
        hint=hint,
        raw=raw,
        snapshot_created=snapshot_created,
        detected="duckdb_error",
    )
