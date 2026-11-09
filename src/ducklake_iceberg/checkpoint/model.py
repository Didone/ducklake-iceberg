"""Data contracts of the flush of inlined data.

The resolved table reference (`TableRef`), the catalog identity fingerprint
(`CatalogFingerprint`) and the flush errors. None of them touches DuckDB.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from ducklake_iceberg.errors import CompatibilityError, DuckLakeIcebergError, ExitCode

Phase = Literal["flush"]


@dataclass(frozen=True, slots=True)
class TableRef:
    """Table resolved against the current snapshot."""

    schema_name: str
    table_name: str
    table_id: int

    @property
    def qualified(self) -> str:
        return f"{self.schema_name}.{self.table_name}"

    def __str__(self) -> str:
        return self.qualified


if TYPE_CHECKING:
    from ducklake_iceberg.checkpoint.variant import VariantColumn


class CheckpointError(DuckLakeIcebergError):
    """Failure of a checkpoint phase (exit 1 by default).

    The raw DuckDB text is preserved in `raw`, never swallowed.
    `NestedVariantError` specializes this base with exit code 2.
    `detected` records whether the failure came from the pre-check
    (`pre_check`) or from translating a DuckDB error (`duckdb_error`).
    """

    code: str = "E-CHECKPOINT-UNKNOWN"

    def __init__(
        self,
        message: str,
        *,
        table: TableRef | None = None,
        phase: Phase | None = None,
        hint: str | None = None,
        raw: str | None = None,
        columns: tuple[VariantColumn, ...] = (),
        snapshot_created: int | None = None,
        detected: Literal["pre_check", "duckdb_error"] | None = None,
        remediation: str | None = None,
    ) -> None:
        super().__init__(message, hint=hint)
        self.table = table
        self.phase = phase
        self.raw = raw
        self.columns = columns
        self.snapshot_created = snapshot_created
        self.detected = detected
        self.remediation = remediation

    def to_json(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "exit_code": int(self.exit_code),
            "phase": self.phase,
            "table": self.table.qualified if self.table else None,
            "detected": self.detected,
            "message": self.message,
            "hint": self.hint,
            "columns": [c.to_json() for c in self.columns],
            "raw": self.raw,
            "snapshot_created": self.snapshot_created,
            "remediation": self.remediation,
        }


class NestedVariantError(CheckpointError, CompatibilityError):
    """`VARIANT` inside `STRUCT`/`LIST`/`MAP` (exit 2).

    Flush and compaction write Parquet, and DuckDB cannot write nested VARIANT
    to Parquet: *"ColumnWriter of type 'VARIANT' requires a transform,
    but is not a root column"*.
    """

    code = "E-VARIANT-NESTED"
    exit_code = ExitCode.BLOCKED


@dataclass(frozen=True, slots=True)
class CatalogFingerprint:
    """Catalog identity, read in READ_ONLY mode and rechecked on write.

    `data_path` and `format_version` must not change between the two opens;
    `max_snapshot_id` and `table_count` may advance due to concurrent
    writes/DDL and only produce a warning.
    """

    data_path: str | None
    format_version: str | None
    max_snapshot_id: int | None
    table_count: int

    def matches(self, other: CatalogFingerprint) -> bool:
        return self.divergence(other) is None

    def divergence(self, other: CatalogFingerprint) -> str | None:
        """Description of the blocking divergence, or None if compatible."""
        if self.data_path != other.data_path:
            return f"data_path {self.data_path!r} != {other.data_path!r}"
        if self.format_version != other.format_version:
            return f"format_version {self.format_version!r} != {other.format_version!r}"
        return None
