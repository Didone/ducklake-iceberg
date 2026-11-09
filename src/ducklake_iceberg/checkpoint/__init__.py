"""Table checkpoint for DuckLake.

Bundles the `ducklake-iceberg checkpoint` command (flush of inlined data + rewrite +
merge, scoped to a table) and the detection of inlined data. It is the only
part of the tool allowed to write to DuckLake.
"""

from __future__ import annotations

from ducklake_iceberg.checkpoint.command import run_checkpoint
from ducklake_iceberg.checkpoint.dry_run import (
    DryRunReport,
    TableDryRun,
    dry_run_checkpoint,
)
from ducklake_iceberg.checkpoint.model import (
    CatalogFingerprint,
    CheckpointError,
    CheckpointRunReport,
    CompactionStep,
    NestedVariantError,
    TableCheckpointResult,
    TableRef,
    TableScope,
)
from ducklake_iceberg.checkpoint.report import (
    CheckpointSnapshotReport,
    build_snapshot_report,
)
from ducklake_iceberg.checkpoint.variant import (
    VariantColumn,
    find_nested_variants,
    find_variant_columns,
)

__all__ = [
    "CatalogFingerprint",
    "CheckpointError",
    "CheckpointRunReport",
    "CheckpointSnapshotReport",
    "CompactionStep",
    "DryRunReport",
    "NestedVariantError",
    "TableCheckpointResult",
    "TableDryRun",
    "TableRef",
    "TableScope",
    "VariantColumn",
    "build_snapshot_report",
    "dry_run_checkpoint",
    "find_nested_variants",
    "find_variant_columns",
    "run_checkpoint",
]
