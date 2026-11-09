"""Data contracts of the `iceduck checkpoint` command.

Immutable dataclasses that flow through the table checkpoint: the resolved
table reference (`TableRef`), the scope (`TableScope`), the catalog identity
fingerprint (`CatalogFingerprint`), the result of each compaction phase
(`CompactionStep`), the per-table result (`TableCheckpointResult`) and the
aggregate report (`CheckpointRunReport`). None of them touches DuckDB.
"""

from __future__ import annotations

import statistics
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal

from iceduck.errors import CompatibilityError, ExitCode, IceduckError

ScopeOrigin = Literal["table", "schema", "all"]
Phase = Literal["flush", "expire", "merge", "rewrite"]
PartitionState = Literal["closed", "open"]


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
    from iceduck.checkpoint.report import CheckpointSnapshotReport
    from iceduck.checkpoint.revalidate import Revalidation
    from iceduck.checkpoint.variant import VariantColumn


class CheckpointError(IceduckError):
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
class TableScope:
    """Resolved scope of the command. Immutable once resolved."""

    tables: tuple[TableRef, ...]
    origin: ScopeOrigin
    schema_filter: str | None = None


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


@dataclass(frozen=True, slots=True)
class CompactionStep:
    """Result of `ducklake_rewrite_data_files` or `merge_adjacent_files`."""

    files_processed: int = 0
    files_created: int = 0

    @property
    def did_work(self) -> bool:
        return self.files_processed > 0

    def to_json(self) -> dict[str, int]:
        return {
            "files_processed": self.files_processed,
            "files_created": self.files_created,
        }


@dataclass(frozen=True, slots=True)
class FileSizeDistribution:
    """Distribution of the live data files after the checkpoint."""

    file_count: int
    total_bytes: int
    min_bytes: int | None
    p25_bytes: float | None
    median_bytes: float | None
    p75_bytes: float | None
    mean_bytes: float | None
    max_bytes: int | None
    target_file_size_bytes: int | None = None
    below_half_target: int | None = None

    @classmethod
    def from_sizes(
        cls, sizes: Iterable[int], target_file_size_bytes: int | None
    ) -> FileSizeDistribution:
        """Distribution of a list of sizes (bytes), with linear quartiles."""
        ordered = sorted(sizes)
        if not ordered:
            return cls(
                file_count=0,
                total_bytes=0,
                min_bytes=None,
                p25_bytes=None,
                median_bytes=None,
                p75_bytes=None,
                mean_bytes=None,
                max_bytes=None,
                target_file_size_bytes=target_file_size_bytes,
                below_half_target=0 if target_file_size_bytes is not None else None,
            )

        def percentile(fraction: float) -> float:
            position = (len(ordered) - 1) * fraction
            lower = int(position)
            upper = min(lower + 1, len(ordered) - 1)
            weight = position - lower
            return ordered[lower] * (1 - weight) + ordered[upper] * weight

        return cls(
            file_count=len(ordered),
            total_bytes=sum(ordered),
            min_bytes=ordered[0],
            p25_bytes=percentile(0.25),
            median_bytes=float(statistics.median(ordered)),
            p75_bytes=percentile(0.75),
            mean_bytes=statistics.fmean(ordered),
            max_bytes=ordered[-1],
            target_file_size_bytes=target_file_size_bytes,
            below_half_target=(
                sum(size < target_file_size_bytes / 2 for size in ordered)
                if target_file_size_bytes is not None
                else None
            ),
        )

    def to_json(self) -> dict[str, int | float | None]:
        return {
            "file_count": self.file_count,
            "total_bytes": self.total_bytes,
            "min_bytes": self.min_bytes,
            "p25_bytes": self.p25_bytes,
            "median_bytes": self.median_bytes,
            "p75_bytes": self.p75_bytes,
            "mean_bytes": self.mean_bytes,
            "max_bytes": self.max_bytes,
            "target_file_size_bytes": self.target_file_size_bytes,
            "below_half_target": self.below_half_target,
        }


@dataclass(frozen=True, slots=True)
class PartitionFileSizes:
    """Distribution of the live data files of a partition.

    `last_write_*` is the last data write (insert, delete, flush) still
    recorded in the catalog; compaction-only snapshots do not count.
    `None` when no recorded write is left (compacted and expired history):
    the partition is treated as closed.
    """

    partition_id: int | None
    values: tuple[str | None, ...]
    label: str
    state: PartitionState
    last_write_snapshot: int | None
    last_write_time: datetime | None
    distribution: FileSizeDistribution
    #: sizes of the live data files (input for the aggregate distribution)
    sizes: tuple[int, ...] = field(default=(), repr=False)

    @property
    def meets_target(self) -> bool | None:
        """Acceptance criterion: median >= 50% of the target, or a single file
        (the whole partition fits below the target and there is nothing to
        merge)."""
        target = self.distribution.target_file_size_bytes
        median = self.distribution.median_bytes
        if target is None or median is None:
            return None
        return self.distribution.file_count == 1 or median >= target / 2

    def to_json(self) -> dict[str, Any]:
        return {
            "partition": self.label,
            "partition_id": self.partition_id,
            "partition_values": list(self.values),
            "state": self.state,
            "last_write_snapshot": self.last_write_snapshot,
            "last_write_time": (
                self.last_write_time.isoformat()
                if self.last_write_time is not None
                else None
            ),
            "meets_target": self.meets_target,
            "file_size_distribution": self.distribution.to_json(),
        }


@dataclass(frozen=True, slots=True)
class PartitionSizeReport:
    """Per-partition distribution, with the closed/open classification."""

    closed_after_seconds: int
    reference_time: datetime
    partitions: tuple[PartitionFileSizes, ...]
    target_file_size_bytes: int | None

    @property
    def closed(self) -> tuple[PartitionFileSizes, ...]:
        return tuple(p for p in self.partitions if p.state == "closed")

    @property
    def closed_files(self) -> FileSizeDistribution:
        """Distribution of all files in the closed partitions."""
        return FileSizeDistribution.from_sizes(
            (size for p in self.closed for size in p.sizes),
            self.target_file_size_bytes,
        )

    @property
    def closed_meeting_target(self) -> int:
        return sum(1 for p in self.closed if p.meets_target)

    @property
    def closed_criterion_met(self) -> bool | None:
        """True if all closed partitions meet the criterion; None without any
        closed partition or without a target."""
        closed = self.closed
        if not closed or self.target_file_size_bytes is None:
            return None
        return all(p.meets_target for p in closed)

    def to_json(self) -> dict[str, Any]:
        closed = self.closed
        return {
            "closed_after_seconds": self.closed_after_seconds,
            "reference_time": self.reference_time.isoformat(),
            "target_file_size_bytes": self.target_file_size_bytes,
            "partition_count": len(self.partitions),
            "closed_partitions": len(closed),
            "open_partitions": len(self.partitions) - len(closed),
            "closed_partitions_meeting_target": self.closed_meeting_target,
            "closed_criterion_met": self.closed_criterion_met,
            "closed_files_distribution": self.closed_files.to_json(),
            "partitions": [p.to_json() for p in self.partitions],
        }


@dataclass(frozen=True, slots=True)
class TableCheckpointResult:
    """Checkpoint result for a single table."""

    table: TableRef
    snapshot_before: int
    snapshot_after: int  # export target for this table
    rows_flushed: int = 0
    snapshots_expired: tuple[int, ...] = ()  # whole catalog (expire is global)
    merge: CompactionStep = field(default_factory=CompactionStep)
    rewrite: CompactionStep = field(default_factory=CompactionStep)
    file_size_distribution: FileSizeDistribution | None = None
    partition_distribution: PartitionSizeReport | None = None
    error: CheckpointError | None = None
    #: `ducklake_snapshot.next_file_id` before the checkpoint (used to classify files)
    next_file_id_before: int | None = None
    snapshot_before_time: Any = None  # datetime | None; input for snapshot reporting
    schema_version_before: int | None = None
    snapshots: CheckpointSnapshotReport | None = None
    revalidation: Revalidation | None = None
    revalidation_error: str | None = None

    @property
    def flushed(self) -> bool:
        return self.rows_flushed > 0

    @property
    def compacted(self) -> bool:
        return self.rewrite.did_work or self.merge.did_work

    @property
    def changed(self) -> bool:
        """True if any phase did work."""
        return self.flushed or self.compacted

    def to_json(self) -> dict[str, Any]:
        # Minimal keys: the inlined rows before are those the flush
        # materialized; the ones after are those the revalidation still saw
        # pending (inlined by others after the flush). With no result
        # (error), ``None``: unknown.
        after: int | None
        if self.revalidation is not None:
            after = self.revalidation.still_pending_rows
        else:
            after = None if self.error is not None else 0
        return {
            "table": self.table.qualified,
            "inlined_rows_before": (self.rows_flushed if self.error is None else None),
            "inlined_rows_after": after,
            "files_processed": (
                self.merge.files_processed + self.rewrite.files_processed
            ),
            "files_created": self.merge.files_created + self.rewrite.files_created,
            "schema_name": self.table.schema_name,
            "table_name": self.table.table_name,
            "table_id": self.table.table_id,
            "snapshot_before": self.snapshot_before,
            "snapshot_after": self.snapshot_after,
            "rows_flushed": self.rows_flushed,
            "snapshots_expired": list(self.snapshots_expired),
            "merge": self.merge.to_json(),
            "rewrite": self.rewrite.to_json(),
            "file_size_distribution": (
                self.file_size_distribution.to_json()
                if self.file_size_distribution is not None
                else None
            ),
            "partition_distribution": (
                self.partition_distribution.to_json()
                if self.partition_distribution is not None
                else None
            ),
            "changed": self.changed,
            "error": self.error.to_json() if self.error else None,
            "snapshots": (
                self.snapshots.to_json() if self.snapshots is not None else None
            ),
            "revalidation": (
                self.revalidation.to_json() if self.revalidation is not None else None
            ),
            "revalidation_error": self.revalidation_error,
        }


@dataclass(frozen=True, slots=True)
class CheckpointRunReport:
    """Aggregate report of one command run (JSON schema)."""

    version: Literal[1]
    dry_run: bool
    catalog_alias: str
    metadata_catalog: str
    snapshot_before: int
    snapshot_after: int | None = None
    results: tuple[TableCheckpointResult, ...] = ()
    wrote: bool = False
    exit_code: ExitCode = ExitCode.SUCCESS
    warnings: tuple[str, ...] = ()

    @property
    def tables_in_scope(self) -> int:
        return len(self.results)

    @property
    def tables_changed(self) -> int:
        return sum(1 for r in self.results if r.changed)

    @property
    def rows_flushed(self) -> int:
        return sum(r.rows_flushed for r in self.results)

    @property
    def snapshots_expired(self) -> int:
        """Distinct snapshots expired in the run (expire is global)."""
        return len({s for r in self.results for s in r.snapshots_expired})

    @property
    def files_processed(self) -> int:
        return sum(
            r.rewrite.files_processed + r.merge.files_processed for r in self.results
        )

    @property
    def files_created(self) -> int:
        return sum(
            r.rewrite.files_created + r.merge.files_created for r in self.results
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "command": "checkpoint",
            "dry_run": self.dry_run,
            "wrote": self.wrote,
            "catalog": {
                "alias": self.catalog_alias,
                "metadata_catalog": self.metadata_catalog,
            },
            "snapshot_before": self.snapshot_before,
            "snapshot_after": self.snapshot_after,
            "tables": [r.to_json() for r in self.results],
            "totals": {
                "tables_in_scope": self.tables_in_scope,
                "tables_changed": self.tables_changed,
                "rows_flushed": self.rows_flushed,
                "snapshots_expired": self.snapshots_expired,
                "files_processed": self.files_processed,
                "files_created": self.files_created,
            },
            "warnings": list(self.warnings),
            "exit_code": int(self.exit_code),
        }
