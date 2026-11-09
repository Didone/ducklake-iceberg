"""Temporal filtering.

Reduces the raw DeleteSet from the reader to the set of deletions actually
visible at the exported snapshot S, applying two independent granularities:
the begin_snapshot/end_snapshot interval of the delete file (file
granularity) and the _ducklake_internal_snapshot_id stored row by row in
partial delete files (row granularity).
"""

from __future__ import annotations

import dataclasses
import enum
import logging
from collections.abc import Sequence, Set

import pyarrow as pa
import pyarrow.compute as pc

from ducklake_iceberg.deletes.reader import DeleteOrigin, DeleteSet, DuckLakeDeleteFile
from ducklake_iceberg.errors import DivergenceError, DuckLakeIcebergError

LOG = logging.getLogger(__name__)


class DanglingPolicy(enum.StrEnum):
    """Policy for deletes that reference data files that are not live."""

    DROP = "drop"  # default: drop and warn
    STRICT = "strict"  # exit 3


@dataclasses.dataclass(frozen=True, slots=True)
class VisibilityReport:
    """Report of the deletion filtering."""

    snapshot_id: int
    files_visible: int
    files_row_filtered: int  # partial_max > S
    rows_read: int
    rows_applied: int
    rows_dropped_future: int  # snapshot_id > S
    rows_dropped_dangling: int  # data file not live at S
    rows_deduplicated: int
    dangling_data_file_ids: tuple[int, ...]

    @property
    def consistent(self) -> bool:
        """Check that the accounting adds up."""
        return self.rows_read == (
            self.rows_applied
            + self.rows_dropped_future
            + self.rows_dropped_dangling
            + self.rows_deduplicated
        )


@dataclasses.dataclass(frozen=True, slots=True)
class DeltaDeleteFiles:
    """Delta of delete files between two snapshots."""

    added_or_changed: tuple[DuckLakeDeleteFile, ...]
    gone: tuple[int, ...]  # published delete_file_ids that disappeared
    unchanged: tuple[DuckLakeDeleteFile, ...]


# -- exceptions ---------------------------------------------------------------


class DanglingDeletesError(DivergenceError):
    """Deletions reference data files outside the snapshot."""

    def __init__(
        self, count: int, snapshot_id: int, data_file_ids: Sequence[int]
    ) -> None:
        ids_str = ", ".join(str(id_) for id_ in data_file_ids[:10])
        if len(data_file_ids) > 10:
            ids_str += f", ... ({len(data_file_ids)} total)"
        msg = (
            f"{count} deletions reference data files outside "
            f"snapshot {snapshot_id}: {ids_str}"
        )
        super().__init__(
            msg,
            hint="Run 'ducklake-iceberg doctor' or consider --materialize-deletes",
        )


class PartialMaxInconsistencyError(DivergenceError):
    """partial_max inconsistent with the file contents."""

    def __init__(
        self,
        path: str,
        partial_max: int | None,
        found_snapshot: int,
        snapshot_id: int,
    ) -> None:
        msg = (
            f"delete file {path}: partial_max={partial_max} but there are rows from "
            f"snapshot {found_snapshot} > {snapshot_id}"
        )
        super().__init__(
            msg,
            hint="Report a catalog bug; do not export this table",
        )


class VisibilityAccountingError(DuckLakeIcebergError):
    """Filtering accounting does not add up."""

    def __init__(self, report: VisibilityReport) -> None:
        msg = (
            f"filtering accounting does not add up: read={report.rows_read} "
            f"applied={report.rows_applied} future={report.rows_dropped_future} "
            f"dangling={report.rows_dropped_dangling} "
            f"duplicates={report.rows_deduplicated}"
        )
        super().__init__(
            msg,
            hint="Internal bug; open an issue",
        )


class NonexistentDataFileError(DivergenceError):
    """Delete file references a nonexistent data_file_id."""

    def __init__(self, delete_file_id: int, data_file_id: int) -> None:
        msg = (
            f"delete file {delete_file_id} references nonexistent data_file_id "
            f"{data_file_id}"
        )
        super().__init__(
            msg,
            hint="Race with maintenance; rerun the sync",
        )


# -- visibility functions -----------------------------------------------


def visible_at(f: DuckLakeDeleteFile, snapshot_id: int) -> bool:
    """Check whether a delete file is visible at the snapshot.

    DuckLake versioning rule: half-open intervals
    S >= begin_snapshot AND (end_snapshot IS NULL OR S < end_snapshot)
    """
    if snapshot_id < f.begin_snapshot:
        return False
    return not (f.end_snapshot is not None and snapshot_id >= f.end_snapshot)


def needs_row_filter(f: DuckLakeDeleteFile, snapshot_id: int) -> bool:
    """Check whether the delete file needs row-level filtering.

    partial_max is not None and partial_max > snapshot_id
    """
    return f.partial_max is not None and f.partial_max > snapshot_id


def _drop_duplicate_positions(t: pa.Table) -> tuple[pa.Table, int]:
    """Remove duplicates of (data_file_path, pos), keeping the lowest snapshot_id.

    Assumes the table is already sorted by (data_file_path, pos, snapshot_id).
    Returns (filtered table, count of removed rows).
    """
    if len(t) == 0:
        return t, 0

    # Vectorized: a row is a duplicate when (path, pos) repeats the previous one
    paths = t["data_file_path"].combine_chunks()
    positions = t["pos"].combine_chunks()
    same_as_prev = pc.and_(
        pc.equal(paths[1:], paths[:-1]),
        pc.equal(positions[1:], positions[:-1]),
    )
    keep_mask = pa.concat_arrays([pa.array([True]), pc.invert(same_as_prev)])

    duplicates = len(t) - pc.sum(pc.cast(keep_mask, pa.int64())).as_py()
    if duplicates == 0:
        return t, 0

    return t.filter(keep_mask), duplicates


def _dangling_data_file_ids(
    rows: pa.Table, files: Sequence[DuckLakeDeleteFile]
) -> tuple[int, ...]:
    """``data_file_id``s referenced by the dangling rows.

    ``source_id`` is the ``delete_file_id`` for ``DELETE_FILE`` (resolved to
    the ``data_file_id`` through the catalog) and the ``data_file_id`` itself
    for ``INLINED``.
    """
    data_file_by_delete = {f.delete_file_id: f.data_file_id for f in files}
    ids: set[int] = set()
    for origin, source_id in zip(
        rows["origin"].cast(pa.string()).to_pylist(),
        rows["source_id"].to_pylist(),
        strict=True,
    ):
        if origin == DeleteOrigin.INLINED:
            ids.add(source_id)
        else:
            ids.add(data_file_by_delete.get(source_id, source_id))
    return tuple(sorted(ids))


def filter_visible(
    ds: DeleteSet,
    *,
    snapshot_id: int,
    live_data_files: Set[str],
    policy: DanglingPolicy = DanglingPolicy.DROP,
) -> tuple[pa.Table, VisibilityReport]:
    """Filter the deletions visible at the snapshot.

    Args:
        ds: Raw DeleteSet from the reader
        snapshot_id: Target snapshot
        live_data_files: Set of paths of data files live at S
        policy: Policy for dangling deletes

    Returns:
        (filtered table, visibility report)
    """
    t = ds.positions
    rows_read = len(t)

    # Count visible files and those that need filtering
    files_visible = sum(1 for f in ds.files if visible_at(f, snapshot_id))
    files_row_filtered = sum(
        1
        for f in ds.files
        if visible_at(f, snapshot_id) and needs_row_filter(f, snapshot_id)
    )

    if rows_read == 0:
        # Empty table: return a zeroed report
        return t, VisibilityReport(
            snapshot_id=snapshot_id,
            files_visible=files_visible,
            files_row_filtered=files_row_filtered,
            rows_read=0,
            rows_applied=0,
            rows_dropped_future=0,
            rows_dropped_dangling=0,
            rows_deduplicated=0,
            dangling_data_file_ids=(),
        )

    # 1. Row rule: snapshot_id <= S (applied unconditionally)
    future_mask = pc.greater(t["snapshot_id"], snapshot_id)
    future_count = pc.sum(pc.cast(future_mask, pa.int64())).as_py()

    # Check for inconsistency with partial_max
    if future_count > 0:
        # Get the max snapshot_id of the future rows
        future_rows = t.filter(future_mask)
        max_future_snapshot = pc.max(future_rows["snapshot_id"]).as_py()

        # Future rows from a file without partial_max > S are an inconsistency:
        # the origin of each row is identified by (origin, source_id), never by
        # data_file_path (which is the data file path, not the delete file's)
        from_delete_file = pc.equal(
            t["origin"].cast(pa.string()), DeleteOrigin.DELETE_FILE.value
        )
        for f in ds.files:
            if visible_at(f, snapshot_id) and not needs_row_filter(f, snapshot_id):
                file_mask = pc.and_(
                    from_delete_file, pc.equal(t["source_id"], f.delete_file_id)
                )
                file_future_mask = pc.and_(file_mask, future_mask)
                if pc.sum(pc.cast(file_future_mask, pa.int64())).as_py() > 0:
                    raise PartialMaxInconsistencyError(
                        f.path,
                        f.partial_max,
                        max_future_snapshot,
                        snapshot_id,
                    )

    t = t.filter(pc.invert(future_mask))
    dropped_future = future_count

    # 2. Delete pointing to a data file outside the snapshot
    if len(t) > 0:
        live_mask = pc.is_in(
            t["data_file_path"],
            value_set=pa.array(sorted(live_data_files), type=pa.large_string()),
        )
        dangling_mask = pc.invert(live_mask)
        dangling_count = pc.sum(pc.cast(dangling_mask, pa.int64())).as_py()

        # Collect the dangling data_file_ids
        dangling_data_file_ids: tuple[int, ...] = ()
        if dangling_count > 0:
            dangling_data_file_ids = _dangling_data_file_ids(
                t.filter(dangling_mask), ds.files
            )

            if policy == DanglingPolicy.STRICT:
                raise DanglingDeletesError(
                    dangling_count, snapshot_id, dangling_data_file_ids
                )

            LOG.warning(
                "Dropping %d dangling deletions (data files not live at snapshot %d)",
                dangling_count,
                snapshot_id,
            )

        t = t.filter(live_mask)
        dropped_dangling = dangling_count
    else:
        dropped_dangling = 0
        dangling_data_file_ids = ()

    # 3. Deduplication (file_path, pos), lowest snapshot_id wins
    if len(t) > 0:
        t = t.sort_by(
            [
                ("data_file_path", "ascending"),
                ("pos", "ascending"),
                ("snapshot_id", "ascending"),
            ]
        )
        t, deduplicated = _drop_duplicate_positions(t)
    else:
        deduplicated = 0

    rows_applied = len(t)

    # Build the report
    report = VisibilityReport(
        snapshot_id=snapshot_id,
        files_visible=files_visible,
        files_row_filtered=files_row_filtered,
        rows_read=rows_read,
        rows_applied=rows_applied,
        rows_dropped_future=dropped_future,
        rows_dropped_dangling=dropped_dangling,
        rows_deduplicated=deduplicated,
        dangling_data_file_ids=dangling_data_file_ids,
    )

    # Check consistency
    if not report.consistent:
        raise VisibilityAccountingError(report)

    if deduplicated > 0:
        LOG.info(
            "Deduplicated %d duplicate positions (keeping the lowest snapshot_id)",
            deduplicated,
        )

    return t, report


def delta_delete_files(
    current: Sequence[DuckLakeDeleteFile],
    *,
    previous_snapshot_id: int,
    published_delete_file_ids: Set[int],
) -> DeltaDeleteFiles:
    """Compute the delta of delete files between two snapshots.

    The rule used for data files does not work here because:
    - begin_snapshot is preserved when a delete file is replaced
    - end_snapshot is not reliable (the row disappears from the catalog)

    The correct rule:
    - Added/changed: begin_snapshot > previous OR partial_max > previous
    - Removed: set difference (published - current)
    - Unchanged: the rest

    Args:
        current: Delete files live at the current snapshot
        previous_snapshot_id: Previous snapshot
        published_delete_file_ids: IDs published to Iceberg in the previous sync

    Returns:
        Classified delta
    """
    added_or_changed: list[DuckLakeDeleteFile] = []
    unchanged: list[DuckLakeDeleteFile] = []

    current_ids = {f.delete_file_id for f in current}

    for f in current:
        # New or changed?
        is_new = f.begin_snapshot > previous_snapshot_id
        is_changed = f.partial_max is not None and f.partial_max > previous_snapshot_id

        if is_new or is_changed:
            added_or_changed.append(f)
        else:
            unchanged.append(f)

    # Removed: published but no longer present
    gone = tuple(sorted(published_delete_file_ids - current_ids))

    return DeltaDeleteFiles(
        added_or_changed=tuple(added_or_changed),
        gone=gone,
        unchanged=tuple(unchanged),
    )
