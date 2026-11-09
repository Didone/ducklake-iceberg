"""Summary bookkeeping.

Fills in the delete file counters in the Iceberg snapshot summary, ensuring
consistency with the data counters and with the actual content of the
published manifests.
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Mapping, Sequence
from typing import Any

from pyiceberg.manifest import DataFileContent, ManifestContent, ManifestEntryStatus
from pyiceberg.table.snapshots import (
    ADDED_DELETE_FILES,
    ADDED_POSITION_DELETE_FILES,
    ADDED_POSITION_DELETES,
    OPERATION,
    REMOVED_DELETE_FILES,
    REMOVED_POSITION_DELETE_FILES,
    REMOVED_POSITION_DELETES,
    TOTAL_DELETE_FILES,
    TOTAL_EQUALITY_DELETES,
    TOTAL_POSITION_DELETES,
)

from ducklake_iceberg.errors import DuckLakeIcebergError, ExitCode

LOG = logging.getLogger(__name__)

# Constants exported for tests and external use
__all__ = [
    "ADDED_DELETE_FILES",
    "ADDED_POSITION_DELETES",
    "ADDED_POSITION_DELETE_FILES",
    "REMOVED_DELETE_FILES",
    "REMOVED_POSITION_DELETES",
    "REMOVED_POSITION_DELETE_FILES",
    "TOTAL_DELETE_FILES",
    "TOTAL_POSITION_DELETES",
    "DeleteDelta",
    "DeleteTotals",
    "SummaryMismatchError",
    "build_summary",
    "collect_delete_delta",
    "propagate_delete_totals",
    "verify_against_manifest_list",
]


# -- exceptions -------------------------------------------------------------


class SummaryError(DuckLakeIcebergError):
    """Error while computing the summary."""


class SummaryMismatchError(SummaryError):
    """Summary counter differs from the manifest list (exit 3)."""

    exit_code = ExitCode.DIVERGED

    def __init__(self, message: str) -> None:
        super().__init__(
            f"summary mismatch: {message}",
            hint="Do not publish; inspect the manifests",
        )


class NegativeCounterError(SummaryError):
    """Negative counter detected (exit 3)."""

    exit_code = ExitCode.DIVERGED

    def __init__(self, key: str, value: int) -> None:
        super().__init__(
            f"negative counter: {key}={value}",
            hint="Parent totals are inconsistent; recompute by scanning",
        )


class InvalidParentSummaryError(SummaryError):
    """Unreadable parent summary."""

    def __init__(self, key: str, value: str) -> None:
        super().__init__(
            f"unreadable parent snapshot summary: {key}={value}",
            hint="Recompute by scanning and warn",
        )


class UnexpectedContentError(DuckLakeIcebergError):
    """Entry with unexpected content (EQUALITY_DELETES, or DATA in DELETES)."""

    def __init__(self, content: DataFileContent | int) -> None:
        super().__init__(
            f"entry with unexpected content in manifest: {content}",
            hint="Writer bug",
        )


class IncoherentOperationError(DuckLakeIcebergError):
    """Operation inconsistent with the counters."""

    def __init__(self, operation: str, added_delete_files: int) -> None:
        super().__init__(
            f"operation={operation} is inconsistent with "
            f"added-delete-files={added_delete_files}",
            hint="Bug in operation derivation",
        )


class EmptyDeleteFileError(SummaryError):
    """Delete file with record_count=0 published (exit 3)."""

    exit_code = ExitCode.DIVERGED

    def __init__(self, total_delete_files: int, total_position_deletes: int) -> None:
        super().__init__(
            f"total_delete_files={total_delete_files} but "
            f"total_position_deletes={total_position_deletes}",
            hint="Investigate the empty delete file",
        )


# -- data contracts ---------------------------------------------------------


@dataclasses.dataclass(frozen=True, slots=True)
class DeleteDelta:
    """Aggregate of the delete entries in the current commit."""

    added_delete_files: int
    added_position_deletes: int
    removed_delete_files: int
    removed_position_deletes: int


@dataclasses.dataclass(frozen=True, slots=True)
class DeleteTotals:
    """Totals of delete files and position deletes."""

    total_delete_files: int
    total_position_deletes: int


# -- main functions ---------------------------------------------------------


def collect_delete_delta(
    entries: Sequence[Mapping[str, Any]],
) -> DeleteDelta:
    """Scan the entries of the commit's delete manifest.

    Ignores DataFileContent.DATA; rejects EQUALITY_DELETES.

    Args:
        entries: Manifest entries (as Avro dicts)

    Returns:
        DeleteDelta with aggregated counters

    Raises:
        UnexpectedContentError: if an EQUALITY_DELETES entry is found
    """
    added_delete_files = 0
    added_position_deletes = 0
    removed_delete_files = 0
    removed_position_deletes = 0

    for entry in entries:
        status_raw = entry.get("status")
        data_file = entry.get("data_file", {})
        content_raw = data_file.get("content")
        record_count = data_file.get("record_count", 0)

        # Skip entries without a content kind
        if content_raw is None:
            continue

        # content: 0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES
        if content_raw == DataFileContent.DATA:
            # Ignore data files in a mixed manifest (should not happen)
            continue
        elif content_raw == DataFileContent.EQUALITY_DELETES:
            # Equality deletes are not supported
            raise UnexpectedContentError(DataFileContent.EQUALITY_DELETES)
        elif content_raw != DataFileContent.POSITION_DELETES:
            # Unknown content
            raise UnexpectedContentError(content_raw)

        # status: 0=ADDED, 1=EXISTING, 2=DELETED
        if status_raw == ManifestEntryStatus.ADDED:
            added_delete_files += 1
            added_position_deletes += record_count
        elif status_raw == ManifestEntryStatus.DELETED:
            removed_delete_files += 1
            removed_position_deletes += record_count
        # EXISTING does not contribute to the delta

    LOG.debug(
        "Delete delta collected: added_files=%d, added_rows=%d, "
        "removed_files=%d, removed_rows=%d",
        added_delete_files,
        added_position_deletes,
        removed_delete_files,
        removed_position_deletes,
    )

    return DeleteDelta(
        added_delete_files=added_delete_files,
        added_position_deletes=added_position_deletes,
        removed_delete_files=removed_delete_files,
        removed_position_deletes=removed_position_deletes,
    )


def _parent_counter(parent_summary: Mapping[str, str], key: str) -> int:
    """Read a counter from the parent summary; absence counts as zero."""
    value = parent_summary.get(key, "0")
    try:
        return int(value)
    except (TypeError, ValueError) as e:
        raise InvalidParentSummaryError(key, str(value)) from e


def propagate_delete_totals(
    parent_summary: Mapping[str, str] | None,
    delta: DeleteDelta,
) -> DeleteTotals:
    """Propagate delete totals from the parent snapshot.

    parent_summary None (first commit) => zero base.

    Args:
        parent_summary: Parent snapshot summary (None for the first commit)
        delta: Delta of the current commit

    Returns:
        Computed DeleteTotals

    Raises:
        InvalidParentSummaryError: if a parent value is not numeric
        NegativeCounterError: if a result is negative
    """
    # Get the parent's base, treating absence as zero; each key is read
    # separately so the error cites the key and value that failed
    if parent_summary is None:
        base_files = 0
        base_rows = 0
    else:
        base_files = _parent_counter(parent_summary, TOTAL_DELETE_FILES)
        base_rows = _parent_counter(parent_summary, TOTAL_POSITION_DELETES)

    # Compute totals
    total_files = base_files + delta.added_delete_files - delta.removed_delete_files
    total_rows = (
        base_rows + delta.added_position_deletes - delta.removed_position_deletes
    )

    # Check that none is negative
    if total_files < 0:
        raise NegativeCounterError(TOTAL_DELETE_FILES, total_files)
    if total_rows < 0:
        raise NegativeCounterError(TOTAL_POSITION_DELETES, total_rows)

    LOG.info(
        "Delete totals propagated: files %d -> %d, rows %d -> %d",
        base_files,
        total_files,
        base_rows,
        total_rows,
    )

    return DeleteTotals(
        total_delete_files=total_files,
        total_position_deletes=total_rows,
    )


def verify_against_manifest_list(
    totals: DeleteTotals,
    manifest_list_rows: Sequence[Mapping[str, Any]],
) -> None:
    """Verify invariants against the commit's manifest list.

    Raises SummaryMismatchError (exit 3) on a mismatch.

    Invariants checked:
    - total_delete_files == sum(added + existing) over DELETES manifests
    - total_position_deletes == sum(added_rows + existing_rows)
    - No DATA manifest contributes to a delete counter
    - total_delete_files == 0 if and only if total_position_deletes == 0

    Args:
        totals: Totals computed by propagation
        manifest_list_rows: Manifest list rows (as Avro dicts)

    Raises:
        SummaryMismatchError: if any invariant is violated
    """
    files_vivos = 0
    rows_vivas = 0

    for row in manifest_list_rows:
        content = row.get("content")

        # content: 0=DATA, 1=DELETES
        if content == ManifestContent.DATA:
            # Data manifests do not contribute to delete counters
            continue
        elif content != ManifestContent.DELETES:
            # Unknown content, ignore
            continue

        # Sum live files and rows (ADDED + EXISTING)
        added_files = row.get("added_files_count", 0)
        existing_files = row.get("existing_files_count", 0)
        added_rows = row.get("added_rows_count", 0)
        existing_rows = row.get("existing_rows_count", 0)

        files_vivos += added_files + existing_files
        rows_vivas += added_rows + existing_rows

    # Check: total_delete_files == live files
    if totals.total_delete_files != files_vivos:
        raise SummaryMismatchError(
            f"total_delete_files={totals.total_delete_files}, "
            f"manifest list={files_vivos}"
        )

    # Check: total_position_deletes == live rows
    if totals.total_position_deletes != rows_vivas:
        raise SummaryMismatchError(
            f"total_position_deletes={totals.total_position_deletes}, "
            f"manifest list={rows_vivas}"
        )

    # Check: total_delete_files == 0 iff total_position_deletes == 0
    if (totals.total_delete_files == 0) != (totals.total_position_deletes == 0):
        raise EmptyDeleteFileError(
            totals.total_delete_files, totals.total_position_deletes
        )

    LOG.debug(
        "Invariants verified: files=%d, rows=%d (manifest list)",
        files_vivos,
        rows_vivas,
    )


def build_summary(
    *,
    operation: str,
    data_part: Mapping[str, str],
    delete_delta: DeleteDelta,
    delete_totals: DeleteTotals,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Build the complete summary dictionary.

    All values are str. Stable ordering for readable diffs.

    Args:
        operation: Snapshot operation (append, overwrite, etc.)
        data_part: Data counters
        delete_delta: Delete delta of the commit
        delete_totals: Delete totals
        extra: Optional extra counters

    Returns:
        Summary dictionary ready for serialization

    Raises:
        IncoherentOperationError: if operation=append with added deletes
    """
    # Check coherence: append cannot have deletes
    if operation == "append" and delete_delta.added_delete_files > 0:
        raise IncoherentOperationError(operation, delete_delta.added_delete_files)

    summary: dict[str, str] = {}

    # Operation (always first)
    summary[OPERATION] = operation

    # Data counters in alphabetical order
    for key in sorted(data_part.keys()):
        summary[key] = data_part[key]

    # Delete counters: added-* and removed-* only if != 0. Every published
    # delete file is a position delete, so it increments both the generic
    # counter (*-delete-files) and the specific one (*-position-delete-files)
    if delete_delta.added_delete_files > 0:
        summary[ADDED_DELETE_FILES] = str(delete_delta.added_delete_files)
        summary[ADDED_POSITION_DELETE_FILES] = str(delete_delta.added_delete_files)
    if delete_delta.added_position_deletes > 0:
        summary[ADDED_POSITION_DELETES] = str(delete_delta.added_position_deletes)
    if delete_delta.removed_delete_files > 0:
        summary[REMOVED_DELETE_FILES] = str(delete_delta.removed_delete_files)
        summary[REMOVED_POSITION_DELETE_FILES] = str(delete_delta.removed_delete_files)
    if delete_delta.removed_position_deletes > 0:
        summary[REMOVED_POSITION_DELETES] = str(delete_delta.removed_position_deletes)

    # Delete totals: always present
    summary[TOTAL_DELETE_FILES] = str(delete_totals.total_delete_files)
    summary[TOTAL_POSITION_DELETES] = str(delete_totals.total_position_deletes)

    # total-equality-deletes is always zero (not supported)
    summary[TOTAL_EQUALITY_DELETES] = "0"

    # Optional extras
    if extra:
        for key in sorted(extra.keys()):
            summary[key] = extra[key]

    LOG.info(
        "Summary built: operation=%s, total_delete_files=%s, total_position_deletes=%s",
        operation,
        summary[TOTAL_DELETE_FILES],
        summary[TOTAL_POSITION_DELETES],
    )

    return summary
