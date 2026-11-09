"""Application order of deletes.

Ensures published position delete files apply to exactly the rows DuckLake
considers deleted, respecting the Iceberg v2 sequence number rule: a delete
file only affects data files with a lower or equal sequence.
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Mapping, Sequence
from typing import Any

from pyiceberg.manifest import DataFileContent, ManifestContent, ManifestEntryStatus

from iceduck.errors import IceduckError

LOG = logging.getLogger(__name__)

# Constant for readability at call sites
INHERITED: None = None


# -- exceptions -------------------------------------------------------------


class SequenceInvariantError(IceduckError):
    """Sequence number invariant violated."""


class MissingSequenceError(SequenceInvariantError):
    """EXISTING/DELETED entry without an original sequence."""

    def __init__(self, path: str, status: ManifestEntryStatus) -> None:
        super().__init__(
            f"entry {path} with status {status} requires an original sequence_number",
            hint="Writer bug; the commit is aborted before writing to S3",
        )


class AddedWithSequenceError(SequenceInvariantError):
    """ADDED entry with a sequence set."""

    def __init__(self, path: str) -> None:
        super().__init__(
            f"ADDED entry {path} must inherit sequence_number (None)",
            hint="Writer bug; the commit is aborted before writing to S3",
        )


class ReapprovedManifestSequenceError(SequenceInvariantError):
    """Reused manifest had its min_sequence_number changed."""

    def __init__(self, path: str, original: int, recalculated: int) -> None:
        super().__init__(
            f"reused manifest {path} had min_sequence_number changed: "
            f"original={original}, recalculated={recalculated}",
            hint="Writer bug; the commit is aborted before writing to S3",
        )


class DeleteApplicabilityError(SequenceInvariantError):
    """Delete does not reach the data file because of inverted sequences."""

    def __init__(
        self, delete_path: str, delete_seq: int, data_path: str, data_seq: int
    ) -> None:
        super().__init__(
            f"delete {delete_path} (seq {delete_seq}) does not reach "
            f"{data_path} (seq {data_seq})",
            hint="Abort the commit and investigate the sequence assignment",
        )


class MixedContentError(SequenceInvariantError):
    """Manifest mixing DATA and POSITION_DELETES."""

    def __init__(self, path: str) -> None:
        super().__init__(
            f"manifest {path} cannot mix content kinds",
            hint="Keep data manifests and delete manifests separate",
        )


# -- data contracts ---------------------------------------------------------


@dataclasses.dataclass(frozen=True, slots=True)
class SequenceAssignment:
    """Sequence number assignment for an entry."""

    sequence_number: int | None
    file_sequence_number: int | None


@dataclasses.dataclass(frozen=True, slots=True)
class ManifestListEntrySequence:
    """Sequences for a manifest list entry."""

    sequence_number: int  # sequence of the commit that wrote the manifest
    min_sequence_number: int  # lowest data sequence among live entries
    added_snapshot_id: int


# -- main functions ---------------------------------------------------------


def resolve_sequence(
    *,
    status: ManifestEntryStatus,
    original_sequence_number: int | None,
    original_file_sequence_number: int | None,
) -> SequenceAssignment:
    """Resolve an entry's sequence numbers according to its status.

    Rule:
    - ADDED: (None, None) - inherited from the manifest list
    - EXISTING/DELETED: original numbers, which must be non-null

    Args:
        status: Entry status
        original_sequence_number: Original sequence (if any)
        original_file_sequence_number: Original file sequence (if any)

    Returns:
        SequenceAssignment with the correct numbers

    Raises:
        MissingSequenceError: if EXISTING/DELETED has no original sequence
        AddedWithSequenceError: if ADDED has a sequence set
    """
    if status == ManifestEntryStatus.ADDED:
        # ADDED: must inherit (None, None)
        if original_sequence_number is not None:
            # No path available here; use a placeholder
            raise AddedWithSequenceError("<entry>")
        return SequenceAssignment(
            sequence_number=None,
            file_sequence_number=None,
        )

    # EXISTING or DELETED: must have original numbers
    if original_sequence_number is None or original_file_sequence_number is None:
        raise MissingSequenceError("<entry>", status)

    return SequenceAssignment(
        sequence_number=original_sequence_number,
        file_sequence_number=original_file_sequence_number,
    )


def assert_sequence_invariants(
    *,
    entries: Sequence[Mapping[str, Any]],
    manifest_content: ManifestContent,
    commit_sequence_number: int,
) -> None:
    """Validate sequence number invariants before closing the Avro file.

    Checks:
    1. ADDED entries have sequence_number and file_sequence_number None
    2. EXISTING/DELETED entries have explicit numbers
    3. The manifest does not mix DATA and POSITION_DELETES

    Args:
        entries: List of entries as Avro dicts
        manifest_content: Manifest content kind (DATA or DELETES)
        commit_sequence_number: Current commit sequence

    Raises:
        SequenceInvariantError: if any invariant is violated
    """
    if not entries:
        return

    seen_contents: set[int] = set()

    for entry in entries:
        status = entry.get("status")
        sequence_number = entry.get("sequence_number")
        file_sequence_number = entry.get("file_sequence_number")
        data_file = entry.get("data_file", {})
        file_path = data_file.get("file_path", "<unknown>")
        content = data_file.get("content", 0)

        # Track content kinds to detect mixing
        seen_contents.add(content)

        # Check sequence numbers by status (Iceberg: EXISTING=0, ADDED=1,
        # DELETED=2 -- always via the enum, never by literal)
        if status == ManifestEntryStatus.ADDED:
            if sequence_number is not None or file_sequence_number is not None:
                raise AddedWithSequenceError(file_path)
        elif status in {ManifestEntryStatus.EXISTING, ManifestEntryStatus.DELETED} and (
            sequence_number is None or file_sequence_number is None
        ):
            raise MissingSequenceError(file_path, ManifestEntryStatus(status))

    # Check there is no mixed or swapped content: a DATA manifest only holds
    # DataFileContent.DATA; a DELETES manifest never holds DATA
    is_data_manifest = manifest_content == ManifestContent.DATA
    has_data = DataFileContent.DATA in seen_contents
    if len(seen_contents) > 1 or has_data != is_data_manifest:
        raise MixedContentError("<manifest>")

    LOG.debug(
        "Sequence invariants validated: %d entries, content=%s",
        len(entries),
        manifest_content,
    )


def assert_delete_applicability(
    *,
    data_sequence_numbers: Mapping[str, int],
    delete_sequence_numbers: Mapping[str, int],
    referenced: Mapping[str, str],
) -> None:
    """Check that data_seq <= delete_seq for every referenced pair.

    The rule: a position delete only applies to data files with a sequence
    lower than or equal to its own. Equality is allowed (full export case).

    Args:
        data_sequence_numbers: Map file_path -> data sequence
        delete_sequence_numbers: Map delete_path -> sequence
        referenced: Map delete_path -> referenced data_path

    Raises:
        DeleteApplicabilityError: if data_seq > delete_seq
    """
    for delete_path, data_path in referenced.items():
        delete_seq = delete_sequence_numbers.get(delete_path)
        data_seq = data_sequence_numbers.get(data_path)

        if delete_seq is None or data_seq is None:
            # Skip if we lack complete information
            continue

        # Check the rule: data_seq <= delete_seq
        if data_seq > delete_seq:
            raise DeleteApplicabilityError(delete_path, delete_seq, data_path, data_seq)

    LOG.debug(
        "Delete applicability validated: %d referenced pairs",
        len(referenced),
    )


def calculate_min_sequence_number(
    entries: Sequence[Mapping[str, Any]],
    commit_sequence_number: int,
) -> int:
    """Compute min_sequence_number for a manifest.

    For a new manifest with only ADDED entries, returns the commit sequence.
    For a manifest with existing entries, returns the lowest data sequence.

    Args:
        entries: List of entries as Avro dicts
        commit_sequence_number: Current commit sequence

    Returns:
        Lowest data sequence among live entries
    """
    if not entries:
        return commit_sequence_number

    # Collect the entries' sequences
    sequences: list[int] = []

    for entry in entries:
        status = entry.get("status")
        # Ignore DELETED entries
        if status == ManifestEntryStatus.DELETED:
            continue

        sequence_number = entry.get("sequence_number")
        if sequence_number is not None:
            # EXISTING entry with an explicit sequence
            sequences.append(sequence_number)
        else:
            # ADDED entry, inherits from the commit
            sequences.append(commit_sequence_number)

    if not sequences:
        return commit_sequence_number

    return min(sequences)


def validate_reapproved_manifest_sequence(
    *,
    manifest_path: str,
    original_sequence_number: int,
    original_min_sequence_number: int,
    recalculated_min_sequence_number: int,
) -> None:
    """Validate that a reused manifest did not have its sequences changed.

    A reused manifest must keep its original sequence_number and
    min_sequence_number in the manifest list.

    Args:
        manifest_path: Manifest path
        original_sequence_number: Manifest's original sequence
        original_min_sequence_number: Original min_sequence
        recalculated_min_sequence_number: Recalculated min_sequence

    Raises:
        ReapprovedManifestSequenceError: if there was a change
    """
    if original_min_sequence_number != recalculated_min_sequence_number:
        raise ReapprovedManifestSequenceError(
            manifest_path,
            original_min_sequence_number,
            recalculated_min_sequence_number,
        )
