"""Sequence number invariants.

Statuses always come from the enum (Iceberg: EXISTING=0, ADDED=1, DELETED=2):
the Avro dicts carry the integer, and the test pins that the code does not
swap the codes.
"""

from __future__ import annotations

from typing import Any

import pytest
from pyiceberg.manifest import DataFileContent, ManifestContent, ManifestEntryStatus

from ducklake_iceberg.writer import sequence as sq

_ADDED = int(ManifestEntryStatus.ADDED)
_EXISTING = int(ManifestEntryStatus.EXISTING)
_DELETED = int(ManifestEntryStatus.DELETED)


def _entry(
    status: int,
    seq: int | None = None,
    file_seq: int | None = None,
    content: DataFileContent = DataFileContent.DATA,
) -> dict[str, Any]:
    return {
        "status": status,
        "sequence_number": seq,
        "file_sequence_number": file_seq,
        "data_file": {"file_path": "/d/a.parquet", "content": int(content)},
    }


def test_status_codes_are_iceberg_codes() -> None:
    assert (_EXISTING, _ADDED, _DELETED) == (0, 1, 2)


# -- resolve_sequence -------------------------------------------------------
def test_resolve_added_inherits() -> None:
    got = sq.resolve_sequence(
        status=ManifestEntryStatus.ADDED,
        original_sequence_number=None,
        original_file_sequence_number=None,
    )
    assert got == sq.SequenceAssignment(sq.INHERITED, sq.INHERITED)


def test_resolve_added_with_sequence_rejected() -> None:
    with pytest.raises(sq.AddedWithSequenceError):
        sq.resolve_sequence(
            status=ManifestEntryStatus.ADDED,
            original_sequence_number=3,
            original_file_sequence_number=3,
        )


@pytest.mark.parametrize(
    "status", [ManifestEntryStatus.EXISTING, ManifestEntryStatus.DELETED]
)
def test_resolve_existing_keeps_original(status: ManifestEntryStatus) -> None:
    got = sq.resolve_sequence(
        status=status, original_sequence_number=3, original_file_sequence_number=2
    )
    assert got == sq.SequenceAssignment(3, 2)


@pytest.mark.parametrize(("seq", "file_seq"), [(None, 2), (3, None)])
def test_resolve_existing_without_original(
    seq: int | None, file_seq: int | None
) -> None:
    with pytest.raises(sq.MissingSequenceError, match="EXISTING"):
        sq.resolve_sequence(
            status=ManifestEntryStatus.EXISTING,
            original_sequence_number=seq,
            original_file_sequence_number=file_seq,
        )


# -- assert_sequence_invariants ---------------------------------------------
def test_invariants_accept_valid_manifest() -> None:
    sq.assert_sequence_invariants(
        entries=[_entry(_ADDED), _entry(_EXISTING, 2, 2), _entry(_DELETED, 1, 1)],
        manifest_content=ManifestContent.DATA,
        commit_sequence_number=3,
    )
    sq.assert_sequence_invariants(
        entries=[], manifest_content=ManifestContent.DATA, commit_sequence_number=1
    )


@pytest.mark.parametrize(("seq", "file_seq"), [(3, None), (None, 3)])
def test_invariants_added_with_sequence(seq: int | None, file_seq: int | None) -> None:
    """Regression: status 1 is ADDED (the code used to treat 0 as ADDED)."""
    with pytest.raises(sq.AddedWithSequenceError, match="ADDED"):
        sq.assert_sequence_invariants(
            entries=[_entry(_ADDED, seq, file_seq)],
            manifest_content=ManifestContent.DATA,
            commit_sequence_number=3,
        )


@pytest.mark.parametrize(
    ("status", "name"), [(_EXISTING, "EXISTING"), (_DELETED, "DELETED")]
)
@pytest.mark.parametrize(("seq", "file_seq"), [(None, 1), (1, None)])
def test_invariants_existing_without_sequence(
    status: int, name: str, seq: int | None, file_seq: int | None
) -> None:
    """Regression: status 0 is EXISTING and requires an explicit sequence."""
    with pytest.raises(sq.MissingSequenceError, match=name):
        sq.assert_sequence_invariants(
            entries=[_entry(status, seq, file_seq)],
            manifest_content=ManifestContent.DATA,
            commit_sequence_number=3,
        )


def test_invariants_reject_mixed_content() -> None:
    with pytest.raises(sq.MixedContentError):
        sq.assert_sequence_invariants(
            entries=[
                _entry(_ADDED),
                _entry(_ADDED, content=DataFileContent.POSITION_DELETES),
            ],
            manifest_content=ManifestContent.DATA,
            commit_sequence_number=3,
        )


@pytest.mark.parametrize(
    ("content", "manifest"),
    [
        (DataFileContent.POSITION_DELETES, ManifestContent.DATA),
        (DataFileContent.DATA, ManifestContent.DELETES),
    ],
)
def test_invariants_reject_content_of_other_manifest(
    content: DataFileContent, manifest: ManifestContent
) -> None:
    with pytest.raises(sq.MixedContentError):
        sq.assert_sequence_invariants(
            entries=[_entry(_ADDED, content=content)],
            manifest_content=manifest,
            commit_sequence_number=3,
        )


def test_invariants_accept_delete_manifest() -> None:
    sq.assert_sequence_invariants(
        entries=[_entry(_ADDED, content=DataFileContent.POSITION_DELETES)],
        manifest_content=ManifestContent.DELETES,
        commit_sequence_number=3,
    )


# -- assert_delete_applicability --------------------------------------------
def test_delete_applicability() -> None:
    sq.assert_delete_applicability(
        data_sequence_numbers={"/d/a": 2, "/d/b": 3},
        delete_sequence_numbers={"/x/1": 3, "/x/2": 3},
        referenced={"/x/1": "/d/a", "/x/2": "/d/b", "/x/3": "/d/c"},
    )
    with pytest.raises(sq.DeleteApplicabilityError, match="seq 2"):
        sq.assert_delete_applicability(
            data_sequence_numbers={"/d/a": 4},
            delete_sequence_numbers={"/x/1": 2},
            referenced={"/x/1": "/d/a"},
        )


# -- calculate_min_sequence_number ------------------------------------------
def test_min_sequence_number() -> None:
    assert sq.calculate_min_sequence_number([], 5) == 5
    assert sq.calculate_min_sequence_number([_entry(_ADDED)], 5) == 5
    assert (
        sq.calculate_min_sequence_number(
            [_entry(_ADDED), _entry(_EXISTING, 2, 2), _entry(_DELETED, 1, 1)], 5
        )
        == 2
    )
    # only DELETED: nothing live, falls back to the commit sequence
    assert sq.calculate_min_sequence_number([_entry(_DELETED, 1, 1)], 5) == 5


# -- validate_reapproved_manifest_sequence ----------------------------------
def test_reapproved_manifest_sequence() -> None:
    sq.validate_reapproved_manifest_sequence(
        manifest_path="/m.avro",
        original_sequence_number=3,
        original_min_sequence_number=2,
        recalculated_min_sequence_number=2,
    )
    with pytest.raises(sq.ReapprovedManifestSequenceError, match="original=2"):
        sq.validate_reapproved_manifest_sequence(
            manifest_path="/m.avro",
            original_sequence_number=3,
            original_min_sequence_number=2,
            recalculated_min_sequence_number=1,
        )


def test_data_contracts() -> None:
    entry = sq.ManifestListEntrySequence(
        sequence_number=3, min_sequence_number=2, added_snapshot_id=9
    )
    assert entry.min_sequence_number <= entry.sequence_number
