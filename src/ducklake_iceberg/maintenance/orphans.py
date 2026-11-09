"""Orphan cleanup: reachable set, candidates, and deletion.

The rule that organizes everything (Iceberg): *deleting a file merely because
it did not show up in a listing is incorrect*. Therefore:

- the reachable set starts from Glue's **current** ``metadata_location`` (plus
  ``previous_metadata_location`` and the target of the ``export`` handoff
  pointer, which is the base of the next export) and reads the
  ``metadata.json`` files of the ``metadata-log`` of those roots, **without**
  following their own logs (this is what leaves only the retained history
  reachable), with **all** snapshots, all manifest lists, and all manifests;
- **delete** manifests are opened to protect the position delete files that
  ducklake-iceberg wrote in ``{location}/deletes/``; data manifests are never opened,
  since their ``file_path`` entries are DuckLake data;
- any read failure that is **not** "not found" aborts the cleanup: an
  unreadable file is never treated as unreachable. A cited file that is
  missing also aborts (``Reachable.missing``);
- paths are compared in canonical form, and the current ``metadata_location``
  itself must be in the reachable set.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final, TypeVar, cast

from botocore.exceptions import BotoCoreError, ClientError
from pyiceberg.manifest import ManifestContent, read_manifest_list

from ducklake_iceberg.errors import DuckLakeIcebergError
from ducklake_iceberg.s3 import (
    ObjectStore,
    OrphanKind,
    PrefixOverlapError,
    S3Object,
    canonical_uri,
    check_parquet_in_prefix,
    classify_object,
    validate_prefix_guards,
)

if TYPE_CHECKING:
    from pyiceberg.catalog.glue import GlueCatalog
    from pyiceberg.io import FileIO
    from pyiceberg.manifest import ManifestFile

LOG = logging.getLogger("ducklake_iceberg")

#: Absolute floor of the grace period.
MIN_GRACE: Final[timedelta] = timedelta(hours=1)
#: Lease TTL + 15 min margin.
LEASE_FLOOR: Final[timedelta] = timedelta(minutes=45)
#: Floor derived from ``--commit-timeout``.
COMMIT_TIMEOUT_FLOOR: Final[timedelta] = timedelta(seconds=300)

T = TypeVar("T")


# -- structures -----------------------------------------------------------------


@dataclass(frozen=True)
class Reachable:
    """Files reachable from the roots (canonical paths)."""

    metadata_files: frozenset[str]
    manifest_lists: frozenset[str]
    manifests: frozenset[str]
    missing: frozenset[str]  # reachable and missing -> abort
    delete_files: frozenset[str] = frozenset()  # position deletes
    metadata_location: str | None = None  # current root (Glue), canonical

    def all(self) -> frozenset[str]:
        """All reachable files."""
        return (
            self.metadata_files
            | self.manifest_lists
            | self.manifests
            | self.delete_files
        )


@dataclass(frozen=True)
class OrphanCandidate:
    """Object under the prefix with the deletion decision and the reason."""

    obj: S3Object
    kind: OrphanKind
    age: timedelta
    deletable: bool
    reason: str


@dataclass(frozen=True)
class CleanReport:
    """Cleanup report for one table."""

    prefix: str
    grace: timedelta
    scanned: int
    reachable: int
    candidates: tuple[OrphanCandidate, ...]
    deleted: tuple[str, ...]
    bytes_reclaimed: int
    dry_run: bool
    truncated: bool  # hit --max-deletes
    failed: tuple[tuple[str, str], ...] = ()  # (key, error) of the deletion

    @property
    def deletable(self) -> tuple[OrphanCandidate, ...]:
        return tuple(c for c in self.candidates if c.deletable)

    def to_json(self) -> dict[str, object]:
        return {
            "prefix": self.prefix,
            "grace_s": int(self.grace.total_seconds()),
            "scanned": self.scanned,
            "reachable": self.reachable,
            "deletable": len(self.deletable),
            "deletable_bytes": sum(c.obj.size_bytes for c in self.deletable),
            "deleted": list(self.deleted),
            "bytes_reclaimed": self.bytes_reclaimed,
            "dry_run": self.dry_run,
            "truncated": self.truncated,
            "failed": [{"key": k, "error": e} for k, e in self.failed],
            "candidates": [
                {
                    "key": c.obj.key,
                    "kind": str(c.kind),
                    "size_bytes": c.obj.size_bytes,
                    "last_modified": c.obj.last_modified.isoformat(),
                    "age_s": int(c.age.total_seconds()),
                    "deletable": c.deletable,
                    "reason": c.reason,
                }
                for c in self.candidates
            ],
        }


class CleanError(DuckLakeIcebergError):
    """Cleanup aborted for safety (exit 1)."""


# -- reading (not found vs unreadable) ------------------------------------------


class _NotFound(Exception):  # noqa: N818 - internal signal
    pass


def _read(path: str, what: str, fn: Callable[[], T]) -> T:
    """Run the read; absence becomes ``_NotFound``, anything else aborts."""
    try:
        return fn()
    except FileNotFoundError as exc:
        raise _NotFound(path) from exc
    except Exception as exc:
        raise CleanError(
            f"{what} {path} unreadable ({type(exc).__name__}: {exc}); "
            f"cleanup aborted: an unreadable file is never treated as an orphan",
            hint="check permissions (s3:GetObject) and the file integrity",
        ) from exc


def _exists(io: FileIO, path: str, what: str) -> bool:
    return bool(_read(path, what, lambda: io.new_input(path).exists()))


@dataclass
class _Walker:
    io: FileIO
    metadata_files: set[str] = field(default_factory=set)
    manifest_lists: set[str] = field(default_factory=set)
    manifests: set[str] = field(default_factory=set)
    delete_files: set[str] = field(default_factory=set)
    missing: set[str] = field(default_factory=set)
    _logs: dict[str, list[str]] = field(default_factory=dict)
    _expanded: set[str] = field(default_factory=set)

    def metadata(self, location: str, *, root: bool = False) -> None:
        """Read a ``metadata.json``; for a root, also those in its ``metadata-log``.

        The log of a ``metadata.json`` that only appears in another one's log
        is not followed: without that, every old version would remain
        reachable through the chain and snapshot retention would never free
        anything.
        """
        canon = canonical_uri(location)
        if canon not in self.metadata_files and canon not in self.missing:
            self._read_metadata(location, canon)
        if root and canon not in self._expanded:
            self._expanded.add(canon)
            for previous in self._logs.get(canon, []):
                self.metadata(previous)

    def _read_metadata(self, location: str, canon: str) -> None:
        def load() -> dict[str, Any]:
            with self.io.new_input(location).open() as stream:
                doc = json.load(stream)
            if not isinstance(doc, dict):
                raise ValueError("metadata.json is not a JSON object")
            return doc

        try:
            doc = _read(location, "metadata.json", load)
        except _NotFound:
            LOG.warning("reachable metadata.json is missing: %s", location)
            self.missing.add(canon)
            return
        self.metadata_files.add(canon)
        self._logs[canon] = [
            str(entry["metadata-file"])
            for entry in doc.get("metadata-log") or []
            if isinstance(entry, dict) and entry.get("metadata-file")
        ]
        for snapshot in doc.get("snapshots") or []:
            manifest_list = (
                snapshot.get("manifest-list") if isinstance(snapshot, dict) else None
            )
            if manifest_list:
                self.manifest_list(str(manifest_list))

    def manifest_list(self, path: str) -> None:
        canon = canonical_uri(path)
        if canon in self.manifest_lists or canon in self.missing:
            return
        try:
            manifests = _read(
                path,
                "manifest list",
                lambda: tuple(read_manifest_list(self.io.new_input(path))),
            )
        except _NotFound:
            LOG.warning("reachable manifest list is missing: %s", path)
            self.missing.add(canon)
            return
        self.manifest_lists.add(canon)
        for manifest in manifests:
            self.manifest(manifest)

    def manifest(self, manifest: ManifestFile) -> None:
        path = manifest.manifest_path
        canon = canonical_uri(path)
        if canon in self.manifests or canon in self.missing:
            return
        if manifest.content == ManifestContent.DELETES:
            # opened only to protect ducklake-iceberg's position deletes
            try:
                entries = _read(
                    path,
                    "delete manifest",
                    lambda: tuple(
                        manifest.fetch_manifest_entry(self.io, discard_deleted=False)
                    ),
                )
            except _NotFound:
                LOG.warning("reachable manifest is missing: %s", path)
                self.missing.add(canon)
                return
            self.manifests.add(canon)
            for entry in entries:
                delete_path = entry.data_file.file_path
                if not _exists(self.io, delete_path, "delete file"):
                    self.missing.add(canonical_uri(delete_path))
                self.delete_files.add(canonical_uri(delete_path))
            return
        # data manifest: existence only (the entries point to DuckLake)
        if not _exists(self.io, path, "manifest"):
            LOG.warning("reachable manifest is missing: %s", path)
            self.missing.add(canon)
            return
        self.manifests.add(canon)


# -- reachable set --------------------------------------------------------------


def glue_locations(
    glue: GlueCatalog, *, database: str, table: str
) -> tuple[str, str | None]:
    """Fresh ``(metadata_location, previous_metadata_location)`` from Glue."""
    try:
        response = cast("Any", glue).glue.get_table(DatabaseName=database, Name=table)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code == "EntityNotFoundException":
            raise CleanError(
                f"table {database}.{table} missing from Glue; without a root there is "
                f"no way to decide reachability"
            ) from exc
        raise CleanError(f"glue:GetTable on {database}.{table} failed: {code}") from exc
    except BotoCoreError as exc:
        raise CleanError(f"glue:GetTable on {database}.{table} failed: {exc}") from exc
    params = response.get("Table", {}).get("Parameters") or {}
    location = params.get("metadata_location")
    if not location:
        raise CleanError(f"table {database}.{table} has no metadata_location")
    previous = params.get("previous_metadata_location")
    return str(location), (str(previous) if previous else None)


def compute_reachable(
    glue: GlueCatalog,
    io: FileIO,
    *,
    database: str,
    table: str,
    extra_roots: Sequence[str] = (),
) -> Reachable:
    """Transitive closure starting from Glue.

    Args:
        glue: Glue catalog (``ducklake_iceberg.glue.build_catalog``).
        io: PyIceberg FileIO used to read metadata.
        database: Database in Glue.
        table: Table in Glue.
        extra_roots: Additional roots (e.g. the handoff pointer target).

    Raises:
        CleanError: Missing table, unreadable ``metadata_location``, any read
            that fails for a reason other than "not found", or the current
            ``metadata_location`` outside the reachable set.
    """
    location, previous = glue_locations(glue, database=database, table=table)
    return reachable_from(
        io, location, [*([previous] if previous else []), *extra_roots]
    )


def reachable_from(
    io: FileIO, metadata_location: str, extra_roots: Sequence[str] = ()
) -> Reachable:
    """Transitive closure from a ``metadata_location`` and extra roots."""
    walker = _Walker(io)
    for root in (metadata_location, *extra_roots):
        walker.metadata(root, root=True)
    root_canon = canonical_uri(metadata_location)
    if root_canon not in walker.metadata_files:
        raise CleanError(
            f"current metadata_location {metadata_location} missing or unreadable",
            hint="broken table; investigate before cleaning",
        )
    return Reachable(
        metadata_files=frozenset(walker.metadata_files),
        manifest_lists=frozenset(walker.manifest_lists),
        manifests=frozenset(walker.manifests),
        missing=frozenset(walker.missing),
        delete_files=frozenset(walker.delete_files),
        metadata_location=root_canon,
    )


# -- candidates -----------------------------------------------------------------


def find_orphans(
    objects: Sequence[S3Object],
    reachable: Reachable,
    *,
    grace: timedelta,
    now: datetime,
) -> tuple[OrphanCandidate, ...]:
    """Classify each listed object, in ascending ``last_modified`` order."""
    reachable_all = reachable.all()
    candidates: list[OrphanCandidate] = []
    for obj in objects:
        kind = classify_object(obj.key)
        age = now - obj.last_modified
        if canonical_uri(obj.key) in reachable_all:
            deletable, reason = False, "reachable"
        elif kind is OrphanKind.UNKNOWN:
            deletable, reason = False, "unknown type (never deleted)"
        elif obj.last_modified > now:
            deletable, reason = False, "last_modified in the future (clock)"
        elif age <= grace:
            deletable, reason = False, f"within the grace period ({age})"
        else:
            deletable, reason = True, f"eligible orphan (age {age})"
        candidates.append(
            OrphanCandidate(
                obj=obj, kind=kind, age=age, deletable=deletable, reason=reason
            )
        )
    candidates.sort(key=lambda c: (c.obj.last_modified, c.obj.key))
    return tuple(candidates)


def validate_grace(grace: timedelta) -> None:
    """Grace period floors: 1 h absolute, lease + margin, commit-timeout."""
    floor = max(MIN_GRACE, LEASE_FLOOR, COMMIT_TIMEOUT_FLOOR)
    if grace < floor:
        raise CleanError(
            f"--grace {grace} is below the floor of {floor} "
            f"(lease TTL, --commit-timeout, and absolute floor)"
        )


# -- cleanup --------------------------------------------------------------------


def clean_table(
    glue: GlueCatalog,
    io: FileIO,
    store: ObjectStore,
    *,
    glue_identifier: tuple[str, str],
    prefix: str,
    data_path: str,
    grace: timedelta = timedelta(hours=24),
    apply: bool = False,
    max_deletes: int = 1000,
    extra_roots: Sequence[str] = (),
    now: datetime | None = None,
) -> CleanReport:
    """Delete (or only list, without ``apply``) the orphans of a table.

    Args:
        glue: Glue catalog.
        io: FileIO used to read the metadata.
        store: Listing/deletion of the prefix.
        glue_identifier: ``(database, table)`` in Glue.
        prefix: Metadata prefix of the table (same as the export ``location``).
        data_path: DuckLake ``data_path`` **from the configuration** (guard G2).
        grace: Grace window.
        apply: If False, nothing is deleted.
        max_deletes: Cap on deletions per run.
        extra_roots: Extra roots of the reachable set.
        now: Reference clock (tests); defaults to ``datetime.now(UTC)``.

    Raises:
        CleanError: Safety aborts (exit 1).
        PrefixGuardError: G1/G3 (exit 1); ``PrefixOverlapError``: G2/G4 (exit 2).
    """
    database, table = glue_identifier
    validate_grace(grace)
    if max_deletes < 1:
        raise CleanError(f"--max-deletes must be >= 1 (got {max_deletes})")

    location, previous = glue_locations(glue, database=database, table=table)
    validate_prefix_guards(
        prefix=prefix, metadata_location=location, data_path=data_path
    )

    LOG.info("computing the reachable set of %s.%s", database, table)
    reachable = reachable_from(
        io, location, [*([previous] if previous else []), *extra_roots]
    )
    if reachable.missing:
        raise CleanError(
            f"{len(reachable.missing)} referenced file(s) do not exist: "
            f"{sorted(reachable.missing)[:20]}",
            hint="incident: do not clean a broken table",
        )

    objects = list(store.list_objects(prefix))
    parquet = check_parquet_in_prefix(objects)
    if parquet:
        raise PrefixOverlapError(
            f"metadata prefix {prefix} contains {len(parquet)} .parquet file(s) "
            f"that are not ducklake-iceberg position deletes: {parquet[:5]}",
            hint="review the prefix configuration (the metadata prefix must "
            "not overlap data_path)",
        )

    reference = now if now is not None else datetime.now(UTC)
    candidates = find_orphans(objects, reachable, grace=grace, now=reference)
    to_delete = [c for c in candidates if c.deletable]
    if not apply or not to_delete:
        return CleanReport(
            prefix=prefix,
            grace=grace,
            scanned=len(objects),
            reachable=len(reachable.all()),
            candidates=candidates,
            deleted=(),
            bytes_reclaimed=0,
            dry_run=not apply,
            truncated=False,
        )

    # a commit during the scan invalidates the reachable set
    location_now, _ = glue_locations(glue, database=database, table=table)
    if canonical_uri(location_now) != canonical_uri(location):
        raise CleanError(
            f"concurrent commit detected ({location} -> {location_now}); "
            f"cleanup aborted",
            hint="run again",
        )

    truncated = len(to_delete) > max_deletes
    batch = to_delete[:max_deletes]
    deleted, failed = store.delete_objects([c.obj.key for c in batch])
    deleted_set = set(deleted)
    size_by_key = {c.obj.key: c.obj.size_bytes for c in batch}
    bytes_reclaimed = sum(size_by_key.get(k, 0) for k in deleted_set)
    LOG.info(
        "removed %d file(s) (%d bytes) from prefix %s",
        len(deleted),
        bytes_reclaimed,
        prefix,
    )
    return CleanReport(
        prefix=prefix,
        grace=grace,
        scanned=len(objects),
        reachable=len(reachable.all()),
        candidates=candidates,
        deleted=tuple(deleted),
        bytes_reclaimed=bytes_reclaimed,
        dry_run=False,
        truncated=truncated,
        failed=tuple(failed),
    )
