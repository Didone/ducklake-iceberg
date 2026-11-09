"""Physical cleanup of DuckLake files scheduled for deletion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from iceduck.catalog.connection import DuckLakeConnection
from iceduck.doctor.model import quote_ident
from iceduck.errors import ExitCode, IceduckError
from iceduck.s3 import ObjectStore, canonical_uri, is_under


class DuckLakeCleanupError(IceduckError):
    """Failure to plan or finish the DuckLake physical cleanup."""


@dataclass(frozen=True, slots=True)
class CleanupRecord:
    data_file_id: int
    path: str
    path_is_relative: bool
    candidate_path: str


@dataclass(frozen=True, slots=True)
class DuckLakeCleanupPlan:
    records: tuple[CleanupRecord, ...]
    truncated: bool

    @property
    def paths(self) -> tuple[str, ...]:
        return tuple(record.candidate_path for record in self.records)


@dataclass(frozen=True, slots=True)
class DuckLakeCleanupReport:
    candidates: tuple[str, ...]
    referenced: tuple[str, ...]
    deleted: tuple[str, ...]
    failed: tuple[tuple[str, str], ...]
    dry_run: bool
    truncated: bool

    @property
    def exit_code(self) -> ExitCode:
        if self.referenced:
            return ExitCode.BLOCKED
        if self.failed:
            return ExitCode.ERROR
        if self.candidates and (self.dry_run or self.deleted):
            return ExitCode.SUCCESS
        return ExitCode.NOTHING_TO_DO

    def to_json(self) -> dict[str, Any]:
        return {
            "scope": "ducklake",
            "candidates": list(self.candidates),
            "referenced": list(self.referenced),
            "deleted": list(self.deleted),
            "failed": [{"path": path, "error": error} for path, error in self.failed],
            "dry_run": self.dry_run,
            "truncated": self.truncated,
        }


def _matches_record(path: str, record_path: str, relative: bool) -> bool:
    candidate = canonical_uri(path)
    if not relative:
        return candidate == canonical_uri(record_path)
    suffix = record_path.replace("\\", "/").lstrip("/")
    return candidate.endswith("/" + suffix)


def _cleanup_records(
    con: DuckLakeConnection,
    *,
    older_than: datetime,
    max_deletes: int,
    data_path: str,
) -> DuckLakeCleanupPlan:
    if older_than.tzinfo is None:
        raise DuckLakeCleanupError("older_than must include a timezone")
    if max_deletes < 1:
        raise DuckLakeCleanupError("--max-deletes must be greater than zero")

    try:
        paths = [
            canonical_uri(str(row[0]))
            for row in con.raw.execute(
                "SELECT path FROM ducklake_cleanup_old_files("
                "?, older_than => ?::TIMESTAMPTZ, dry_run => true)",
                [con.alias, older_than],
            ).fetchall()
            if row[0]
        ]
        scheduled = con.raw.execute(
            f"SELECT data_file_id, path, path_is_relative "
            f"FROM {quote_ident(con.meta)}.ducklake_files_scheduled_for_deletion"
        ).fetchall()
    except Exception as exc:
        raise DuckLakeCleanupError(
            f"failed to list scheduled DuckLake files: {exc}"
        ) from exc

    unique_paths = sorted(set(paths))
    records: list[CleanupRecord] = []
    for candidate in unique_paths:
        if not is_under(candidate, data_path):
            raise DuckLakeCleanupError(
                f"eligible file outside data_path: {candidate}",
                hint="check data_path and the paths registered in the catalog",
            )
        matches = [
            row
            for row in scheduled
            if row[0] is not None
            and _matches_record(candidate, str(row[1]), bool(row[2]))
        ]
        if len(matches) != 1:
            raise DuckLakeCleanupError(
                f"could not uniquely match the scheduled file "
                f"{candidate} to the catalog"
            )
        data_file_id, path, relative = matches[0]
        records.append(
            CleanupRecord(
                data_file_id=int(data_file_id),
                path=str(path),
                path_is_relative=bool(relative),
                candidate_path=candidate,
            )
        )

    truncated = len(records) > max_deletes
    return DuckLakeCleanupPlan(tuple(records[:max_deletes]), truncated)


def plan_scheduled_files(
    con: DuckLakeConnection,
    *,
    older_than: datetime,
    max_deletes: int,
    data_path: str,
) -> DuckLakeCleanupPlan:
    """Plan only files returned by the official DuckLake dry run."""
    with con.read_transaction():
        return _cleanup_records(
            con,
            older_than=older_than,
            max_deletes=max_deletes,
            data_path=data_path,
        )


def apply_cleanup_plan(
    con: DuckLakeConnection,
    store: ObjectStore,
    plan: DuckLakeCleanupPlan,
    *,
    apply: bool,
    referenced_paths: set[str],
) -> DuckLakeCleanupReport:
    """Delete via the object store, then confirm in the DuckLake catalog."""
    records = plan.records
    candidates = plan.paths
    referenced = tuple(
        sorted(set(candidates) & {canonical_uri(path) for path in referenced_paths})
    )
    if referenced or not apply or not records:
        return DuckLakeCleanupReport(
            candidates=candidates,
            referenced=referenced,
            deleted=(),
            failed=(),
            dry_run=not apply,
            truncated=plan.truncated,
        )

    deleted_raw, failed = store.delete_objects(candidates)
    deleted_set = {canonical_uri(path) for path in deleted_raw}
    failed_paths = {canonical_uri(path) for path, _error in failed}
    unconfirmed = [
        path
        for path in candidates
        if canonical_uri(path) not in deleted_set
        and canonical_uri(path) not in failed_paths
    ]
    failed.extend(
        (path, "object store did not confirm the deletion") for path in unconfirmed
    )
    deleted = tuple(path for path in candidates if canonical_uri(path) in deleted_set)
    deleted_records = [
        record
        for record in records
        if canonical_uri(record.candidate_path) in deleted_set
    ]

    if deleted_records:
        table = f"{quote_ident(con.meta)}.ducklake_files_scheduled_for_deletion"
        with con.transaction():
            for record in deleted_records:
                con.execute(
                    f"DELETE FROM {table} "
                    "WHERE data_file_id = ? AND path = ? AND path_is_relative = ?",
                    [
                        record.data_file_id,
                        record.path,
                        record.path_is_relative,
                    ],
                )
                remaining = con.raw.execute(
                    f"SELECT count(*) FROM {table} "
                    "WHERE data_file_id = ? AND path = ? AND path_is_relative = ?",
                    [
                        record.data_file_id,
                        record.path,
                        record.path_is_relative,
                    ],
                ).fetchone()
                if remaining is None or int(remaining[0]) != 0:
                    raise DuckLakeCleanupError(
                        f"could not update the queue entry for file "
                        f"{record.candidate_path}"
                    )

    return DuckLakeCleanupReport(
        candidates=candidates,
        referenced=(),
        deleted=deleted,
        failed=tuple(failed),
        dry_run=False,
        truncated=plan.truncated,
    )
