"""Report on the snapshots created by a checkpoint.

Determines the DuckLake snapshot that the export publishes as an Iceberg
snapshot. `export_target_snapshot` is a commitment -- the current snapshot read
after the checkpoint, always >= the last one it created -- and is always
correct. Attributing which snapshot came from the flush and which from
maintenance is **best-effort**, based on tokens in `ducklake_snapshot_changes`:
it may be impossible under concurrent writes, but the target is still
determined.

Verified facts this module assumes: the checkpoint creates up to two
snapshots; `changes_made` records `inline_flush:<id>`, `rewrite_delete:<id>`,
`merge_adjacent:<id>`; merge **deletes** the rows of the merged files from
`ducklake_data_file`, so retired files are read from two sources (the
`end_snapshot` in the window and `ducklake_files_scheduled_for_deletion` by
`schedule_start`).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, Literal

from ducklake_iceberg.catalog.connection import DuckLakeConnection
from ducklake_iceberg.checkpoint.model import TableRef
from ducklake_iceberg.doctor.model import quote_ident

Attribution = Literal["exact", "ambiguous", "absent", "no_op"]
ChangeKind = Literal["inline_flush", "rewrite_delete", "merge_adjacent"]
OperationHint = Literal["append", "replace", "overwrite", "none"]

_FLUSH_TOKENS: Final[frozenset[str]] = frozenset({"inline_flush"})
_COMPACTION_TOKENS: Final[frozenset[str]] = frozenset(
    {"rewrite_delete", "merge_adjacent"}
)
_CHANGE_RE: Final[re.Pattern[str]] = re.compile(r"([a-z_]+):(\d+)")


@dataclass(frozen=True, slots=True)
class FileRecord:
    kind: Literal["data", "delete"]
    file_id: int
    path: str
    path_is_relative: bool
    record_count: int | None  # None for files merged by merge
    file_size_bytes: int | None
    partial_max: int | None
    begin_snapshot: int | None
    end_snapshot: int | None

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "file_id": self.file_id,
            "path": self.path,
            "record_count": self.record_count,
            "begin_snapshot": self.begin_snapshot,
            "end_snapshot": self.end_snapshot,
        }


@dataclass(frozen=True, slots=True)
class SnapshotRow:
    snapshot_id: int
    snapshot_time: datetime | None
    schema_version: int
    changes: tuple[tuple[str, int | None], ...]

    def to_json(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "schema_version": self.schema_version,
            "changes": [[kind, tid] for kind, tid in self.changes],
        }


@dataclass(frozen=True, slots=True)
class CheckpointSnapshotReport:
    version: Literal[1]
    table: TableRef
    snapshot_before: int
    export_target_snapshot: int
    flush_snapshot_id: int | None
    compaction_snapshot_id: int | None
    attribution: Attribution
    window: tuple[SnapshotRow, ...]
    foreign_snapshots: tuple[int, ...]
    files_created: tuple[FileRecord, ...]
    files_retired: tuple[FileRecord, ...]
    schema_version_changed: bool
    warnings: tuple[str, ...] = ()

    @property
    def compacted(self) -> bool:
        return self.compaction_snapshot_id is not None

    @property
    def iceberg_operation_hint(self) -> OperationHint:
        """Iceberg operation suggestion: empty window => none; no ended files
        => append; additions and removals => replace/overwrite."""
        if not self.files_created and not self.files_retired:
            return "none"
        if not self.files_retired:
            return "append"
        if not self.files_created:
            return "replace"
        # Additions and removals: `replace` when the row set is the same
        # (pure compaction, no flush), otherwise `overwrite`.
        return "replace" if self.flush_snapshot_id is None else "overwrite"

    def to_json(self) -> dict[str, Any]:
        return {
            "snapshot_before": self.snapshot_before,
            "export_target_snapshot": self.export_target_snapshot,
            "flush_snapshot_id": self.flush_snapshot_id,
            "compaction_snapshot_id": self.compaction_snapshot_id,
            "attribution": self.attribution,
            "iceberg_operation_hint": self.iceberg_operation_hint,
            "schema_version_changed": self.schema_version_changed,
            "foreign_snapshots": list(self.foreign_snapshots),
            "window": [s.to_json() for s in self.window],
            "files_created": [f.to_json() for f in self.files_created],
            "files_retired": [f.to_json() for f in self.files_retired],
            "warnings": list(self.warnings),
        }


def _parse_changes(changes_made: str | None) -> tuple[tuple[str, int | None], ...]:
    if not changes_made:
        return ()
    return tuple(
        (m.group(1), int(m.group(2))) for m in _CHANGE_RE.finditer(changes_made)
    )


def _window(
    con: DuckLakeConnection, snapshot_before: int, target: int
) -> tuple[SnapshotRow, ...]:
    meta = quote_ident(con.meta)
    rows = con.query_arrow(
        f"SELECT s.snapshot_id, CAST(s.snapshot_time AS TIMESTAMPTZ) AS snapshot_time, "
        f"       s.schema_version, c.changes_made "
        f"FROM {meta}.ducklake_snapshot AS s "
        f"LEFT JOIN {meta}.ducklake_snapshot_changes AS c "
        f"  ON c.snapshot_id = s.snapshot_id "
        f"WHERE s.snapshot_id > $before AND s.snapshot_id <= $target "
        f"ORDER BY s.snapshot_id",
        {"before": snapshot_before, "target": target},
    ).to_pylist()
    return tuple(
        SnapshotRow(
            snapshot_id=int(r["snapshot_id"]),
            snapshot_time=r["snapshot_time"]
            if isinstance(r["snapshot_time"], datetime)
            else None,
            schema_version=int(r["schema_version"]),
            changes=_parse_changes(
                str(r["changes_made"]) if r["changes_made"] is not None else None
            ),
        )
        for r in rows
    )


def _attribute(
    window: tuple[SnapshotRow, ...], table_id: int
) -> tuple[int | None, int | None, Attribution, tuple[int, ...]]:
    """(`flush_snapshot_id`, `compaction_snapshot_id`, attribution, foreign)."""
    if not window:
        return None, None, "no_op", ()

    def _candidates(kinds: frozenset[str]) -> list[int]:
        return [
            s.snapshot_id
            for s in window
            if any(k in kinds and tid == table_id for k, tid in s.changes)
        ]

    flush = _candidates(_FLUSH_TOKENS)
    compaction = _candidates(_COMPACTION_TOKENS)
    ours = set(flush) | set(compaction)
    foreign = tuple(s.snapshot_id for s in window if s.snapshot_id not in ours)

    if not flush and not compaction:
        return None, None, "absent", foreign
    attribution: Attribution = (
        "ambiguous" if len(flush) > 1 or len(compaction) > 1 else "exact"
    )
    flush_id = max(flush) if flush else None
    compaction_id = max(compaction) if compaction else None
    return flush_id, compaction_id, attribution, foreign


def _created(
    con: DuckLakeConnection, table_id: int, before: int, target: int
) -> list[FileRecord]:
    meta = quote_ident(con.meta)
    out: list[FileRecord] = []
    for kind, tbl, idc, cnt in (
        ("data", "ducklake_data_file", "data_file_id", "record_count"),
        ("delete", "ducklake_delete_file", "delete_file_id", "delete_count"),
    ):
        rows = con.query_arrow(
            f"SELECT {idc} AS file_id, path, path_is_relative, {cnt} AS record_count, "
            f"       file_size_bytes, partial_max, begin_snapshot, end_snapshot "
            f"FROM {meta}.{tbl} "
            f"WHERE table_id = $table_id AND begin_snapshot > $before "
            f"  AND $target >= begin_snapshot "
            f"  AND (end_snapshot IS NULL OR $target < end_snapshot) "
            f"ORDER BY {idc}",
            {"table_id": table_id, "before": before, "target": target},
        ).to_pylist()
        out.extend(_record(kind, r) for r in rows)
    return out


def _retired_by_end_snapshot(
    con: DuckLakeConnection, table_id: int, before: int, target: int
) -> list[FileRecord]:
    meta = quote_ident(con.meta)
    out: list[FileRecord] = []
    for kind, tbl, idc, cnt in (
        ("data", "ducklake_data_file", "data_file_id", "record_count"),
        ("delete", "ducklake_delete_file", "delete_file_id", "delete_count"),
    ):
        rows = con.query_arrow(
            f"SELECT {idc} AS file_id, path, path_is_relative, {cnt} AS record_count, "
            f"       file_size_bytes, partial_max, begin_snapshot, end_snapshot "
            f"FROM {meta}.{tbl} "
            f"WHERE table_id = $table_id AND end_snapshot > $before "
            f"  AND end_snapshot <= $target "
            f"ORDER BY {idc}",
            {"table_id": table_id, "before": before, "target": target},
        ).to_pylist()
        out.extend(_record(kind, r) for r in rows)
    return out


def _retired_by_schedule(
    con: DuckLakeConnection, table: TableRef, before_time: datetime | None
) -> list[FileRecord]:
    """Files retired by merge: row deleted from `ducklake_data_file`, present
    only in `ducklake_files_scheduled_for_deletion`.

    That table has no `table_id`; it filters by the table's path prefix
    (`<schema>/<table>/`). `schedule_start` is the scheduling time.
    """
    if before_time is None:
        return []
    meta = quote_ident(con.meta)
    prefix = f"{table.schema_name}/{table.table_name}/"
    rows = con.query_arrow(
        f"SELECT data_file_id AS file_id, path, path_is_relative "
        f"FROM {meta}.ducklake_files_scheduled_for_deletion "
        f"WHERE CAST(schedule_start AS TIMESTAMPTZ) >= $before_time "
        f"  AND path LIKE $prefix "
        f"ORDER BY data_file_id",
        {"before_time": before_time, "prefix": f"%{prefix}%"},
    ).to_pylist()
    return [
        FileRecord(
            kind="data",
            file_id=int(r["file_id"]),
            path=str(r["path"]),
            path_is_relative=bool(r["path_is_relative"]),
            record_count=None,
            file_size_bytes=None,
            partial_max=None,
            begin_snapshot=None,
            end_snapshot=None,
        )
        for r in rows
    ]


def _record(kind: str, r: dict[str, Any]) -> FileRecord:
    return FileRecord(
        kind=kind,  # type: ignore[arg-type]
        file_id=int(r["file_id"]),
        path=str(r["path"]),
        path_is_relative=bool(r["path_is_relative"]),
        record_count=int(r["record_count"]) if r["record_count"] is not None else None,
        file_size_bytes=(
            int(r["file_size_bytes"]) if r["file_size_bytes"] is not None else None
        ),
        partial_max=int(r["partial_max"]) if r["partial_max"] is not None else None,
        begin_snapshot=(
            int(r["begin_snapshot"]) if r["begin_snapshot"] is not None else None
        ),
        end_snapshot=int(r["end_snapshot"]) if r["end_snapshot"] is not None else None,
    )


def build_snapshot_report(
    con: DuckLakeConnection,
    table: TableRef,
    *,
    snapshot_before: int,
    snapshot_before_time: datetime | None = None,
    schema_version_before: int | None = None,
) -> CheckpointSnapshotReport:
    """Read the post-checkpoint state in a single read transaction."""
    meta = quote_ident(con.meta)
    head = con.query_arrow(
        f"SELECT snapshot_id, schema_version FROM {meta}.ducklake_snapshot "
        "ORDER BY snapshot_id DESC LIMIT 1"
    ).to_pylist()
    target = int(head[0]["snapshot_id"])
    target_schema_version = int(head[0]["schema_version"])

    window = _window(con, snapshot_before, target)
    flush_id, compaction_id, attribution, foreign = _attribute(window, table.table_id)

    files_created = tuple(_created(con, table.table_id, snapshot_before, target))
    retired = _retired_by_end_snapshot(con, table.table_id, snapshot_before, target)
    retired_paths = {f.path for f in retired}
    retired.extend(
        f
        for f in _retired_by_schedule(con, table, snapshot_before_time)
        if f.path not in retired_paths
    )

    warnings: list[str] = []
    if attribution in ("ambiguous", "absent") and window:
        warnings.append(
            f"could not attribute the checkpoint snapshots "
            f"({len(window)} in the window)"
        )

    schema_changed = (
        schema_version_before is not None
        and schema_version_before != target_schema_version
    )
    if schema_changed:
        warnings.append(
            f"schema_version changed from {schema_version_before} to "
            f"{target_schema_version} during the checkpoint"
        )

    return CheckpointSnapshotReport(
        version=1,
        table=table,
        snapshot_before=snapshot_before,
        export_target_snapshot=target,
        flush_snapshot_id=flush_id,
        compaction_snapshot_id=compaction_id,
        attribution=attribution,
        window=window,
        foreign_snapshots=foreign,
        files_created=files_created,
        files_retired=tuple(retired),
        schema_version_changed=schema_changed,
        warnings=tuple(warnings),
    )
