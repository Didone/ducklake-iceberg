"""What the export reads from DuckLake, in one read transaction.

``read_catalog`` resolves the snapshot to publish (the latest one without
pending inlined data), refuses to lose pending inlined rows, enumerates the
live data and delete files and maps the schema and partition specs with the
DuckLake IDs. ``check_initial_defaults`` blocks a column added with a
``DEFAULT`` that older data files do not contain (Iceberg v2 would read NULL).
"""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Final

from pyiceberg.schema import Schema

from ducklake_iceberg import timing
from ducklake_iceberg.catalog.files import (
    FileSet,
    enumerate_files,
)
from ducklake_iceberg.catalog.reader import (
    TableIdentity,
    current_snapshot,
    parse_table_ref,
    resolve_table,
    snapshot_at,
)
from ducklake_iceberg.checkpoint.detect import (
    cache_inlined_reads,
    clean_snapshot,
    detect_inlined,
)
from ducklake_iceberg.checkpoint.model import TableRef as CheckpointTableRef
from ducklake_iceberg.concurrency import (
    ContextThreadPoolExecutor,
    footer_workers,
)
from ducklake_iceberg.doctor.model import DoctorTarget, quote_ident
from ducklake_iceberg.errors import (
    CompatibilityError,
)
from ducklake_iceberg.mapping.partition import (
    SpecMapping,
    build_spec_mapping,
    read_partition_specs,
    read_partition_values,
)
from ducklake_iceberg.mapping.schema import (
    DuckLakeColumn,
    SchemaMapping,
    build_iceberg_schema,
    read_columns,
    read_last_column_id,
)
from ducklake_iceberg.writer.catalog_stats import ColumnStats, read_file_column_stats
from ducklake_iceberg.writer.stats import file_field_ids

if TYPE_CHECKING:
    from pyiceberg.io import FileIO

    from ducklake_iceberg.catalog.connection import DuckLakeConnection


#: ``kind:table_id`` tokens of ``ducklake_snapshot_changes.changes_made``
_CHANGE_RE: Final[re.Pattern[str]] = re.compile(r"([a-z_]+):(\d+)")


# -- reading DuckLake (step 2) -------------------------------------------------


@dataclass(frozen=True, slots=True)
class CatalogView:
    identity: TableIdentity
    snapshot_id: int
    snapshot_time: datetime
    files: FileSet
    schema_mapping: SchemaMapping
    spec_mapping: SpecMapping
    values_by_file: dict[int, dict[int, str | None]]
    table_changes: tuple[str, ...]  # tokens of the (S_previous, S] window
    #: column_id with a non-null ``initial_default`` -> snapshot that added it
    defaults_added_at: Mapping[int, int] = dataclasses.field(default_factory=dict)
    #: ``S_previous`` of the ``table_changes`` window (None = no window)
    table_changes_since: int | None = None
    #: the window has all snapshots (none expired in between): only then does
    #: the absence of a token prove the table did not change
    table_changes_complete: bool = False
    historical_schemas: tuple[Schema, ...] = ()
    file_column_stats: Mapping[int, Mapping[int, ColumnStats]] = dataclasses.field(
        default_factory=dict
    )


def read_catalog(
    con: DuckLakeConnection,
    ref_text: str,
    previous_snapshot: int | None,
    *,
    include_stats: bool = False,
) -> CatalogView:
    with cache_inlined_reads(con.raw):
        view = _read_catalog(con, ref_text, previous_snapshot)
        if include_stats:
            view = dataclasses.replace(
                view,
                file_column_stats=read_file_column_stats(
                    con,
                    view.identity.table_id,
                    (f.data_file_id for f in view.files.files),
                ),
            )
        return view


def _read_catalog(
    con: DuckLakeConnection, ref_text: str, previous_snapshot: int | None
) -> CatalogView:
    ref = parse_table_ref(ref_text)
    with timing.phase("resolve"):
        snapshot = current_snapshot(con)
        identity = resolve_table(con, ref, snapshot)

        # Target: the most recent snapshot without pending inlined data. An
        # inlined commit that lands between the flush and the export is not
        # lost: it is left for the next export. Never below what is already
        # published; if needed, the guard below blocks.
        target_id = clean_snapshot(
            con.raw,
            con.meta,
            CheckpointTableRef(
                identity.ref.schema_name, identity.ref.table_name, identity.table_id
            ),
            snapshot.snapshot_id,
        )
        if previous_snapshot is not None:
            target_id = max(target_id, previous_snapshot)
        if target_id != snapshot.snapshot_id:
            snapshot = snapshot_at(con, target_id)
            identity = resolve_table(con, ref, snapshot)

    target = DoctorTarget(
        catalog=con.alias,
        metadata_catalog=con.meta,
        schema_name=identity.ref.schema_name,
        table_name=identity.ref.table_name,
        table_id=identity.table_id,
        snapshot_id=snapshot.snapshot_id,
        schema_version=identity.schema_version,
    )

    # Inlined guard: with files already existing, the enumeration does not see
    # the pending inlined rows - the export would silently lose them.
    inlined = detect_inlined(
        con.raw,
        con.meta,
        CheckpointTableRef(
            identity.ref.schema_name, identity.ref.table_name, identity.table_id
        ),
        snapshot.snapshot_id,
    )
    if inlined.has_pending:
        raise CompatibilityError(
            f"{target.qualified_name} has {inlined.pending_data_rows} row(s) and "
            f"{inlined.pending_delete_rows} deletion(s) of pending inlined data; "
            f"the export would lose them",
            hint=(f"ducklake-iceberg sync --table {target.qualified_name}"),
        )

    file_set = enumerate_files(con.raw, target, allow_historic=True)
    columns = read_columns(con.raw, con.meta, identity.table_id, snapshot.snapshot_id)
    schema_mapping = build_iceberg_schema(
        columns,
        schema_id=identity.schema_version,
        last_column_id=read_last_column_id(con.raw, con.meta, identity.table_id),
    )
    partition_infos = read_partition_specs(con.raw, con.meta, identity.table_id)
    historical: list[Schema] = []
    for info in partition_infos:
        if all(c.column_id in schema_mapping.schema.field_ids for c in info.columns):
            continue
        old_columns = read_columns(
            con.raw, con.meta, identity.table_id, info.begin_snapshot
        )
        historical.append(
            build_iceberg_schema(
                old_columns,
                schema_id=identity.schema_version + len(historical) + 1,
                last_column_id=schema_mapping.last_column_id,
            ).schema
        )
    spec_mapping = build_spec_mapping(
        partition_infos,
        schema_mapping.schema,
        snapshot_id=snapshot.snapshot_id,
        historical_schemas=historical,
    )
    values_by_file = read_partition_values(con.raw, con.meta, identity.table_id)
    defaults_added_at = _defaults_added_at(
        con, identity.table_id, schema_mapping.columns_by_id
    )
    changes: tuple[str, ...] = ()
    complete = False
    if previous_snapshot is not None:
        changes = _table_changes(
            con, identity.table_id, previous_snapshot, snapshot.snapshot_id
        )
        complete = _window_complete(con, previous_snapshot, snapshot.snapshot_id)
    return CatalogView(
        identity=identity,
        snapshot_id=snapshot.snapshot_id,
        snapshot_time=snapshot.snapshot_time,
        files=file_set,
        schema_mapping=schema_mapping,
        spec_mapping=spec_mapping,
        values_by_file=values_by_file,
        table_changes=changes,
        defaults_added_at=defaults_added_at,
        table_changes_since=previous_snapshot,
        table_changes_complete=complete,
        historical_schemas=tuple(historical),
    )


def _window_complete(con: DuckLakeConnection, previous: int, target: int) -> bool:
    """All snapshots in ``(prev, S]`` exist, with their change records.

    DuckLake's ``snapshot_id`` is sequential; ``ducklake_expire_snapshots``
    deletes snapshots (and their ``ducklake_snapshot_changes`` rows). A window
    with a gap may have lost precisely this table's token.
    """
    meta = quote_ident(con.meta)
    row = con.query_arrow(
        f"SELECT (SELECT count(*) FROM {meta}.ducklake_snapshot "
        f"WHERE snapshot_id > $prev AND snapshot_id <= $target) AS snapshots, "
        f"(SELECT count(*) FROM {meta}.ducklake_snapshot_changes "
        f"WHERE snapshot_id > $prev AND snapshot_id <= $target) AS changes",
        {"prev": previous, "target": target},
    ).to_pylist()[0]
    expected = target - previous
    return int(row["snapshots"]) == expected and int(row["changes"]) == expected


def _table_changes(
    con: DuckLakeConnection, table_id: int, previous: int, target: int
) -> tuple[str, ...]:
    """Change kinds recorded for the table in the ``(prev, S]`` window."""
    rows = con.query_arrow(
        f"SELECT changes_made FROM {quote_ident(con.meta)}.ducklake_snapshot_changes "
        f"WHERE snapshot_id > $prev AND snapshot_id <= $target",
        {"prev": previous, "target": target},
    ).to_pylist()
    kinds: list[str] = []
    for row in rows:
        for kind, ident in _CHANGE_RE.findall(str(row["changes_made"] or "")):
            if int(ident) == table_id:
                kinds.append(kind)
    return tuple(kinds)


# -- initial default ---------------------------------------------------------


def _has_initial_default(col: DuckLakeColumn) -> bool:
    raw = col.initial_default
    return raw is not None and raw.strip().upper() != "NULL"


def _defaults_added_at(
    con: DuckLakeConnection, table_id: int, columns: Mapping[int, DuckLakeColumn]
) -> dict[int, int]:
    """Snapshot in which each column with an ``initial_default`` was added.

    It is the smallest ``begin_snapshot`` of the ``column_id``: ``SET DEFAULT``
    opens a new row in ``ducklake_column`` and does not change the
    ``initial_default``.
    """
    wanted = sorted(cid for cid, col in columns.items() if _has_initial_default(col))
    if not wanted:
        return {}
    rows = con.query_arrow(
        f"SELECT column_id, min(begin_snapshot) AS added_at "
        f"FROM {quote_ident(con.meta)}.ducklake_column "
        f"WHERE table_id = $table_id GROUP BY column_id",
        {"table_id": table_id},
    ).to_pylist()
    added = {int(r["column_id"]): int(r["added_at"]) for r in rows}
    return {cid: added[cid] for cid in wanted if cid in added}


def check_initial_defaults(view: CatalogView, io: FileIO) -> None:
    """Block a live data file that lacks a column with an ``initial_default`` (exit 2).

    In DuckLake the rows of a file written before ``ADD COLUMN ... DEFAULT x``
    read ``x``; Iceberg v2 has no ``initial-default`` and every reader (Athena
    included) reads NULL - a silent divergence. Only the footer tells whether
    the file has the column: the checkpoint compaction may have rewritten it
    with the materialized value while keeping the original ``begin_snapshot``.
    The candidates are the files that predate the column; in the common case
    (no column with a default, or no file older than it) no footer is read.
    """
    if not view.defaults_added_at:
        return
    candidates = [
        f
        for f in view.files.files
        if any(f.begin_snapshot < s for s in view.defaults_added_at.values())
    ]
    if not candidates:
        return
    with ContextThreadPoolExecutor(max_workers=footer_workers(len(candidates))) as pool:
        present = list(pool.map(lambda f: file_field_ids(f.path, io), candidates))
    missing: dict[int, list[str]] = {}
    for f, ids in zip(candidates, present, strict=True):
        for cid, added_at in view.defaults_added_at.items():
            if f.begin_snapshot < added_at and cid not in ids:
                missing.setdefault(cid, []).append(f.path)
    if not missing:
        return
    paths = view.schema_mapping.paths_by_id
    columns = ", ".join(
        f"{paths.get(cid, str(cid))!r} (DEFAULT "
        f"{view.schema_mapping.columns_by_id[cid].initial_default}, "
        f"{len(files)} file(s))"
        for cid, files in sorted(missing.items())
    )
    # the UPDATE rewrites the top-level column (the whole struct for a nested field)
    roots = dict.fromkeys(paths.get(cid, str(cid)).split(".")[0] for cid in missing)
    quoted = ['"' + r.replace('"', '""') + '"' for r in roots]
    assignments = ", ".join(f"{q} = {q}" for q in quoted)
    table = view.identity.ref
    raise CompatibilityError(
        f"{table.schema_name}.{table.table_name}: column(s) added with DEFAULT "
        f"have no value in the earlier data files: {columns}; DuckLake reads the "
        f"default for those rows, Iceberg v2 (no initial-default) would read NULL",
        hint=(
            f"rewrite the old rows in DuckLake, e.g. UPDATE "
            f"{table.schema_name}.{table.table_name} SET {assignments} "
            f"(violates zero-copy), and run sync again"
        ),
    )
