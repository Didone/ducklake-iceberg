r"""Delta computation.

Given two DuckLake snapshot numbers (the published ``S_prev``, recorded in
``ducklake.snapshot-id``, and the target ``S``), produces the set of data
files that **entered** and the set that **left** the table between the two,
without enumerating the whole table and without replaying the intermediate
snapshots.

The reference computation is the **set difference**:
``added = live(S) \ live(S_prev)`` and ``removed = live(S_prev) \ live(S)``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from iceduck.catalog.files import PathLayers, canonical, compose_path
from iceduck.errors import CompatibilityError, DivergenceError, ExitCode, IceduckError

if TYPE_CHECKING:
    import duckdb


class DeltaMode(StrEnum):
    """Delta computation mode."""

    FULL = "full"  # no S_prev, or --force-full
    INCREMENTAL = "incremental"
    NOOP = "noop"  # S_prev == S


@dataclass(frozen=True)
class DeltaFile:
    """A data file in the delta, with DuckLake catalog metadata."""

    data_file_id: int
    path: str  # composed absolute path
    raw_path: str  # ducklake_data_file.path, as stored
    path_is_relative: bool
    file_format: str  # 'parquet'
    record_count: int
    file_size_bytes: int
    footer_size: int | None
    row_group_count: int | None
    partition_id: int | None
    mapping_id: int | None
    partial_max: int | None
    begin_snapshot: int
    end_snapshot: int | None


@dataclass(frozen=True)
class DeltaCounters:
    """Aggregated delta counters."""

    added_files: int
    removed_files: int
    added_records: int
    removed_records: int
    added_bytes: int
    removed_bytes: int
    transient_files: int


@dataclass(frozen=True)
class TableDelta:
    """Full delta between two DuckLake snapshots."""

    table_id: int
    mode: DeltaMode
    previous_snapshot_id: int | None
    target_snapshot_id: int
    added: tuple[DeltaFile, ...]
    removed: tuple[DeltaFile, ...]
    transient: tuple[DeltaFile, ...]
    counters: DeltaCounters

    @property
    def is_empty(self) -> bool:
        """True if there are no added or removed files."""
        return not self.added and not self.removed


# -- exceptions ---------------------------------------------------------------


class DeltaError(IceduckError):
    """Delta computation error without a dedicated class (exit 1)."""


class CatalogRewoundError(DivergenceError):
    """State points to a future snapshot (S_prev > S). Exit 3."""

    exit_code = ExitCode.DIVERGED


class RetentionGapError(DivergenceError):
    """Previous snapshot expired or files scheduled for deletion. Exit 3."""

    exit_code = ExitCode.DIVERGED


class CrossCheckError(DivergenceError):
    """Cross-check between catalog and list_files or Iceberg failed. Exit 3."""

    exit_code = ExitCode.DIVERGED


class CompatibilityBlockerError(CompatibilityError):
    """Compatibility blocker found in the delta. Exit 2."""

    exit_code = ExitCode.BLOCKED


# -- path composition -------------------------------------------------


def compose_absolute_path(
    data_path: str,
    schema_path: str | None,
    schema_path_is_relative: bool,
    table_path: str | None,
    table_path_is_relative: bool,
    file_path: str,
    file_path_is_relative: bool,
) -> str:
    """Compose the absolute path of a file.

    Delegates to ``iceduck.catalog.files.compose_path``: the delta and the
    export must compose exactly the same path, otherwise the set difference
    sees "new" files that are the same.
    """
    layers = PathLayers(
        data_path=data_path,
        schema_path=schema_path,
        schema_path_is_relative=schema_path_is_relative,
        table_path=table_path,
        table_path_is_relative=table_path_is_relative,
    )
    return compose_path(layers, file_path, file_path_is_relative)


def canonical_path(path: str) -> str:
    """Normalize a path for comparison.

    Delegates to ``iceduck.catalog.files.canonical``: it resolves neither ``..``
    nor percent-encoding (in S3 the key is literal).
    """
    return canonical(path)


# -- SQL queries ------------------------------------------------------------


_FILE_COLUMNS = """
    data_file_id, path, path_is_relative, file_format,
    record_count, file_size_bytes, footer_size, row_group_count,
    partition_id, mapping_id, partial_max,
    begin_snapshot, end_snapshot
"""


def _rows(
    con: duckdb.DuckDBPyConnection, sql: str, params: list[object]
) -> list[dict[str, Any]]:
    result = con.execute(sql, params).fetchall()
    columns = [desc[0] for desc in con.description or ()]
    return [dict(zip(columns, row, strict=True)) for row in result]


def _query_added(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    table_id: int,
    prev: int,
    target: int,
) -> list[dict[str, Any]]:
    """Files added between prev and target."""
    sql = f"""
        SELECT {_FILE_COLUMNS}
        FROM {meta}.ducklake_data_file
        WHERE table_id = ?
          AND begin_snapshot >  ?
          AND begin_snapshot <= ?
          AND (end_snapshot IS NULL OR end_snapshot > ?)
        ORDER BY data_file_id
    """  # noqa: S608
    return _rows(con, sql, [table_id, prev, target, target])


def _query_removed(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    table_id: int,
    prev: int,
    target: int,
) -> list[dict[str, Any]]:
    """Files removed between prev and target."""
    sql = f"""
        SELECT {_FILE_COLUMNS}
        FROM {meta}.ducklake_data_file
        WHERE table_id = ?
          AND begin_snapshot <= ?
          AND end_snapshot >  ?
          AND end_snapshot <= ?
        ORDER BY data_file_id
    """  # noqa: S608
    return _rows(con, sql, [table_id, prev, prev, target])


def _query_transient(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    table_id: int,
    prev: int,
    target: int,
) -> list[dict[str, Any]]:
    """Transient files: created and removed within the interval."""
    sql = f"""
        SELECT {_FILE_COLUMNS}
        FROM {meta}.ducklake_data_file
        WHERE table_id = ?
          AND begin_snapshot >  ?
          AND begin_snapshot <= ?
          AND end_snapshot IS NOT NULL
          AND end_snapshot <= ?
        ORDER BY data_file_id
    """  # noqa: S608
    return _rows(con, sql, [table_id, prev, target, target])


def _query_live(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    table_id: int,
    snapshot: int,
) -> list[dict[str, Any]]:
    """Live set at a snapshot."""
    sql = f"""
        SELECT {_FILE_COLUMNS}
        FROM {meta}.ducklake_data_file
        WHERE table_id = ?
          AND begin_snapshot <= ?
          AND (end_snapshot IS NULL OR ? < end_snapshot)
        ORDER BY data_file_id
    """  # noqa: S608
    return _rows(con, sql, [table_id, snapshot, snapshot])


def _query_list_files(
    con: duckdb.DuckDBPyConnection,
    lake: str,
    schema_name: str,
    table_name: str,
    snapshot: int,
) -> list[str]:
    """Paths via ducklake_list_files (cross-check A)."""
    sql = """
        SELECT data_file
        FROM ducklake_list_files(?, ?, schema => ?, snapshot_version => ?)
        ORDER BY data_file
    """
    result = con.execute(sql, [lake, table_name, schema_name, snapshot]).fetchall()
    return [row[0] for row in result]


# -- guards (G1-G5) --------------------------------------------------------


def _guard_retention(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    prev: int | None,
) -> tuple[int | None, int]:
    """G1: is S_prev still within the retention window?"""
    sql = f"""
        SELECT min(snapshot_id) AS oldest_retained,
               max(snapshot_id) AS newest
        FROM {meta}.ducklake_snapshot
    """  # noqa: S608
    # fetchall: a pending result would block the next query on the connection
    rows = con.execute(sql).fetchall()
    oldest = rows[0][0] if rows else None
    newest = rows[0][1] if rows and rows[0][1] is not None else 0

    if prev is not None and oldest is not None and prev < oldest:
        raise RetentionGapError(f"snapshot {prev} expired (oldest retained: {oldest})")

    return oldest, newest


def _guard_scheduled_for_deletion(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    table_id: int,
) -> None:
    """G2: is any live file scheduled for deletion?"""
    sql = f"""
        SELECT d.data_file_id, d.path
        FROM {meta}.ducklake_files_scheduled_for_deletion d
        JOIN {meta}.ducklake_data_file f
          ON f.data_file_id = d.data_file_id
        WHERE f.table_id = ?
    """  # noqa: S608
    result = con.execute(sql, [table_id]).fetchall()
    if result:
        paths = [row[1] for row in result[:5]]
        raise RetentionGapError(
            f"{len(result)} live files are in "
            f"ducklake_files_scheduled_for_deletion: {paths}"
        )


def _guard_partial_files(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    table_id: int,
    target: int,
) -> None:
    """G3: partial file outside the exportable window."""
    sql = f"""
        SELECT data_file_id, path, partial_max
        FROM {meta}.ducklake_data_file
        WHERE table_id = ?
          AND partial_max IS NOT NULL
          AND partial_max > ?
          AND begin_snapshot <= ?
          AND (end_snapshot IS NULL OR ? < end_snapshot)
    """  # noqa: S608
    result = con.execute(sql, [table_id, target, target, target]).fetchall()
    if result:
        _file_id, path, partial_max = result[0]
        raise CompatibilityBlockerError(
            f"file {path} has partial_max={partial_max} > {target}; "
            f"export the current snapshot"
        )


def _guard_delete_files(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    table_id: int,
    target: int,
) -> None:
    """G4: delete files live at ``target`` in a non-convertible format.

    Parquet delete files are converted into Iceberg position deletes and do
    not block; only a format that v2 cannot read (``puffin``, deletion
    vectors) is a blocker.
    """
    sql = f"""
        SELECT delete_file_id, path, format
        FROM {meta}.ducklake_delete_file
        WHERE table_id = ?
          AND begin_snapshot <= ?
          AND (end_snapshot IS NULL OR ? < end_snapshot)
          AND lower(coalesce(format, 'parquet')) <> 'parquet'
        ORDER BY delete_file_id
    """  # noqa: S608
    result = con.execute(sql, [table_id, target, target]).fetchall()
    if result:
        _file_id, path, fmt = result[0]
        raise CompatibilityBlockerError(
            f"table has {len(result)} live delete file(s) in format "
            f"{fmt!r} (e.g. {path}); Iceberg v2 only reads Parquet position deletes"
        )


def _guard_file_format(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    table_id: int,
    prev: int | None,
    target: int,
) -> None:
    """G5: format and encryption of the files that enter.

    Incremental: those added in ``(prev, target]``; FULL (``prev`` is null):
    the whole live set. Only files live at ``target`` count - a transient
    file never reaches Iceberg.
    """
    sql = f"""
        SELECT data_file_id, file_format, encryption_key
        FROM {meta}.ducklake_data_file
        WHERE table_id = ?
          AND begin_snapshot >  ?
          AND begin_snapshot <= ?
          AND (end_snapshot IS NULL OR ? < end_snapshot)
          AND (file_format <> 'parquet' OR encryption_key IS NOT NULL)
        ORDER BY data_file_id
    """  # noqa: S608
    lower = -1 if prev is None else prev
    result = con.execute(sql, [table_id, lower, target, target]).fetchall()
    if result:
        file_id, fmt, enc_key = result[0]
        if fmt != "parquet":
            raise CompatibilityBlockerError(
                f"file {file_id}: format {fmt!r} not supported (parquet only)"
            )
        if enc_key:
            raise CompatibilityBlockerError(
                f"file {file_id}: encryption_key present (out of scope)"
            )


# -- cross-checks --------------------------------------------------


def _cross_check_list_files(
    con: duckdb.DuckDBPyConnection,
    lake: str,
    schema_name: str,
    table_name: str,
    snapshot: int,
    catalog_paths: frozenset[str],
) -> None:
    """Check (A): catalog vs ducklake_list_files."""
    list_files_paths = _query_list_files(con, lake, schema_name, table_name, snapshot)
    list_files_set = frozenset(canonical_path(p) for p in list_files_paths)

    if catalog_paths != list_files_set:
        missing_in_list = catalog_paths - list_files_set
        extra_in_list = list_files_set - catalog_paths
        raise CrossCheckError(
            f"ducklake_list_files returned {len(list_files_set)} paths, "
            f"catalog returned {len(catalog_paths)}; "
            f"missing from list_files: {len(missing_in_list)}, "
            f"extra in list_files: {len(extra_in_list)}"
        )


def _cross_check_reconstruction(
    iceberg_live: frozenset[str],
    added_paths: frozenset[str],
    removed_paths: frozenset[str],
    catalog_live: frozenset[str],
) -> None:
    """Check (B): state reconstruction."""
    # (iceberg_live - removed) U added == catalog_live
    reconstructed = (iceberg_live - removed_paths) | added_paths

    if reconstructed != catalog_live:
        missing = catalog_live - reconstructed
        extra = reconstructed - catalog_live
        raise CrossCheckError(
            f"reconstructed state differs from live(S) in {len(missing) + len(extra)} "
            f"paths; missing: {len(missing)}, extra: {len(extra)}"
        )


# -- DeltaFile construction ------------------------------------------------


def _build_delta_files(
    rows: list[dict[str, Any]],
    data_path: str,
    schema_path: str | None,
    schema_path_is_relative: bool,
    table_path: str | None,
    table_path_is_relative: bool,
) -> tuple[DeltaFile, ...]:
    """Build the DeltaFile list from catalog rows."""
    files = []
    for row in rows:
        abs_path = compose_absolute_path(
            data_path,
            schema_path,
            schema_path_is_relative,
            table_path,
            table_path_is_relative,
            row["path"],
            row["path_is_relative"],
        )
        files.append(
            DeltaFile(
                data_file_id=row["data_file_id"],
                path=abs_path,
                raw_path=row["path"],
                path_is_relative=row["path_is_relative"],
                file_format=row["file_format"],
                record_count=row["record_count"],
                file_size_bytes=row["file_size_bytes"],
                footer_size=row.get("footer_size"),
                row_group_count=row.get("row_group_count"),
                partition_id=row.get("partition_id"),
                mapping_id=row.get("mapping_id"),
                partial_max=row.get("partial_max"),
                begin_snapshot=row["begin_snapshot"],
                end_snapshot=row.get("end_snapshot"),
            )
        )
    return tuple(files)


# -- main function -------------------------------------------------------


def compute_delta(
    con: duckdb.DuckDBPyConnection,
    *,
    meta: str,
    lake: str,
    schema_name: str,
    table_name: str,
    table_id: int,
    previous_snapshot_id: int | None,
    target_snapshot_id: int,
    data_path: str,
    schema_path: str | None = None,
    schema_path_is_relative: bool = True,
    table_path: str | None = None,
    table_path_is_relative: bool = True,
    iceberg_live_paths: frozenset[str] | None = None,
) -> TableDelta:
    """Compute the delta between two DuckLake snapshots.

    Args:
        con: DuckDB connection in a read transaction.
        meta: Name of the METADATA_CATALOG (e.g. 'lake_meta').
        lake: Alias of the ducklake ATTACH (e.g. 'lake').
        schema_name: DuckLake schema name.
        table_name: DuckLake table name.
        table_id: Table ID in the catalog.
        previous_snapshot_id: Previously published snapshot (None = first sync).
        target_snapshot_id: Target snapshot.
        data_path: Base path of the data (ducklake_metadata).
        schema_path: Schema path, or None.
        schema_path_is_relative: Whether schema_path is relative.
        table_path: Table path, or None.
        table_path_is_relative: Whether table_path is relative.
        iceberg_live_paths: Live paths in the Iceberg table (for check B).

    Returns:
        TableDelta with added, removed, transient and counters.

    Raises:
        CatalogRewoundError: S_prev > S (exit 3).
        RetentionGapError: S_prev expired or files scheduled for deletion (exit 3).
        CrossCheckError: Cross-check failed (exit 3).
        CompatibilityBlockerError: Compatibility blocker (exit 2).
    """
    # Decide the mode
    if previous_snapshot_id is None:
        mode = DeltaMode.FULL
    elif previous_snapshot_id > target_snapshot_id:
        raise CatalogRewoundError(
            f"state points to snapshot {previous_snapshot_id}, "
            f"catalog is at {target_snapshot_id}"
        )
    elif previous_snapshot_id == target_snapshot_id:
        # NOOP: nothing to do
        return TableDelta(
            table_id=table_id,
            mode=DeltaMode.NOOP,
            previous_snapshot_id=previous_snapshot_id,
            target_snapshot_id=target_snapshot_id,
            added=(),
            removed=(),
            transient=(),
            counters=DeltaCounters(0, 0, 0, 0, 0, 0, 0),
        )
    else:
        mode = DeltaMode.INCREMENTAL

    # G1 - retention
    _guard_retention(con, meta, previous_snapshot_id)

    # Compute the sets
    prev = previous_snapshot_id
    if prev is None:
        added_rows = _query_live(con, meta, table_id, target_snapshot_id)
        removed_rows: list[dict[str, Any]] = []
        transient_rows: list[dict[str, Any]] = []
    else:
        added_rows = _query_added(con, meta, table_id, prev, target_snapshot_id)
        removed_rows = _query_removed(con, meta, table_id, prev, target_snapshot_id)
        transient_rows = _query_transient(con, meta, table_id, prev, target_snapshot_id)

    # G2-G5 (G5 also applies to FULL: the whole live set enters)
    _guard_scheduled_for_deletion(con, meta, table_id)
    _guard_partial_files(con, meta, table_id, target_snapshot_id)
    _guard_delete_files(con, meta, table_id, target_snapshot_id)
    _guard_file_format(con, meta, table_id, prev, target_snapshot_id)

    # Compose absolute paths (the same composition as the export)
    def build(rows: list[dict[str, Any]]) -> tuple[DeltaFile, ...]:
        return _build_delta_files(
            rows,
            data_path,
            schema_path,
            schema_path_is_relative,
            table_path,
            table_path_is_relative,
        )

    added = build(added_rows)
    removed = build(removed_rows)
    transient = build(transient_rows)  # observability only

    # Cross-check (A) against ducklake_list_files
    live_files = (
        added
        if prev is None
        else build(_query_live(con, meta, table_id, target_snapshot_id))
    )
    catalog_live_paths = frozenset(canonical_path(f.path) for f in live_files)
    _cross_check_list_files(
        con, lake, schema_name, table_name, target_snapshot_id, catalog_live_paths
    )

    # Check (B) if there is an Iceberg table
    if iceberg_live_paths is not None:
        _cross_check_reconstruction(
            frozenset(canonical_path(p) for p in iceberg_live_paths),
            frozenset(canonical_path(f.path) for f in added),
            frozenset(canonical_path(f.path) for f in removed),
            catalog_live_paths,
        )

    # Validate counts
    for f in added:
        # record_count == 0 is accepted; size == 0 never is
        if f.file_size_bytes <= 0:
            raise DeltaError(
                f"file {f.path} has file_size_bytes={f.file_size_bytes} <= 0"
            )

    # Counters
    counters = DeltaCounters(
        added_files=len(added),
        removed_files=len(removed),
        added_records=sum(f.record_count for f in added),
        removed_records=sum(f.record_count for f in removed),
        added_bytes=sum(f.file_size_bytes for f in added),
        removed_bytes=sum(f.file_size_bytes for f in removed),
        transient_files=len(transient),
    )

    return TableDelta(
        table_id=table_id,
        mode=mode,
        previous_snapshot_id=previous_snapshot_id,
        target_snapshot_id=target_snapshot_id,
        added=added,
        removed=removed,
        transient=transient,
        counters=counters,
    )
