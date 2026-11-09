"""File size distribution per partition, and closed partitions.

Reads catalog metadata only (`ducklake_data_file`, `ducklake_delete_file`,
`ducklake_file_partition_value`, `ducklake_snapshot`,
`ducklake_snapshot_changes`), without opening any Parquet. It serves the
checkpoint report (after compaction) and `--dry-run` (current state).

**Closed partition**: a partition whose last data write recorded in the
catalog is older than `closed_after_seconds`, relative to the clock of whoever
runs the command. The write time of a data file is
`coalesce(partial_max, begin_snapshot)` (merge keeps the smallest
`begin_snapshot` and stores the largest in `partial_max`; flush keeps the
snapshot of the inlined insert); for a delete file it is its `begin_snapshot`.
Live files and ended files that expire has not removed yet both count.
Compaction-only snapshots (`merge_adjacent`/`rewrite_delete` of the table) are
not writes: DuckLake does not commit compaction together with a change.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Final

from ducklake_iceberg.catalog.connection import DuckLakeConnection
from ducklake_iceberg.checkpoint.model import (
    FileSizeDistribution,
    PartitionFileSizes,
    PartitionSizeReport,
    TableRef,
)
from ducklake_iceberg.doctor.model import quote_ident

#: Default window: a partition with no write for 24 h is closed (daily cadence).
DEFAULT_CLOSED_AFTER_SECONDS: Final[int] = 86_400

_COMPACTION_TOKENS: Final[frozenset[str]] = frozenset(
    {"merge_adjacent", "rewrite_delete"}
)
_CHANGE_RE: Final[re.Pattern[str]] = re.compile(r"([a-z_]+):(\d+)")

PartitionKey = tuple[int | None, tuple[str | None, ...]]


def file_size_distribution(
    con: DuckLakeConnection,
    table: TableRef,
    snapshot_id: int,
    target_file_size_bytes: int | None,
) -> FileSizeDistribution:
    """Distribution of the live data files of the whole table in the snapshot."""
    rows = con.query_arrow(
        f"SELECT file_size_bytes FROM {quote_ident(con.meta)}.ducklake_data_file "
        "WHERE table_id = $table_id "
        "  AND $snapshot_id >= begin_snapshot "
        "  AND (end_snapshot IS NULL OR $snapshot_id < end_snapshot) "
        "  AND file_size_bytes IS NOT NULL",
        {"table_id": table.table_id, "snapshot_id": snapshot_id},
    ).to_pylist()
    return FileSizeDistribution.from_sizes(
        (int(r["file_size_bytes"]) for r in rows), target_file_size_bytes
    )


def _compaction_snapshots(changes: Mapping[int, str | None], table_id: int) -> set[int]:
    """Snapshots whose changes to this table are compaction only."""
    out: set[int] = set()
    for snapshot_id, made in changes.items():
        kinds = {
            m.group(1)
            for m in _CHANGE_RE.finditer(made or "")
            if int(m.group(2)) == table_id
        }
        if kinds and kinds <= _COMPACTION_TOKENS:
            out.add(snapshot_id)
    return out


def _fields(con: DuckLakeConnection, table: TableRef) -> dict[int, list[str]]:
    """`partition_id` -> `transform(column)` labels in key order."""
    meta = quote_ident(con.meta)
    rows = con.query_arrow(
        f"SELECT pc.partition_id, pc.partition_key_index, pc.transform, "
        f"       c.column_name "
        f"FROM {meta}.ducklake_partition_column AS pc "
        f"LEFT JOIN {meta}.ducklake_column AS c "
        f"  ON c.table_id = pc.table_id AND c.column_id = pc.column_id "
        f" AND c.end_snapshot IS NULL "
        f"WHERE pc.table_id = $table_id "
        f"ORDER BY pc.partition_id, pc.partition_key_index",
        {"table_id": table.table_id},
    ).to_pylist()
    out: dict[int, list[str]] = defaultdict(list)
    for r in rows:
        column = str(r["column_name"]) if r["column_name"] is not None else "?"
        transform = str(r["transform"] or "identity")
        out[int(r["partition_id"])].append(
            column if transform == "identity" else f"{transform}({column})"
        )
    return out


def _label(values: tuple[str | None, ...], fields: list[str] | None) -> str:
    if not values:
        return "(no partition)"
    shown = ["NULL" if v is None else v for v in values]
    if fields and len(fields) == len(shown):
        return ", ".join(f"{f}={v}" for f, v in zip(fields, shown, strict=True))
    return "/".join(shown)


def partition_size_report(
    con: DuckLakeConnection,
    table: TableRef,
    snapshot_id: int,
    target_file_size_bytes: int | None,
    closed_after_seconds: int = DEFAULT_CLOSED_AFTER_SECONDS,
    *,
    now: datetime | None = None,
) -> PartitionSizeReport:
    """Per-partition distribution of the live data files at `snapshot_id`."""
    meta = quote_ident(con.meta)
    params = {"table_id": table.table_id}
    files = con.query_arrow(
        f"SELECT data_file_id, partition_id, file_size_bytes, begin_snapshot, "
        f"       end_snapshot, partial_max "
        f"FROM {meta}.ducklake_data_file WHERE table_id = $table_id",
        params,
    ).to_pylist()
    values = con.query_arrow(
        f"SELECT data_file_id, partition_key_index, partition_value "
        f"FROM {meta}.ducklake_file_partition_value WHERE table_id = $table_id",
        params,
    ).to_pylist()
    deletes = con.query_arrow(
        f"SELECT data_file_id, begin_snapshot "
        f"FROM {meta}.ducklake_delete_file WHERE table_id = $table_id",
        params,
    ).to_pylist()
    # only the snapshots referenced by the table's files
    snapshots = con.query_arrow(
        f"SELECT s.snapshot_id, "
        f"       epoch(CAST(s.snapshot_time AS TIMESTAMPTZ)) AS snapshot_epoch, "
        f"       c.changes_made "
        f"FROM {meta}.ducklake_snapshot AS s "
        f"LEFT JOIN {meta}.ducklake_snapshot_changes AS c "
        f"  ON c.snapshot_id = s.snapshot_id "
        f"WHERE s.snapshot_id IN ("
        f"  SELECT begin_snapshot FROM {meta}.ducklake_data_file "
        f"  WHERE table_id = $table_id "
        f"  UNION SELECT partial_max FROM {meta}.ducklake_data_file "
        f"  WHERE table_id = $table_id "
        f"  UNION SELECT begin_snapshot FROM {meta}.ducklake_delete_file "
        f"  WHERE table_id = $table_id)",
        params,
    ).to_pylist()
    fields = _fields(con, table)

    epochs: dict[int, float | None] = {}
    changes: dict[int, str | None] = {}
    for r in snapshots:
        sid = int(r["snapshot_id"])
        epochs[sid] = (
            float(r["snapshot_epoch"]) if r["snapshot_epoch"] is not None else None
        )
        changes[sid] = str(r["changes_made"]) if r["changes_made"] is not None else None
    compaction = _compaction_snapshots(changes, table.table_id)

    by_file: dict[int, dict[int, str | None]] = defaultdict(dict)
    for r in values:
        value = r["partition_value"]
        by_file[int(r["data_file_id"])][int(r["partition_key_index"])] = (
            None if value is None else str(value)
        )

    def key_of(row: dict[str, Any]) -> PartitionKey:
        file_values = by_file.get(int(row["data_file_id"]), {})
        pid = row["partition_id"]
        return (
            int(pid) if pid is not None else None,
            tuple(file_values[i] for i in sorted(file_values)),
        )

    last_write: dict[PartitionKey, int] = {}
    live_sizes: dict[PartitionKey, list[int]] = defaultdict(list)
    key_by_file: dict[int, PartitionKey] = {}

    def touch(key: PartitionKey, raw: Any) -> None:
        """Record `raw` as a partition write unless it is compaction."""
        if raw is None:
            return
        sid = int(raw)
        if sid <= snapshot_id and sid not in compaction:
            last_write[key] = max(sid, last_write.get(key, -1))

    for row in files:
        key = key_of(row)
        key_by_file[int(row["data_file_id"])] = key
        begin = row["begin_snapshot"]
        end = row["end_snapshot"]
        # merge: begin = smallest snapshot, partial_max = largest; one of them
        # may be a compaction one (merged rewrite file), the other is not
        touch(key, row["partial_max"])
        touch(key, begin)
        live = (
            begin is not None
            and snapshot_id >= int(begin)
            and (end is None or snapshot_id < int(end))
        )
        if live and row["file_size_bytes"] is not None:
            live_sizes[key].append(int(row["file_size_bytes"]))
    for row in deletes:
        file_key = key_by_file.get(int(row["data_file_id"]))
        if file_key is not None:
            touch(file_key, row["begin_snapshot"])

    reference = now if now is not None else datetime.now(UTC)
    cutoff = reference - timedelta(seconds=closed_after_seconds)
    partitions: list[PartitionFileSizes] = []
    for key in sorted(live_sizes, key=_sort_key):
        last = last_write.get(key)
        epoch = epochs.get(last) if last is not None else None
        when = datetime.fromtimestamp(epoch, UTC) if epoch is not None else None
        sizes = tuple(sorted(live_sizes[key]))
        names = fields.get(key[0]) if key[0] is not None else None
        partitions.append(
            PartitionFileSizes(
                partition_id=key[0],
                values=key[1],
                label=_label(key[1], names),
                state="closed" if when is None or when <= cutoff else "open",
                last_write_snapshot=last,
                last_write_time=when,
                distribution=FileSizeDistribution.from_sizes(
                    sizes, target_file_size_bytes
                ),
                sizes=sizes,
            )
        )
    return PartitionSizeReport(
        closed_after_seconds=closed_after_seconds,
        reference_time=reference,
        partitions=tuple(partitions),
        target_file_size_bytes=target_file_size_bytes,
    )


def _sort_key(key: PartitionKey) -> tuple[int, tuple[tuple[int, int, str], ...]]:
    """Sort by partition spec and then by value; integer values (epoch_day,
    year) in numeric order, NULL first."""

    def one(value: str | None) -> tuple[int, int, str]:
        if value is None:
            return (0, 0, "")
        if value.removeprefix("-").isdigit():
            return (1, int(value), "")
        return (2, 0, value)

    return (key[0] if key[0] is not None else -1, tuple(one(v) for v in key[1]))
