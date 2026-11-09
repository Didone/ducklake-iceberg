"""Unit tests for the checkpoint contract (model, fingerprint)."""

from __future__ import annotations

from datetime import UTC, datetime

from iceduck.checkpoint.model import (
    CatalogFingerprint,
    CheckpointError,
    CheckpointRunReport,
    CompactionStep,
    FileSizeDistribution,
    PartitionFileSizes,
    PartitionSizeReport,
    TableCheckpointResult,
    TableRef,
)
from iceduck.checkpoint.partitions import _compaction_snapshots, _sort_key
from iceduck.errors import ExitCode

T1 = TableRef("vendas", "pedidos", 5)
T2 = TableRef("vendas", "itens", 6)


def _fp(
    data_path: str = "/data/", version: str = "1.1", snap: int = 41
) -> CatalogFingerprint:
    return CatalogFingerprint(
        data_path=data_path,
        format_version=version,
        max_snapshot_id=snap,
        table_count=2,
    )


def test_fingerprint_matches_identical() -> None:
    assert _fp().matches(_fp())
    assert _fp().divergence(_fp()) is None


def test_fingerprint_snapshot_may_advance() -> None:
    assert _fp(snap=41).matches(_fp(snap=42))


def test_fingerprint_data_path_divergence_blocks() -> None:
    message = _fp(data_path="/data/").divergence(_fp(data_path="/other/"))
    assert message is not None
    assert "data_path" in message


def test_fingerprint_version_divergence_blocks() -> None:
    assert not _fp(version="1.1").matches(_fp(version="1.0"))
    assert "format_version" in (_fp(version="1.1").divergence(_fp(version="1.0")) or "")


def test_table_ref_qualified() -> None:
    assert T1.qualified == "vendas.pedidos"
    assert str(T1) == "vendas.pedidos"


def test_compaction_step_did_work() -> None:
    assert not CompactionStep().did_work
    assert CompactionStep(files_processed=2, files_created=1).did_work


def test_file_size_distribution_json() -> None:
    distribution = FileSizeDistribution(
        file_count=2,
        total_bytes=300,
        min_bytes=100,
        p25_bytes=125.0,
        median_bytes=150.0,
        p75_bytes=175.0,
        mean_bytes=150.0,
        max_bytes=200,
        target_file_size_bytes=400,
        below_half_target=1,
    )
    assert distribution.to_json()["median_bytes"] == 150.0
    assert distribution.to_json()["below_half_target"] == 1


def test_result_changed_by_phase() -> None:
    assert not TableCheckpointResult(T1, 41, 41).changed
    assert TableCheckpointResult(T1, 41, 42, rows_flushed=3).flushed
    merged = TableCheckpointResult(T1, 41, 42, merge=CompactionStep(2, 1))
    assert merged.compacted
    assert merged.changed


def test_expire_alone_is_not_a_change() -> None:
    # expire is global and does not change the table's live data
    result = TableCheckpointResult(T1, 41, 41, snapshots_expired=(10, 11))
    assert not result.changed


def test_result_json_phase_order() -> None:
    payload = TableCheckpointResult(
        T1,
        41,
        43,
        rows_flushed=3,
        snapshots_expired=(12,),
        merge=CompactionStep(4, 1),
    ).to_json()
    keys = list(payload)
    assert (
        keys.index("rows_flushed")
        < keys.index("snapshots_expired")
        < keys.index("merge")
        < keys.index("rewrite")
    )
    assert payload["snapshots_expired"] == [12]
    assert payload["merge"] == {"files_processed": 4, "files_created": 1}
    assert payload["changed"] is True


def test_report_totals_and_json() -> None:
    report = CheckpointRunReport(
        version=1,
        dry_run=False,
        catalog_alias="lake",
        metadata_catalog="lake_meta",
        snapshot_before=41,
        snapshot_after=43,
        results=(
            TableCheckpointResult(
                T1,
                41,
                43,
                rows_flushed=3,
                snapshots_expired=(10, 11),
                merge=CompactionStep(4, 1),
            ),
            TableCheckpointResult(T2, 43, 43, snapshots_expired=(11,)),
        ),
        wrote=True,
    )
    payload = report.to_json()
    assert payload["command"] == "checkpoint"
    assert payload["totals"] == {
        "tables_in_scope": 2,
        "tables_changed": 1,
        "rows_flushed": 3,
        "snapshots_expired": 2,  # distinct
        "files_processed": 4,
        "files_created": 1,
    }
    assert payload["tables"][1]["changed"] is False
    assert payload["exit_code"] == 0


def test_checkpoint_error_preserves_raw_and_phase() -> None:
    err = CheckpointError("failed", table=T1, phase="expire", raw="Conflict")
    assert err.exit_code == ExitCode.ERROR
    assert err.to_json()["phase"] == "expire"
    assert err.to_json()["raw"] == "Conflict"
    assert err.to_json()["code"] == "E-CHECKPOINT-UNKNOWN"


# --- per-partition distribution ----------------------------------------------
def _part(
    sizes: tuple[int, ...], state: str = "closed", target: int | None = 400
) -> PartitionFileSizes:
    return PartitionFileSizes(
        partition_id=0,
        values=("20454",),
        label="epoch_day(created_at)=20454",
        state=state,  # type: ignore[arg-type]
        last_write_snapshot=7,
        last_write_time=datetime(2026, 1, 1, tzinfo=UTC),
        distribution=FileSizeDistribution.from_sizes(sizes, target),
        sizes=sizes,
    )


def test_from_sizes_quartiles_and_below_half() -> None:
    dist = FileSizeDistribution.from_sizes([300, 100, 200], 400)
    assert (dist.min_bytes, dist.median_bytes, dist.max_bytes) == (100, 200.0, 300)
    assert dist.p25_bytes == 150.0
    assert dist.below_half_target == 1
    empty = FileSizeDistribution.from_sizes([], None)
    assert empty.file_count == 0
    assert empty.below_half_target is None


def test_partition_meets_target() -> None:
    assert _part((100, 150, 300)).meets_target is False  # median 150 < 200
    assert _part((200, 250)).meets_target is True
    # a single file: the whole partition is below target, nothing to merge
    assert _part((50,)).meets_target is True
    assert _part((50,), target=None).meets_target is None


def test_partition_report_criterion_only_on_closed() -> None:
    report = PartitionSizeReport(
        closed_after_seconds=3600,
        reference_time=datetime(2026, 1, 2, tzinfo=UTC),
        partitions=(_part((200, 300)), _part((10, 20, 30), state="open")),
        target_file_size_bytes=400,
    )
    assert report.closed_criterion_met is True
    payload = report.to_json()
    assert payload["closed_partitions"] == 1
    assert payload["open_partitions"] == 1
    assert payload["closed_files_distribution"]["file_count"] == 2
    assert payload["partitions"][0]["last_write_time"].startswith("2026-01-01")
    no_closed = PartitionSizeReport(
        3600, datetime(2026, 1, 2, tzinfo=UTC), (_part((1, 2), "open"),), 400
    )
    assert no_closed.closed_criterion_met is None


def test_compaction_snapshots_and_sort_key() -> None:
    changes = {
        3: "inserted_into_table:1",
        9: "merge_adjacent:1",
        10: "rewrite_delete:1,inserted_into_table:2",
        11: "deleted_from_table:1,inline_flush:1",
    }
    assert _compaction_snapshots(changes, 1) == {9, 10}
    assert _compaction_snapshots(changes, 2) == set()
    keys = [(0, ("10",)), (0, ("-1",)), (0, (None,)), (0, ("9",))]
    assert sorted(keys, key=_sort_key) == [
        (0, (None,)),
        (0, ("-1",)),
        (0, ("9",)),
        (0, ("10",)),
    ]
