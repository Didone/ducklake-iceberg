"""Unit tests for the flush contract (model, fingerprint)."""

from __future__ import annotations

from ducklake_iceberg.checkpoint.model import (
    CatalogFingerprint,
    CheckpointError,
    TableRef,
)
from ducklake_iceberg.errors import ExitCode

T1 = TableRef("vendas", "pedidos", 5)


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


def test_checkpoint_error_preserves_raw_and_phase() -> None:
    err = CheckpointError("failed", table=T1, phase="flush", raw="Conflict")
    assert err.exit_code == ExitCode.ERROR
    assert err.to_json()["phase"] == "flush"
    assert err.to_json()["raw"] == "Conflict"
    assert err.to_json()["code"] == "E-CHECKPOINT-UNKNOWN"
