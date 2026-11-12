"""Publication boundary failures must recover without new source changes."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from pathlib import Path
from unittest import mock

import pytest
from moto import mock_aws

from tests.integration.conftest import CatalogHarness
from tests.integration.test_sync_budget import _config, _sync

pytestmark = [pytest.mark.integration, pytest.mark.aws]


@pytest.fixture
def glue_env() -> Iterator[None]:
    with mock_aws(), mock.patch("ducklake_iceberg.glue._validate_metadata_location"):
        yield


def _hashes(path: Path) -> dict[str, str]:
    return {
        str(p): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in path.rglob("*.parquet")
    }


@pytest.mark.parametrize("attempt", range(3))
@pytest.mark.parametrize("successor", [False, True])
@pytest.mark.parametrize(
    "boundary",
    ["write_snapshot", "write_metadata_json", "write_pointer", "register"],
)
def test_publication_failure_and_dry_run_recovery(
    catalog: CatalogHarness,
    tmp_path: Path,
    glue_env: None,
    attempt: int,
    successor: bool,
    boundary: str,
) -> None:
    config = _config(catalog, tmp_path, inlining=0)
    catalog.write("CREATE TABLE lake.main.t(id BIGINT)")
    catalog.write("INSERT INTO lake.main.t SELECT * FROM range(100)")
    if successor:
        code, doc = _sync(config)
        assert code == 0, doc["problems"]
        catalog.write("INSERT INTO lake.main.t VALUES (101)")
    before = _hashes(Path(catalog.data_path))
    target = (
        "ducklake_iceberg.pipeline.run_register"
        if boundary == "register"
        else "ducklake_iceberg.export." + boundary
    )
    with mock.patch(target, side_effect=OSError("injected publication failure")):
        code, doc = _sync(config)
    assert code == 1, (attempt, doc)
    assert _hashes(Path(catalog.data_path)) == before
    artifacts = {
        str(p): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in (tmp_path / "iceberg").rglob("*")
        if p.is_file()
    }
    code, doc = _sync(config, "--dry-run")
    assert code in (0, 4), doc["problems"]
    assert artifacts == {
        str(p): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in (tmp_path / "iceberg").rglob("*")
        if p.is_file()
    }
    code, doc = _sync(config)
    assert code == 0, doc["problems"]
    code, doc = _sync(config)
    assert code == 4, doc["problems"]
    assert _hashes(Path(catalog.data_path)) == before
