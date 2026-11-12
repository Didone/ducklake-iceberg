"""No-op discovery stays bounded as live files and source history grow."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from unittest import mock

import pytest
from moto import mock_aws

from ducklake_iceberg import timing
from ducklake_iceberg.catalog.connection import open_catalog
from tests.integration.conftest import CatalogHarness
from tests.integration.test_sync_budget import _config, _sync

pytestmark = [pytest.mark.integration, pytest.mark.aws]


@pytest.fixture
def glue_env() -> Iterator[None]:
    with mock_aws(), mock.patch("ducklake_iceberg.glue._validate_metadata_location"):
        yield


@pytest.mark.parametrize("files", [8, 64, 1024])
@pytest.mark.parametrize("history", [1, 32, 256])
def test_noop_file_and_source_history_scaling(
    catalog: CatalogHarness, tmp_path: Path, glue_env: None, files: int, history: int
) -> None:
    config = _config(catalog, tmp_path, inlining=0)
    with open_catalog(catalog.settings(), read_only=False, s3=False) as source:
        source.execute("CREATE TABLE lake.main.t(id BIGINT)")
        source.execute("CREATE TABLE lake.main.other(id BIGINT)")
        for i in range(files):
            source.execute("INSERT INTO lake.main.t VALUES ($id)", {"id": i})
    code, doc = _sync(config)
    assert code == 0, doc["problems"]
    with open_catalog(catalog.settings(), read_only=False, s3=False) as source:
        for i in range(history - 1):
            source.execute("INSERT INTO lake.main.other VALUES ($id)", {"id": i})
    code, doc = _sync(config)
    assert code == 4, doc["problems"]
    assert timing.counter("duckdb.connect") == 1
    assert timing.counter("aws.glue.GetTable") == 1
    assert timing.counter("catalog.queries") <= 25
    assert timing.counter("footers.files") == 0
    assert doc["results"][0]["steps"][2]["data_files"] == files
