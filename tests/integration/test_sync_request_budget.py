"""Logical query and lifecycle budgets, including publication and mutations."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from unittest import mock

import pytest
from moto import mock_aws

from ducklake_iceberg import timing
from tests.integration.conftest import CatalogHarness, run_sync, sync_config

pytestmark = [pytest.mark.integration, pytest.mark.aws]


@pytest.fixture
def glue_env() -> Iterator[None]:
    with mock_aws(), mock.patch("ducklake_iceberg.glue._validate_metadata_location"):
        yield


@pytest.mark.parametrize(
    ("scenario", "cap"),
    [("N", 25), ("I", 30), ("A", 30), ("F", 50), ("D", 35), ("U", 35)],
)
def test_reference_logical_budgets(
    catalog: CatalogHarness, tmp_path: Path, glue_env: None, scenario: str, cap: int
) -> None:
    config = sync_config(catalog, tmp_path)
    catalog.write(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.t (id BIGINT, p INTEGER, payload VARCHAR)",
        "ALTER TABLE lake.main.t SET PARTITIONED BY (p)",
        *(
            f"INSERT INTO lake.main.t SELECT id, {i}, 'value' "
            f"FROM range({i * 100},{(i + 1) * 100}) t(id)"
            for i in range(4)
        ),
    )
    if scenario != "I":
        code, doc = run_sync(config)
        assert code == 0, doc["problems"]
    if scenario == "A":
        catalog.write("INSERT INTO lake.main.t VALUES (500, 0, 'new')")
    elif scenario == "F":
        catalog.write(
            "CALL lake.set_option('data_inlining_row_limit', 1000)",
            "INSERT INTO lake.main.t VALUES (500, 0, 'inline')",
        )
    elif scenario == "D":
        catalog.write("DELETE FROM lake.main.t WHERE id = 1")
    elif scenario == "U":
        catalog.write("UPDATE lake.main.t SET payload = 'changed' WHERE id = 1")
    code, doc = run_sync(config)
    assert code == (4 if scenario == "N" else 0), doc["problems"]
    assert timing.counter("catalog.queries") <= cap, timing.snapshot()["counters"]
    assert timing.counter("duckdb.connect") == 1
    assert timing.counter("ducklake.attach.successes") == (3 if scenario == "F" else 1)
    assert timing.counter("footers.files") == 0
