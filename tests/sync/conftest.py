"""Fixtures for tests/sync: real SQLite DuckLake catalog (no Postgres)."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.integration.conftest import CatalogHarness, _make_sqlite


@pytest.fixture
def lake(tmp_path: Path) -> CatalogHarness:
    """Empty SQLite catalog with inlining off (every INSERT becomes a file)."""
    harness = _make_sqlite(tmp_path)
    harness.write("CALL lake.set_option('data_inlining_row_limit', 0)")
    return harness
