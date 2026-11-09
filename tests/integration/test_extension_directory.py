"""Read-only ``HOME`` and ``duckdb_extension_directory``.

Without the setting, ``INSTALL`` tries to create ``$HOME/.duckdb`` and fails in
a controlled way, with a clear message and a hint about the setting. With the
setting pointing to a pre-populated directory, the connection opens without
touching ``HOME``, and the ``ATTACH`` stays ``READ_ONLY``.
"""

from __future__ import annotations

import os
import shutil
import stat
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pytest

from ducklake_iceberg.catalog.connection import (
    CONNECT_CONFIG,
    DuckLakeConnection,
    open_catalog,
)
from ducklake_iceberg.errors import CatalogError
from tests.integration.conftest import CatalogHarness

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        hasattr(os, "geteuid") and os.geteuid() == 0,
        reason="root ignores the directory write permission",
    ),
]

_EXTENSOES = ("ducklake", "httpfs", "aws", "sqlite", "postgres")


@pytest.fixture
def extension_dir(tmp_path: Path) -> Path:
    """Copy the already installed extensions into a dedicated directory.

    Reproduces the ``<root>/v<version>/<platform>/`` layout DuckDB expects.
    """
    con = duckdb.connect(":memory:")
    try:
        for name in _EXTENSOES:
            con.execute(f"INSTALL {name}")
        row = con.execute(
            "SELECT install_path FROM duckdb_extensions() "
            "WHERE extension_name = 'ducklake'"
        ).fetchone()
    finally:
        con.close()
    assert row is not None
    platform_dir = Path(str(row[0])).parent
    root = tmp_path / "ext"
    target = root / platform_dir.parent.name / platform_dir.name
    shutil.copytree(platform_dir, target)
    return root


@pytest.fixture
def read_only_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    home = tmp_path / "home"
    home.mkdir()
    home.chmod(stat.S_IRUSR | stat.S_IXUSR)
    monkeypatch.setenv("HOME", str(home))
    try:
        yield home
    finally:
        home.chmod(stat.S_IRWXU)


def test_read_only_home_fails_with_hint(
    catalog: CatalogHarness, read_only_home: Path
) -> None:
    with pytest.raises(CatalogError) as exc:
        open_catalog(catalog.settings())
    assert exc.value.message == "could not load extension 'ducklake'"
    assert exc.value.hint is not None
    assert "duckdb_extension_directory" in exc.value.hint
    assert "DUCKLAKE_ICEBERG_DUCKDB_EXTENSION_DIRECTORY" in exc.value.hint
    assert not (read_only_home / ".duckdb").exists()


def test_extension_directory_allows_read_only_home(
    catalog: CatalogHarness, extension_dir: Path, read_only_home: Path
) -> None:
    # extension_dir before read_only_home: the copy uses the real HOME
    settings = catalog.settings(duckdb_extension_directory=str(extension_dir))
    con = DuckLakeConnection(settings)
    con.open()
    try:
        env = con.environment()
        assert env.extensions["ducklake"] is True
        assert env.read_only is True
        assert con._last_attach_sql is not None
        assert "READ_ONLY" in con._last_attach_sql
        rows = con.raw.execute(
            "SELECT install_path FROM duckdb_extensions() "
            "WHERE extension_name = 'ducklake'"
        ).fetchall()
        assert str(rows[0][0]).startswith(str(extension_dir))
        snapshots = con.query_arrow(
            "SELECT count(*) AS n FROM lake_meta.ducklake_snapshot"
        ).to_pydict()["n"][0]
        assert snapshots >= 1
    finally:
        con.close()
    assert not (read_only_home / ".duckdb").exists()


def test_extension_directory_from_env(
    monkeypatch: pytest.MonkeyPatch, catalog: CatalogHarness, tmp_path: Path
) -> None:
    monkeypatch.setenv(
        "DUCKLAKE_ICEBERG_DUCKDB_EXTENSION_DIRECTORY", str(tmp_path / "x")
    )
    settings = catalog.settings()
    assert settings.duckdb_extension_directory == str(tmp_path / "x")
    config = DuckLakeConnection(settings)._connect_config()
    assert config["extension_directory"] == str(tmp_path / "x")
    # the rest of the connect config is preserved (no autoinstall/autoload)
    assert {k: config[k] for k in CONNECT_CONFIG} == dict(CONNECT_CONFIG)
    unset = DuckLakeConnection(catalog.settings(duckdb_extension_directory=None))
    assert "extension_directory" not in unset._connect_config()
