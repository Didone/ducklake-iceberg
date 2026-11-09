"""Packaging contract: the package imports and exposes the pyproject version."""

from __future__ import annotations

import tomllib
from importlib.metadata import version
from pathlib import Path

import ducklake_iceberg

_PYPROJECT = Path(__file__).resolve().parents[2] / "pyproject.toml"


def _pyproject_version() -> str:
    data = tomllib.loads(_PYPROJECT.read_text(encoding="utf-8"))
    return str(data["project"]["version"])


def test_package_imports_and_exposes_version() -> None:
    assert ducklake_iceberg.__version__
    assert ducklake_iceberg.__version__ == version("ducklake-iceberg")


def test_version_matches_pyproject() -> None:
    assert ducklake_iceberg.__version__ == _pyproject_version()
