"""Pinned versions of the fragile dependencies.

DuckDB, the ``ducklake``/``postgres``/``httpfs``/``aws`` extensions, and
PyIceberg silently break ducklake-iceberg when they change: extensions are published
per DuckDB version (``v<duckdb>/<platform>``) and the ``core`` repository may
swap an extension's build without changing the DuckDB version; the writer uses
internal PyIceberg constructors.

The versions below are the ones the code expects. ``pyproject.toml`` pins
``duckdb``/``pyiceberg`` with ``==`` and the image checks the extensions at
build time with ``python -m ducklake_iceberg.deps``. To upgrade: change
``pyproject.toml`` and this module together, run ``poetry lock`` and the whole
suite (``-m roundtrip`` and ``-m pins`` first). The ``compat-latest`` CI job
shows beforehand whether the new version passes.

Usage::

    python -m ducklake_iceberg.deps                  # exits 1 if anything diverges
    python -m ducklake_iceberg.deps --report-only    # report only (compat-latest job)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import Final

import duckdb

#: Exact version of the ``duckdb`` package (and engine: ``SELECT version()``).
DUCKDB_VERSION: Final[str] = "2.0.0"

#: Exact version of the ``pyiceberg`` package.
PYICEBERG_VERSION: Final[str] = "0.12.0"

#: ``extension_version`` from ``duckdb_extensions()`` (build hash in the
#: ``core`` repository for DuckDB v2.0.0), by registered name. These are the
#: extensions the image bundles; the hash is the same on all platforms.
EXTENSION_VERSIONS: Final[Mapping[str, str]] = {
    "ducklake": "417a9117f5",
    "postgres_scanner": "c0f5a3c0c4",
    "httpfs": "77392a7207",
    "aws": "16ae9c1353",
}


@dataclass(frozen=True, slots=True)
class Pin:
    """A pinned dependency: the expected and the found version (``None``: missing)."""

    name: str
    pinned: str
    found: str | None

    @property
    def ok(self) -> bool:
        return self.found == self.pinned


def _package_version(dist: str) -> str | None:
    try:
        return version(dist)
    except PackageNotFoundError:
        return None


def package_pins() -> list[Pin]:
    """Check the installed versions of ``duckdb`` and ``pyiceberg``."""
    return [
        Pin("duckdb", DUCKDB_VERSION, _package_version("duckdb")),
        Pin("pyiceberg", PYICEBERG_VERSION, _package_version("pyiceberg")),
    ]


def installed_extension_versions(
    con: duckdb.DuckDBPyConnection, names: Sequence[str] | None = None
) -> dict[str, str]:
    """``extension_version`` of the installed extensions, by registered name."""
    wanted = set(names if names is not None else EXTENSION_VERSIONS)
    rows = con.execute(
        "SELECT extension_name, extension_version FROM duckdb_extensions() "
        "WHERE installed"
    ).fetchall()
    return {str(name): str(ver) for name, ver in rows if name in wanted}


def extension_pins(con: duckdb.DuckDBPyConnection) -> list[Pin]:
    """Check the installed extensions against ``EXTENSION_VERSIONS``."""
    found = installed_extension_versions(con)
    return [
        Pin(name, pinned, found.get(name))
        for name, pinned in EXTENSION_VERSIONS.items()
    ]


def check(extension_directory: str | None = None) -> list[Pin]:
    """All checks; opens an in-memory connection without autoinstall."""
    config: dict[str, str | bool | int | float | list[str]] = {
        "autoinstall_known_extensions": False,
        "autoload_known_extensions": False,
    }
    if extension_directory:
        config["extension_directory"] = extension_directory
    con = duckdb.connect(":memory:", config=config)
    try:
        return [*package_pins(), *extension_pins(con)]
    finally:
        con.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m ducklake_iceberg.deps",
        description="Check DuckDB, extensions, and PyIceberg against the pinned "
        "versions.",
    )
    parser.add_argument(
        "--extension-directory",
        default=os.environ.get("DUCKLAKE_ICEBERG_DUCKDB_EXTENSION_DIRECTORY"),
        help=(
            "extension directory (default: DUCKLAKE_ICEBERG_DUCKDB_EXTENSION_DIRECTORY)"
        ),
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="report divergences without failing (exit 0)",
    )
    args = parser.parse_args(argv)
    pins = check(args.extension_directory)
    drift = [p for p in pins if not p.ok]
    print(
        json.dumps(
            {"ok": not drift, "pins": [asdict(p) | {"ok": p.ok} for p in pins]},
            indent=2,
        )
    )
    for p in drift:
        print(
            f"divergence: {p.name} pinned at {p.pinned}, found {p.found}",
            file=sys.stderr,
        )
    return 0 if args.report_only or not drift else 1


if __name__ == "__main__":  # pragma: no cover - build entry point
    sys.exit(main())
