"""Pinned versions of DuckDB, extensions, and PyIceberg.

The ``-m pins`` tests fail loudly on an accidental upgrade: a package outside the
``==`` pin in ``pyproject.toml`` or an extension with a different build in the
DuckDB ``core`` repository. The CI ``compat-latest`` job excludes them (there,
drift is the goal) and only reports ``python -m iceduck.deps --report-only``.
"""

from __future__ import annotations

import json
import tomllib
from collections.abc import Callable
from pathlib import Path

import duckdb
import pyiceberg
import pytest
import yaml

from iceduck import deps
from iceduck.catalog.connection import CONNECT_CONFIG, EXTENSIONS

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.pins
def test_installed_packages_match_pins() -> None:
    assert duckdb.__version__ == deps.DUCKDB_VERSION
    assert pyiceberg.__version__ == deps.PYICEBERG_VERSION
    assert all(p.ok for p in deps.package_pins()), deps.package_pins()
    con = duckdb.connect(":memory:")
    try:
        (engine,) = con.execute("SELECT version()").fetchone() or ("",)
    finally:
        con.close()
    assert engine == f"v{deps.DUCKDB_VERSION}"


@pytest.mark.pins
def test_pyproject_pins_exact_versions() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text("utf-8"))
    dependencies = project["project"]["dependencies"]
    assert f"duckdb (=={deps.DUCKDB_VERSION})" in dependencies
    assert f"pyiceberg[glue,s3fs] (=={deps.PYICEBERG_VERSION})" in dependencies


@pytest.mark.pins
def test_pinned_extensions_are_the_ones_the_image_installs() -> None:
    # The image installs exactly these (Dockerfile); all have a pinned hash
    assert set(deps.EXTENSION_VERSIONS) == {
        EXTENSIONS[n] for n in ("ducklake", "postgres", "httpfs", "aws")
    }
    dockerfile = (ROOT / "Dockerfile").read_text("utf-8")
    assert "python -m iceduck.deps" in dockerfile


@pytest.mark.pins
def test_installed_extensions_match_pins() -> None:
    con = duckdb.connect(":memory:", config=dict(CONNECT_CONFIG))
    try:
        for name in ("ducklake", "postgres", "httpfs", "aws"):
            con.execute(f"INSTALL {name}")
            con.execute(f"LOAD {name}")
        pins = deps.extension_pins(con)
    finally:
        con.close()
    drift = [p for p in pins if not p.ok]
    assert not drift, f"extensions differ from the pins in iceduck.deps: {drift}"


# -- CLI ``python -m iceduck.deps`` (no network: only what is already installed) ------


def _fake_check(*pins: deps.Pin) -> Callable[[str | None], list[deps.Pin]]:
    def check(extension_directory: str | None = None) -> list[deps.Pin]:
        return list(pins)

    return check


def test_main_ok(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(deps, "check", _fake_check(deps.Pin("duckdb", "1", "1")))
    assert deps.main([]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out == {
        "ok": True,
        "pins": [{"name": "duckdb", "pinned": "1", "found": "1", "ok": True}],
    }


def test_main_drift_fails_unless_report_only(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(deps, "check", _fake_check(deps.Pin("ducklake", "abc", None)))
    assert deps.main([]) == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out)["ok"] is False
    assert "ducklake pinned at abc, found None" in captured.err
    assert deps.main(["--report-only"]) == 0


def test_check_reads_installed_extensions(tmp_path: Path) -> None:
    # Empty directory: no extension installed, all show up as missing
    pins = {p.name: p for p in deps.check(str(tmp_path))}
    assert set(pins) == {"duckdb", "pyiceberg", *deps.EXTENSION_VERSIONS}
    assert all(pins[n].found is None for n in deps.EXTENSION_VERSIONS)
    assert pins["duckdb"].found == duckdb.__version__


def test_compat_workflow_is_scheduled_manual_and_non_blocking() -> None:
    path = ROOT / ".github/workflows/compat-latest.yml"
    workflow = yaml.safe_load(path.read_text("utf-8"))
    trigger = workflow.get("on") or workflow.get(True)
    assert set(trigger) == {"schedule", "workflow_dispatch"}  # never on PR/push
    (job,) = workflow["jobs"].values()
    assert job["continue-on-error"] is True
    runs = "\n".join(str(step.get("run", "")) for step in job["steps"])
    assert "pip install --upgrade" in runs
    assert "FORCE INSTALL" in runs
    assert "pytest -m roundtrip" in runs
    assert "not pins" in runs
    assert "iceduck.deps --report-only" in runs
