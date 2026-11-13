"""End-to-end envelope, log and redaction checks on the real CLI output.

Real DuckLake (SQLite and Postgres, via the ``catalog`` fixture) -> moto Glue,
Iceberg metadata under a local prefix. Covers the scenarios that need a real
command: the ``sync`` envelope against the schema, log line format,
``sync.end`` fields, phase metrics, credential redaction on every output
surface and the exit code with a closed stderr. Also proves that worker
events carry ``run_id``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import duckdb
from jsonschema import Draft202012Validator
from moto import mock_aws
from typer.testing import CliRunner

from ducklake_iceberg import cli, pipeline
from ducklake_iceberg.errors import AwsError, CatalogError
from ducklake_iceberg.obs.logging import EVENTS, log_event

from .conftest import CatalogHarness

pytestmark = [pytest.mark.integration, pytest.mark.aws]

runner = CliRunner()
REGION = "us-east-1"
SCHEMA = json.loads(
    (
        Path(__file__).parents[2] / "src/ducklake_iceberg/schemas/envelope-v1.json"
    ).read_text(encoding="utf-8")
)


@pytest.fixture
def glue_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("DUCKLAKE_ICEBERG_STATE_DIR", str(tmp_path / "state"))
    with mock_aws(), mock.patch("ducklake_iceberg.glue._validate_metadata_location"):
        yield


@pytest.fixture
def config(catalog: CatalogHarness, tmp_path: Path) -> Path:
    catalog.write("CALL lake.set_option('data_inlining_row_limit', 0)")
    path = tmp_path / "ducklake-iceberg.yaml"
    path.write_text(
        f"catalog_dsn: '{catalog.dsn}'\n"
        f"data_path: '{catalog.data_path}'\n"
        f"iceberg_metadata_prefix: '{tmp_path / 'iceberg'}'\n"
        "glue_database: analytics\n"
        f"aws_region: {REGION}\n"
    )
    return path


def _seed(catalog: CatalogHarness, name: str = "t") -> None:
    catalog.write(
        f"CREATE TABLE lake.main.{name} (id INTEGER, v VARCHAR)",
        f"INSERT INTO lake.main.{name} VALUES (1, 'a'), (2, 'b')",
    )


def _run(config: Path, *args: str) -> tuple[int, dict[str, Any], str]:
    result = runner.invoke(
        cli.app, [*args, "--config", str(config), "--json", "--log-format", "json"]
    )
    return result.exit_code, json.loads(result.stdout), result.stderr


def _events(stderr: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in stderr.splitlines()]


def _phases(result: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {phase["phase"]: phase for phase in result["metrics"]["phases"]}


# -- every sync scenario's envelope validates against the schema --------------

#: (arguments, setup commands, accepted exit codes)
SCENARIOS: dict[str, tuple[tuple[str, ...], tuple[tuple[str, ...], ...], set[int]]] = {
    "sync": (("sync", "--table", "main.t"), (), {0}),
    "sync-dry-run": (("sync", "--table", "main.t", "--dry-run"), (), {0}),
    "sync-again": (
        ("sync", "--table", "main.t"),
        (("sync", "--table", "main.t"),),
        {4},
    ),
}


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_sync_envelope_matches_schema(
    catalog: CatalogHarness, config: Path, glue_env: None, name: str
) -> None:
    args, setup, accepted = SCENARIOS[name]
    _seed(catalog)
    for step in setup:
        code, doc, _ = _run(config, *step)
        assert code in (0, 4), doc["problems"]

    code, doc, stderr = _run(config, *args)

    assert code in accepted, doc["problems"]
    Draft202012Validator(SCHEMA).validate(doc)
    assert doc["command"] == "sync"
    assert doc["results"], "command returned no result"
    first = doc["results"][0]
    assert first["metrics"]["run_id"] == _events(stderr)[0]["run_id"]
    # stdout is exactly one JSON object; stderr, one JSON line per event
    assert all(event["event"] in EVENTS for event in _events(stderr))


def test_blocked_envelope_matches_schema(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.bad (id INTEGER, qty UINTEGER)")

    code, doc, _ = _run(config, "sync", "--table", "main.bad")

    assert code == 2
    Draft202012Validator(SCHEMA).validate(doc)
    assert doc["status"] == "blocked"
    assert doc["problems"][0]["code"] == "blocked"


def test_schema_rejects_other_commands_and_malformed_results(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """The schema only describes ``sync``: the validation is not vacuous."""
    _seed(catalog)
    code, doc, _ = _run(config, "sync", "--table", "main.t")
    assert code == 0
    validator = Draft202012Validator(SCHEMA)
    assert not validator.is_valid({**doc, "command": "doctor"})
    broken = {
        **doc,
        "results": [{k: v for k, v in doc["results"][0].items() if k != "steps"}],
    }
    assert not validator.is_valid(broken)
    assert not validator.is_valid({**doc, "version": 2})


def test_metrics_block_is_always_present(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    _seed(catalog)
    code, doc, _ = _run(config, "sync", "--table", "main.t")
    assert code == 0
    phases = _phases(doc["results"][0])
    assert phases["resolve"]["items"] == 1
    assert phases["doctor"]["items"] >= 6  # evaluated rules


# -- log lines, sync.end fields and the workers' context ----------------------


def test_sync_log_lines_and_sync_end_fields(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    _seed(catalog, "t1")

    result = runner.invoke(
        cli.app,
        [
            "sync",
            "--table",
            "main.t1",
            "--config",
            str(config),
            "--json",
            "--log-format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.stdout
    envelope = json.loads(result.stdout)  # a single object on stdout
    Draft202012Validator(SCHEMA).validate(envelope)
    lines = result.stderr.splitlines()
    events = [json.loads(line) for line in lines]  # every line is parseable
    assert all("\n" not in line for line in lines)
    run_id = events[0]["run_id"]
    for event in events:
        # events emitted from the worker threads carry it too
        assert event["run_id"] == run_id, event
        assert event["command"] == "sync"
        assert event["log_schema_version"] == 1
        assert event["event"] in EVENTS
        assert {"ts", "level"} <= set(event)
    assert events[0]["event"] == "command.start"
    assert events[-1]["event"] == "command.end"
    assert events[-1]["exit_code"] == 0

    ends = {e["table"]: e for e in events if e["event"] == "sync.end"}
    assert set(ends) == {"main.t1"}
    types = {
        "table": str,
        "snapshot_id": int,
        "iceberg_snapshot_id": int,
        "files_added": int,
        "files_removed": int,
        "delete_files_added": int,
        "bytes_referenced": int,
        "bytes_written": int,
        "duration_ms": int,
        "dry_run": bool,
        "exit_code": int,
    }
    for end in ends.values():
        for key, kind in types.items():
            assert isinstance(end[key], kind), (key, end)
    phase_ends = [e for e in events if e["event"] == "phase.end"]
    assert {e["table"] for e in phase_ends} == {"main.t1"}

    by_table = {r["table"]: r for r in envelope["results"]}
    result_t1 = by_table["main.t1"]
    assert result_t1["ducklake_snapshot_id"] == ends["main.t1"]["snapshot_id"]
    assert result_t1["bytes_written"] == ends["main.t1"]["bytes_written"]
    phases = _phases(result_t1)
    assert {
        "resolve",
        "doctor",
        "delta",
        "enumerate",
        "manifest",
        "manifest_list",
        "metadata_json",
        "s3_put",
        "glue",
    } <= set(phases)
    assert phases["enumerate"]["items"] == 1
    assert phases["enumerate"]["bytes"] == result_t1["bytes_referenced"] > 0
    assert phases["delta"]["items"] == 1
    assert phases["manifest"]["items"] == 1
    assert phases["manifest_list"]["items"] == 1
    assert phases["metadata_json"]["items"] == 1
    # manifest + manifest list + metadata.json + handoff pointer
    assert phases["s3_put"]["items"] == phases["s3_put"]["calls"] == 4
    assert phases["glue"]["items"] == 1
    assert phases["glue"]["calls"] >= 1  # counted by the botocore hook
    assert result_t1["bytes_written"] == sum(
        phases[name]["bytes"] for name in ("manifest", "manifest_list", "metadata_json")
    )
    metrics = result_t1["metrics"]
    assert sum(p["duration_ms"] for p in metrics["phases"]) <= metrics["duration_ms"]
    assert metrics["totals"]["bytes_written"] == result_t1["bytes_written"]


def test_glue_failure_is_recorded_in_phase_metrics(
    catalog: CatalogHarness,
    config: Path,
    glue_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The failed phase shows up with ``ok=false`` and ``error_code``."""
    _seed(catalog)

    def broken(*_: Any, **__: Any) -> Any:
        raise AwsError("Glue unavailable")

    monkeypatch.setattr(pipeline, "run_register", broken)
    code, doc, stderr = _run(config, "sync", "--table", "main.t")

    assert code == 1
    Draft202012Validator(SCHEMA).validate(doc)
    glue = _phases(doc["results"][0])["glue"]
    assert (glue["ok"], glue["error_code"]) == (False, "AwsError")
    failed = [e for e in _events(stderr) if e["event"] == "phase.end" and not e["ok"]]
    assert [e["phase"] for e in failed] == ["glue"]


def test_sync_is_zero_copy(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """KB written, MB referenced: the zero-copy indicator."""
    catalog.write(
        "CREATE TABLE lake.main.big (id BIGINT, h VARCHAR)",
        "INSERT INTO lake.main.big SELECT i, md5(i::VARCHAR) FROM range(200000) r(i)",
    )

    code, doc, _ = _run(config, "sync", "--table", "main.big")

    assert code == 0, doc["problems"]
    result = doc["results"][0]
    assert 0 < result["bytes_written"] < 1_000_000
    assert result["bytes_referenced"] > 100 * result["bytes_written"]


# -- synthetic credentials on every output surface ----------------------------

PASSWORD = "S3cr3t-PW-0123"
ACCESS_KEY = "AKIASYNTHETIC0000001"
SESSION_KEY = "ASIASYNTHETIC0000002"
SIGNATURE = "synthetic-signature-9f8e"
STS_TOKEN = "synthetic-token-7a6b"
ENCRYPTION_KEY = "synthetic-encryption-key"
HOST = "db.internal.example"
MARKERS = (
    PASSWORD,
    ACCESS_KEY,
    SESSION_KEY,
    SIGNATURE,
    STS_TOKEN,
    ENCRYPTION_KEY,
    HOST,
)
LEAKY_MESSAGE = (
    f'IO Error: Unable to connect to Postgres at "host={HOST} port=5432 '
    f'user=ducklake-iceberg password={PASSWORD} dbname=lake": refused; '
    f"aws_access_key_id={ACCESS_KEY} {SESSION_KEY} "
    f"https://b.s3.amazonaws.com/k?X-Amz-Signature={SIGNATURE}"
    f"&X-Amz-Security-Token={STS_TOKEN}"
)


def _surfaces(config: Path, *args: str) -> list[tuple[str, str]]:
    """stdout/stderr with ``--json`` + JSON log + DEBUG, and the terminal mode."""
    outputs: list[tuple[str, str]] = []
    for extra in (
        ("--json", "--log-format", "json", "--log-level", "DEBUG"),
        ("--log-format", "text", "--log-level", "DEBUG"),
    ):
        result = runner.invoke(cli.app, [*args, "--config", str(config), *extra])
        assert result.exit_code in (0, 1, 4), result.output
        assert result.exception is None or isinstance(result.exception, SystemExit)
        outputs += [(f"stdout {extra[0]}", result.stdout)]
        outputs += [(f"stderr {extra[0]}", result.stderr)]
    return outputs


def _assert_clean(outputs: list[tuple[str, str]]) -> None:
    for where, text in outputs:
        for marker in MARKERS:
            assert marker not in text, f"{marker} leaked in {where}"


def test_password_forced_by_env_never_surfaces(
    catalog: CatalogHarness,
    config: Path,
    glue_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DSN with a literal password is rejected without echoing it."""
    monkeypatch.setenv(
        "DUCKLAKE_ICEBERG_CATALOG_DSN",
        f"ducklake:postgres:dbname=lake host={HOST} password={PASSWORD}",
    )
    outputs = _surfaces(config, "sync", "--table", "main.t")
    doc = json.loads(outputs[0][1])
    assert doc["exit_code"] == 1
    assert doc["problems"][0]["code"] == "config_invalid"
    _assert_clean(outputs)


@pytest.mark.parametrize("wrapped", [False, True], ids=["duckdb", "catalog_error"])
def test_postgres_extension_error_is_scrubbed_everywhere(
    catalog: CatalogHarness,
    config: Path,
    glue_env: None,
    monkeypatch: pytest.MonkeyPatch,
    wrapped: bool,
) -> None:
    """The raw ``postgres`` extension message is emitted with ``host=***``,
    whether converted to ``CatalogError`` or raised as an unhandled exception."""
    _seed(catalog)

    def fail(*_: Any, **__: Any) -> Any:
        if wrapped:
            raise CatalogError(f"failed to attach the catalog: {LEAKY_MESSAGE}")
        raise duckdb.IOException(LEAKY_MESSAGE)

    monkeypatch.setattr(pipeline, "open_catalog", fail)
    outputs = _surfaces(config, "sync", "--table", "main.t")

    doc = json.loads(outputs[0][1])
    Draft202012Validator(SCHEMA).validate(doc)
    (problem,) = doc["problems"]
    assert problem["code"] == ("error" if wrapped else "internal_error")
    assert "host=***" in problem["message"]
    logged = [e for e in _events(outputs[1][1]) if e["event"] == "problem"]
    assert "host=***" in logged[0]["message"]
    _assert_clean(outputs)


def test_encryption_key_and_botocore_debug_never_surface(
    catalog: CatalogHarness,
    config: Path,
    glue_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``data_file_encryption_key``/``key_metadata`` stay out of the output;
    ``--log-level DEBUG`` does not print the ``Authorization`` header."""
    _seed(catalog)
    real = pipeline.sync_table

    def leaky(*args: Any, **kwargs: Any) -> Any:
        code = real(*args, **kwargs)
        result = kwargs["envelope"].results[0]
        result["files"] = [
            {
                "path": "s3://b/f.parquet",
                "data_file_encryption_key": ENCRYPTION_KEY,
                "key_metadata": ENCRYPTION_KEY,
            }
        ]
        log_event(
            "phase.end", phase="enumerate", data_file_encryption_key=ENCRYPTION_KEY
        )
        return code

    monkeypatch.setattr("ducklake_iceberg.cli.sync_table", leaky)
    outputs = _surfaces(config, "sync", "--table", "main.t")

    doc = json.loads(outputs[0][1])
    assert doc["results"][0]["files"] == [{"path": "s3://b/f.parquet"}]
    _assert_clean(outputs)
    for where, text in outputs:
        if where.startswith("stderr"):
            assert "AWS4-HMAC-SHA256" not in text
            assert "Authorization" not in text


# -- a closed stderr does not change the exit code ----------------------------


def test_closed_stderr_keeps_the_exit_code(
    catalog: CatalogHarness, config: Path
) -> None:
    _seed(catalog)
    executable = Path(sys.executable).with_name("ducklake-iceberg")
    args = [str(executable), "sync", "--table", "main.t", "--dry-run"]
    args += ["--json", "--config", str(config)]

    normal = subprocess.run(args, capture_output=True, text=True, check=False)  # noqa: S603
    closed = subprocess.run(  # noqa: S603
        ["/bin/sh", "-c", '"$@" 2>&-', "sh", *args],
        stdout=subprocess.PIPE,
        text=True,
        check=False,
    )

    assert normal.returncode == 0, normal.stderr
    assert closed.returncode == normal.returncode
    assert json.loads(closed.stdout)["exit_code"] == normal.returncode
