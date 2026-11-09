"""Unit tests for the output envelope and exit severity (output.py)."""

from __future__ import annotations

import json
from typing import Any

import pytest

from iceduck.errors import ExitCode
from iceduck.output import (
    ENVELOPE_VERSION,
    STATUS_BY_EXIT,
    Envelope,
    Problem,
    worse_exit,
)


def test_status_by_exit_covers_all_codes() -> None:
    assert set(STATUS_BY_EXIT) == set(ExitCode)


@pytest.mark.parametrize(
    ("a", "b", "expected"),
    [
        (ExitCode.SUCCESS, ExitCode.NOTHING_TO_DO, ExitCode.NOTHING_TO_DO),
        (ExitCode.NOTHING_TO_DO, ExitCode.DIVERGED, ExitCode.DIVERGED),
        (ExitCode.DIVERGED, ExitCode.BLOCKED, ExitCode.BLOCKED),
        (ExitCode.BLOCKED, ExitCode.ERROR, ExitCode.ERROR),
        (ExitCode.ERROR, ExitCode.SUCCESS, ExitCode.ERROR),
        (ExitCode.SUCCESS, ExitCode.SUCCESS, ExitCode.SUCCESS),
    ],
)
def test_worse_exit_severity(a: ExitCode, b: ExitCode, expected: ExitCode) -> None:
    # severity 0 < 4 < 3 < 2 < 1
    assert worse_exit(a, b) == expected
    assert worse_exit(b, a) == expected


def test_envelope_to_json_is_valid_and_versioned() -> None:
    env = Envelope(command="doctor", dry_run=True)
    env.results.append({"table": "vendas.pedidos"})
    env.problems.append(Problem(code="x", message="failed", table="a.b"))
    payload = json.loads(env.to_json())
    assert payload["version"] == ENVELOPE_VERSION
    assert payload["command"] == "doctor"
    assert payload["dry_run"] is True
    assert payload["status"] == "ok"
    assert payload["results"][0]["table"] == "vendas.pedidos"
    assert payload["problems"][0]["code"] == "x"
    assert payload["problems"][0]["table"] == "a.b"
    # started_at serializes as an ISO string
    assert isinstance(payload["started_at"], str)


def test_set_exit_syncs_status() -> None:
    err = Envelope(command="export", dry_run=False)
    err.set_exit(ExitCode.ERROR)
    assert err.exit_code == 1
    assert err.status == "error"

    nothing = Envelope(command="sync", dry_run=False)
    nothing.set_exit(ExitCode.NOTHING_TO_DO)
    assert nothing.exit_code == 4
    assert nothing.status == "nothing-to-do"


def _doctor_result(table: str, *, glue_exists: bool, s3: object) -> dict[str, object]:
    return {
        "table": table,
        "identity": {
            "table_id": 2,
            "table_uuid": "uuid-x",
            "schema_id": 1,
            "schema_version": 2,
        },
        "snapshot": {
            "snapshot_id": 4,
            "snapshot_time": "2026-01-01",
            "schema_version": 2,
        },
        "environment": {
            "duckdb_version": "v2.0.0",
            "extensions": {"ducklake": True, "aws": True},
            "attached": ["lake", "lake_meta"],
            "read_only": True,
        },
        "aws": {
            "region": "sa-east-1",
            "profile": None,
            "glue_database": "analytics",
            "glue_reachable": True,
            "glue_database_exists": glue_exists,
            "data_path_readable": s3,
        },
        "inlined_rows": 0,
    }


def test_emit_human_single(capsys: pytest.CaptureFixture[str]) -> None:
    from iceduck.output import emit

    env = Envelope(command="doctor", dry_run=False)
    env.results.append(_doctor_result("vendas.pedidos", glue_exists=False, s3=True))
    emit(env, json_out=False)
    out = capsys.readouterr().out
    assert "vendas.pedidos" in out
    assert "table_id" in out
    assert "will be created on export" in out


def test_emit_human_multi(capsys: pytest.CaptureFixture[str]) -> None:
    from iceduck.output import emit

    env = Envelope(command="doctor", dry_run=False)
    env.results.append(_doctor_result("a.x", glue_exists=True, s3=None))
    env.results.append(_doctor_result("a.y", glue_exists=True, s3=False))
    emit(env, json_out=False)
    out = capsys.readouterr().out
    assert "a.x" in out
    assert "a.y" in out
    assert "--all-tables" in out


def test_emit_human_problem(capsys: pytest.CaptureFixture[str]) -> None:
    from iceduck.output import emit

    env = Envelope(command="doctor", dry_run=False)
    env.problems.append(Problem(code="x", message="failed", hint="tip", table="a.b"))
    env.set_exit(ExitCode.ERROR)
    emit(env, json_out=False)
    out = capsys.readouterr().out
    assert "failed" in out
    assert "a.b" in out
    assert "tip" in out


def test_emit_json_writes_single_object(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from iceduck.output import emit

    env = Envelope(command="doctor", dry_run=False)
    emit(env, json_out=True)
    out = capsys.readouterr().out
    payload = json.loads(out)  # exactly one object
    assert payload["command"] == "doctor"


def _rules_report(exit_code: int, rules: list[dict[str, Any]]) -> dict[str, Any]:
    blocker = sum(
        1
        for r in rules
        for f in r.get("findings", [])
        if f.get("severity") == "blocker"
    )
    skipped = sum(1 for r in rules if r.get("status") == "skipped")
    return {
        "exit_code": exit_code,
        "summary": {
            "blocker": blocker,
            "degraded": 0,
            "info": 0,
            "rules_skipped": skipped,
            "rules_errored": sum(1 for r in rules if r.get("status") == "error"),
            "short_circuited_by": ["inlined-data"] if skipped else [],
        },
        "rules": rules,
    }


def test_render_doctor_with_rules_all_states(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from iceduck.output import render_doctor

    result = _doctor_result("vendas.pedidos", glue_exists=True, s3=True)
    result["rules"] = _rules_report(
        2,
        [
            {
                "id": "inlined-data",
                "status": "failed",
                "findings": [
                    {
                        "severity": "blocker",
                        "subject": "vendas.pedidos",
                        "detail": "3 inlined rows",
                        "remediation": "iceduck checkpoint",
                    }
                ],
                "skipped_by": [],
            },
            {
                "id": "partial-files",
                "status": "skipped",
                "findings": [],
                "skipped_by": ["inlined-data"],
            },
            {"id": "unsupported-type", "status": "passed", "findings": []},
            {
                "id": "mapping-divergence",
                "status": "error",
                "error": "duckdb.Error: x",
                "findings": [],
            },
        ],
    )
    render_doctor(result)
    out = capsys.readouterr().out
    assert "Compatibility preflight" in out
    assert "inlined-data" in out
    assert "SKIPPED" in out
    assert "OK" in out
    assert "ERROR" in out
    assert "not evaluated" in out  # paragraph for skipped rules


def test_render_doctor_table_preflight_verdicts(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from iceduck.output import Envelope, emit

    env = Envelope(command="doctor", dry_run=False)
    clean = _doctor_result("a.clean", glue_exists=True, s3=True)
    clean["rules"] = _rules_report(0, [{"id": "x", "status": "passed", "findings": []}])
    blocked = _doctor_result("a.bad", glue_exists=True, s3=True)
    blocked["rules"] = _rules_report(
        2,
        [
            {
                "id": "unsupported-type",
                "status": "failed",
                "findings": [
                    {
                        "severity": "blocker",
                        "subject": "a.bad",
                        "detail": "d",
                        "remediation": "r",
                    }
                ],
            }
        ],
    )
    env.results.append(clean)
    env.results.append(blocked)
    emit(env, json_out=False)
    out = capsys.readouterr().out
    assert "ok" in out
    assert "blocker" in out


# -- problem level and the consolidated sync report --------------------------


def test_problem_level_is_optional_key_of_envelope_v1() -> None:
    from pathlib import Path

    from jsonschema import Draft202012Validator

    env = Envelope(command="sync", dry_run=False)
    env.problems.append(Problem(code="x", message="failed"))
    env.problems.append(
        Problem(code="workers_capped", message="reduced", level="warning")
    )
    payload = json.loads(env.to_json())

    assert "level" not in payload["problems"][0]
    assert payload["problems"][1]["level"] == "warning"
    schema_path = Path(__file__).parents[2] / "src/iceduck/schemas/envelope-v1.json"
    Draft202012Validator(json.loads(schema_path.read_text("utf-8"))).validate(payload)


def test_warning_problem_is_not_rendered_as_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from iceduck.output import emit

    env = Envelope(command="sync", dry_run=False)
    env.problems.append(
        Problem(code="workers_capped", message="footers reduced", level="warning")
    )
    emit(env, json_out=False)
    out = capsys.readouterr().out

    assert "warning" in out
    assert "error" not in out


def _sync_row(table: str, code: int, ms: int, **extra: Any) -> dict[str, Any]:
    return {"table": table, "steps": [], "exit_code": code, "duration_ms": ms, **extra}


def test_render_sync_table_sorted_with_mandatory_footer(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """One row per table, sorted by name, and the footer."""
    from iceduck.output import emit

    env = Envelope(command="sync", dry_run=False)
    env.results = [
        _sync_row(
            "vendas.pedidos",
            0,
            4200,
            files_added=17,
            bytes_referenced=int(9.1 * 1024**3),
        ),
        _sync_row("vendas.itens", 1, 400, error="boom"),
        _sync_row("vendas.estoque", 4, 300, files_added=0, bytes_referenced=10),
        _sync_row(
            "vendas.clientes",
            0,
            1900,
            files_added=2,
            files_removed=1,
            bytes_referenced=int(1.2 * 1024**3),
        ),
    ]
    env.set_exit(ExitCode.ERROR)
    env.duration_ms = 4900
    emit(env, json_out=False)
    out = capsys.readouterr().out

    lines = [line for line in out.splitlines() if "vendas." in line]
    assert [line.split()[1] for line in lines] == [
        "vendas.clientes",
        "vendas.estoque",
        "vendas.itens",
        "vendas.pedidos",
    ]
    assert "+2 -1" in lines[0]
    assert "1.2 GB" in lines[0]
    assert "1.9 s" in lines[0]
    assert "nothing" in lines[1]
    assert "error" in lines[2]
    assert "—" in lines[2]
    assert "+17" in lines[3]
    assert "9.1 GB" in lines[3]
    assert out.strip().splitlines()[-1] == (
        "4 tables · 2 ok · 1 nothing to do · 1 error · total 4.9 s · exit 1"
    )


def test_render_sync_footer_all_nothing_to_do(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from iceduck.output import emit

    env = Envelope(command="sync", dry_run=False)
    env.results = [_sync_row("a.t", 4, 120)]
    env.set_exit(ExitCode.NOTHING_TO_DO)
    env.duration_ms = 150
    emit(env, json_out=False)

    footer = capsys.readouterr().out.strip().splitlines()[-1]
    assert footer == "1 table · 1 nothing to do · total 0.1 s · exit 4"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "—"),
        (0, "—"),
        (512, "512 B"),
        (1536, "1.5 KB"),
        (5 * 1024**2, "5.0 MB"),
        (3 * 1024**5, "3072.0 TB"),
    ],
)
def test_human_bytes(value: Any, expected: str) -> None:
    from iceduck.output import _human_bytes

    assert _human_bytes(value) == expected
