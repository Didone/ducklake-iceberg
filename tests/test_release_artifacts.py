from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, cast

import pytest

ROOT = Path(__file__).parents[1]

# -- tag version vs. pyproject --------------------------------------------------


def _check_release(
    tmp_path: Path, tag: str, version: str, changelog: str | None = None
) -> subprocess.CompletedProcess[str]:
    import sys

    (tmp_path / "pyproject.toml").write_text(
        f'[project]\nname = "ducklake-iceberg"\nversion = "{version}"\n',
        encoding="utf-8",
    )
    args = [sys.executable, str(ROOT / "scripts/check_release_version.py"), tag]
    if changelog is not None:
        (tmp_path / "CHANGELOG.md").write_text(changelog, encoding="utf-8")
        args += ["--changelog", "CHANGELOG.md"]
    return subprocess.run(  # noqa: S603 - fixed local script and arguments.
        args, check=False, capture_output=True, text=True, cwd=tmp_path
    )


@pytest.mark.parametrize(
    ("tag", "version", "ok"),
    [
        ("v1.2.3", "1.2.3", True),
        ("v0.2.0rc1", "0.2.0rc1", True),
        ("v1.2.4", "1.2.3", False),  # mismatch fails the job
        ("1.2.3", "1.2.3", False),  # tag without the v prefix
        ("", "1.2.3", False),
        ("v1.2", "1.2", False),  # outside the X.Y.Z policy
        ("v01.2.3", "01.2.3", False),
    ],
)
def test_release_tag_must_match_pyproject(
    tmp_path: Path, tag: str, version: str, ok: bool
) -> None:
    result = _check_release(tmp_path, tag, version)
    assert (result.returncode == 0) is ok, result.stderr
    if not ok:
        assert result.stderr


def test_release_requires_changelog_section(tmp_path: Path) -> None:
    missing = _check_release(tmp_path, "v1.2.3", "1.2.3", "## [Unreleased]\n")
    assert missing.returncode == 1
    assert "## [1.2.3]" in missing.stderr
    present = _check_release(
        tmp_path, "v1.2.3", "1.2.3", "## [Unreleased]\n\n## [1.2.3] - 2026-11-06\n"
    )
    assert present.returncode == 0, present.stderr


def test_repository_version_is_releasable() -> None:
    import sys
    import tomllib

    version = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))[
        "project"
    ]["version"]
    result = subprocess.run(  # noqa: S603 - fixed local script and arguments.
        [
            sys.executable,
            str(ROOT / "scripts/check_release_version.py"),
            f"v{version}",
            "--changelog",
            "CHANGELOG.md",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    assert result.returncode == 0, result.stderr


# -- CI and release workflows ---------------------------------------------------


def _workflow(name: str) -> dict[str, Any]:
    import yaml

    loaded = yaml.safe_load((ROOT / ".github/workflows" / name).read_text("utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _run_text(job: dict[str, Any]) -> str:
    return "\n".join(str(step.get("run", "")) for step in job["steps"])


def test_release_workflow_publishes_to_pypi_with_trusted_publishing() -> None:
    workflow = _workflow("release.yml")
    # YAML 1.1 parses the ``on`` key as the boolean True
    trigger = workflow.get("on") or cast(dict[Any, Any], workflow).get(True)
    assert trigger == {"push": {"tags": ["v*"]}}
    jobs = workflow["jobs"]

    build = _run_text(jobs["build"])
    assert 'check_release_version.py "$GITHUB_REF_NAME" --changelog' in build
    assert "poetry check --lock" in build
    assert "twine check --strict" in build

    publish = jobs["publish-pypi"]
    assert set(publish["needs"]) == {"build", "container-smoke"}
    assert publish["environment"]["name"] == "pypi"
    # Trusted Publishing: OIDC token only, no stored API token.
    assert publish["permissions"] == {"id-token": "write"}
    uses = [step.get("uses", "") for step in publish["steps"]]
    assert any(u.startswith("pypa/gh-action-pypi-publish@") for u in uses)
    assert "secrets." not in str(publish)

    assert "if" not in publish  # final versions and pre-releases both go to PyPI
    (upload,) = [s for s in publish["steps"] if "pypi-publish" in s.get("uses", "")]
    assert "repository-url" not in upload.get("with", {})
    assert "testpypi" not in str(workflow).lower()

    image = jobs["container-smoke"]
    image_run = _run_text(image)
    for arg in ("VERSION", "REVISION", "SOURCE", "CREATED"):
        assert f"--build-arg {arg}=" in image_run
    assert "--network none" in image_run
    # no credential becomes a build arg
    assert "secrets." not in image_run


def test_ci_container_smoke_passes_all_label_build_args() -> None:
    job = _workflow("ci.yml")["jobs"]["container-smoke"]
    text = _run_text(job)
    for arg in ("VERSION", "REVISION", "SOURCE", "CREATED"):
        assert f"--build-arg {arg}=" in text
    assert "--network none" in text
    assert "org.opencontainers.image.revision" in text


# -- Dockerfile (static checks) -------------------------------------------------


def _dockerfile_runtime() -> str:
    text = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    return text.split("AS runtime", 1)[1]


def test_dockerfile_declares_oci_labels_from_build_args() -> None:
    runtime = _dockerfile_runtime()
    for label, arg in (
        ("version", "VERSION"),
        ("revision", "REVISION"),
        ("source", "SOURCE"),
        ("created", "CREATED"),
    ):
        assert f"ARG {arg}=" in runtime
        assert f'org.opencontainers.image.{label}="${{{arg}}}"' in runtime


def test_dockerfile_runtime_contract_and_no_credentials() -> None:
    import re

    text = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    runtime = _dockerfile_runtime()
    assert "FROM python:3.12-slim-bookworm AS runtime" in text
    assert "USER 10001:10001" in runtime
    assert "HOME=/home/ducklake-iceberg" in runtime
    assert 'ENTRYPOINT ["ducklake-iceberg"]' in runtime
    assert 'CMD ["--help"]' in runtime
    assert (
        "DUCKLAKE_ICEBERG_DUCKDB_EXTENSION_DIRECTORY=/home/ducklake-iceberg/.duckdb/extensions"
        in (runtime)
    )
    assert (
        "--network=none" in runtime
    )  # extensions proven to load without network at build time
    forbidden = re.compile(
        r"AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN|PGPASSWORD"
        r"|PASSWORD|SECRET|TOKEN",
        re.IGNORECASE,
    )
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith(("ENV", "ARG")):
            assert forbidden.search(stripped) is None, stripped


@pytest.mark.parametrize(
    ("tag", "prerelease"),
    [
        ("v0.1.0", "false"),
        ("v0.1.0rc1", "true"),
        ("v1.2.0a3", "true"),
        ("v2.0.0b1", "true"),
    ],
)
def test_release_detects_prerelease_tags(
    tag: str, prerelease: str, tmp_path: Path
) -> None:
    import shutil

    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash unavailable")
    (step,) = [
        s
        for s in _workflow("release.yml")["jobs"]["build"]["steps"]
        if s.get("id") == "version"
    ]
    output = tmp_path / "github_output"
    subprocess.run(  # noqa: S603 - the workflow's own script, fixed inputs
        [bash, "-c", step["run"]],
        check=True,
        capture_output=True,
        text=True,
        env={"GITHUB_REF_NAME": tag, "GITHUB_OUTPUT": str(output)},
        timeout=30,
    )
    lines = output.read_text(encoding="utf-8").splitlines()
    assert f"version={tag[1:]}" in lines
    assert f"prerelease={prerelease}" in lines


@pytest.mark.parametrize("prerelease", ["true", "false"])
def test_github_release_marks_only_prereleases(prerelease: str, tmp_path: Path) -> None:
    import shutil

    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash unavailable")
    (step,) = [
        s
        for s in _workflow("release.yml")["jobs"]["github-release"]["steps"]
        if s.get("name") == "Create the release"
    ]
    assert step["env"]["PRERELEASE"] == "${{ needs.build.outputs.prerelease }}"
    output = tmp_path / "release_args"
    script = 'gh() { printf "%s\\n" "$@" > "$RELEASE_ARGS"; }\n' + step["run"]
    subprocess.run(  # noqa: S603 - workflow script with a local gh stub
        [bash, "-e", "-c", script],
        check=True,
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env={
            "GITHUB_REF_NAME": "v0.1.0rc1" if prerelease == "true" else "v0.1.0",
            "PRERELEASE": prerelease,
            "RELEASE_ARGS": str(output),
        },
        timeout=30,
    )
    args = output.read_text(encoding="utf-8").splitlines()
    assert args[:2] == ["release", "create"]
    assert ("--prerelease" in args) is (prerelease == "true")
    assert ("--latest=false" in args) is (prerelease == "true")


# -- deploy/: shipped IAM documents ----------------------------------------------


@pytest.mark.parametrize("name", ["sync-policy.json", "metadata-bucket-policy.json"])
def test_shipped_iam_documents_are_valid_policies(name: str) -> None:
    import json
    import re

    text = (ROOT / "deploy" / "iam" / name).read_text(encoding="utf-8")
    placeholders = set(re.findall(r"<([A-Z_]+)>", text))
    assert placeholders <= {
        "DATA_BUCKET",
        "DATA_PREFIX",
        "METADATA_BUCKET",
        "METADATA_PREFIX",
        "REGION",
        "ACCOUNT",
        "GLUE_DATABASE",
        "SYNC_ROLE",
    }
    policy = json.loads(text)
    assert policy["Version"] == "2012-10-17"
    for statement in policy["Statement"]:
        assert statement["Effect"] in ("Allow", "Deny")
        assert statement["Sid"]
        assert statement["Action"]


def test_sync_policy_never_allows_deleting_ducklake_data() -> None:
    import json

    policy = json.loads(
        (ROOT / "deploy" / "iam" / "sync-policy.json").read_text(encoding="utf-8")
    )
    data = "arn:aws:s3:::<DATA_BUCKET>/<DATA_PREFIX>/*"
    for statement in policy["Statement"]:
        resources = statement["Resource"]
        resources = [resources] if isinstance(resources, str) else resources
        if statement["Effect"] == "Allow" and data in resources:
            assert not {"s3:DeleteObject", "s3:DeleteObjectVersion"} & set(
                statement["Action"]
            )
    denied = [
        s
        for s in policy["Statement"]
        if s["Effect"] == "Deny" and s["Resource"] == data
    ]
    assert denied
    assert "s3:DeleteObject" in denied[0]["Action"]
