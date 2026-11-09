"""Check a release tag against ``pyproject.toml``.

Usage: ``python scripts/check_release_version.py v0.1.0 [--changelog CHANGELOG.md]``

Fails (exit 1) if the tag is not ``v<version>``, if the version does not follow
the versioning policy (``X.Y.Z`` with an optional ``aN``/``bN``/``rcN``
pre-release suffix) or, with ``--changelog``, if the changelog has no
``## [X.Y.Z]`` section for the version.
"""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path

VERSION_RE = re.compile(
    r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:(?:a|b|rc)[1-9]\d*)?"
)


def check(tag: str, pyproject: Path, changelog: Path | None = None) -> list[str]:
    """Return the list of errors; empty when the release is consistent."""
    project = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    version = str(project["project"]["version"])
    errors: list[str] = []
    match = re.fullmatch(r"v(.+)", tag)
    if match is None or match.group(1) != version:
        errors.append(f"release tag {tag!r} must match pyproject version v{version}")
    if VERSION_RE.fullmatch(version) is None:
        errors.append(
            f"pyproject version {version!r} must be X.Y.Z[aN|bN|rcN] "
            "(see the Releasing section of the README and CHANGELOG.md)"
        )
    if changelog is not None:
        heading = re.compile(rf"^## \[{re.escape(version)}\]", re.MULTILINE)
        if not changelog.is_file():
            errors.append(f"changelog {changelog} not found")
        elif heading.search(changelog.read_text(encoding="utf-8")) is None:
            errors.append(f"{changelog} has no '## [{version}]' section")
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("tag", nargs="?", default="")
    parser.add_argument("--pyproject", type=Path, default=Path("pyproject.toml"))
    parser.add_argument("--changelog", type=Path, default=None)
    args = parser.parse_args(argv)
    errors = check(args.tag, args.pyproject, args.changelog)
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        return 1
    print(f"release version {args.tag.removeprefix('v')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
