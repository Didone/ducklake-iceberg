"""Isolated cold-extension installation characterization; never reference latency.

Uses owned temporary extension directories and local simulated S3/Glue. Only
DuckDB's extension download may access the network; no real AWS account is used.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

import bench_sync_e2e as reference
import duckdb
import yaml


class InstallFixture(reference.Fixture):
    def prepare(self) -> None:
        super().prepare()
        self.extensions = self.root / "extensions"
        self.extensions.mkdir()
        config = yaml.safe_load(self.config.read_text())
        config["duckdb_extension_directory"] = str(self.extensions)
        self.config.write_text(yaml.safe_dump(config))

    def audit(self) -> dict[str, Any]:
        from ducklake_iceberg.deps import EXTENSION_VERSIONS

        result = super().audit()
        con = duckdb.connect(
            config={"threads": 2, "extension_directory": str(self.extensions)}
        )
        try:
            builds = dict(
                con.execute(
                    "SELECT extension_name, extension_version "
                    "FROM duckdb_extensions() WHERE installed"
                ).fetchall()
            )
        finally:
            con.close()
        result["installed_extension_builds"] = builds
        result["pins_match"] = builds.get("ducklake") == EXTENSION_VERSIONS["ducklake"]
        result["exact_values"] = result["exact_values"] and result["pins_match"]
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-exe", required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--output", type=Path, required=True)
    opts = parser.parse_args()
    opts.profile, opts.suite = "local-mock", "extension-install"
    opts.baseline_exe, opts.baseline_revision = (
        opts.candidate_exe,
        opts.candidate_revision,
    )
    opts.corrected_exe, opts.corrected_revision = None, None
    opts.samples, opts.blocks, opts.warmups, opts.seed, opts.scenarios = (
        1,
        1,
        0,
        42,
        ["I"],
    )
    reference.Fixture = InstallFixture
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    raise SystemExit(reference.run(opts))


if __name__ == "__main__":
    main()
