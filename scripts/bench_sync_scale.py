"""Separate local scale characterization with retained-snapshot value audits.

Run after reference measurements. Preparation uses warm in-process syncs;
the characterized sync itself always starts a fresh candidate CLI process.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Any

from bench_sync_e2e import Fixture, digest
from bench_sync_support import append_sample, measure, write_json


class ScaleFixture(Fixture):
    def __init__(
        self,
        root: Path,
        endpoint: str,
        executable: str,
        files: int,
        history: int,
        rows: int = 100_000,
        initial: bool = False,
        delta: bool = False,
    ) -> None:
        super().__init__(root, endpoint, "I" if initial else "N", executable)
        self.files, self.history, self.rows, self.initial = (
            files,
            history,
            rows,
            initial,
        )
        self.expected_snapshots: dict[int, str] = {}
        self.delta = delta

    def _seed(self) -> None:
        con = self.connect()
        try:
            con.execute("CALL lake.set_option('data_inlining_row_limit', 0)")
            con.execute(
                f"CREATE TABLE lake.main.{self.table} (id BIGINT, p INTEGER, "
                "payload VARCHAR, amount DECIMAL(12,2), "
                "created TIMESTAMP, enabled BOOLEAN)"
            )
            con.execute(f"ALTER TABLE lake.main.{self.table} SET PARTITIONED BY (p)")
            con.execute(self.insert(0, 1, 0))
            for batch in range(self.files - 1):
                lo = 1 + (self.rows - 1) * batch // (self.files - 1)
                hi = 1 + (self.rows - 1) * (batch + 1) // (self.files - 1)
                con.execute(self.insert(lo, hi, batch * 4 // (self.files - 1)))
        finally:
            con.close()

    def current_table(self) -> Any:
        from pyiceberg.catalog.glue import GlueCatalog

        catalog = GlueCatalog(
            "benchmark",
            self.glue,
            **{
                "glue.region": "us-east-1",
                "s3.endpoint": self.endpoint,
                "s3.access-key-id": "testing",
                "s3.secret-access-key": "testing",
            },
        )
        return catalog.load_table(("benchmark", self.table))

    def prepare(self) -> None:
        from typer.testing import CliRunner

        from ducklake_iceberg.cli import app

        self._seed()
        self.base_shape = self.shape()
        self.base_digest = digest(self.query(f"SELECT * FROM lake.main.{self.table}"))
        if not self.initial:
            initial = measure(self.cmd, 0)
            if not initial["correct"]:
                raise ValueError("base publication failed")
            table = self.current_table()
            self.expected_snapshots[table.current_snapshot().snapshot_id] = (
                self.base_digest
            )
            for index in range(1, self.history):
                con = self.connect()
                try:
                    con.execute(
                        f"UPDATE lake.main.{self.table} "
                        "SET payload=lpad($value,64,'h'), "
                        "amount=$amount WHERE id=0",
                        {"value": str(index), "amount": index},
                    )
                finally:
                    con.close()
                result = CliRunner().invoke(app, self.cmd[1:])
                if result.exit_code != 0:
                    raise ValueError("untimed history publication failed")
                expected = digest(self.query(f"SELECT * FROM lake.main.{self.table}"))
                table = self.current_table()
                self.expected_snapshots[table.current_snapshot().snapshot_id] = expected
            if len(table.snapshots()) != self.history:
                raise ValueError(
                    "published snapshot count does not match requested history"
                )
            if len(list(table.scan().plan_files())) != self.files:
                raise ValueError("live file count does not match requested scale")
        if self.delta:
            con = self.connect()
            try:
                con.execute(self.insert(self.rows, self.rows + 1000, 0))
            finally:
                con.close()
        self.expected_digest = digest(
            self.query(f"SELECT * FROM lake.main.{self.table}")
        )
        self.before = self.source_hashes()
        self.before_objects = self.objects()
        self.before_glue = self.pointer()
        self.base_manifests = (
            len(
                self.current_table()
                .current_snapshot()
                .manifests(self.current_table().io)
            )
            if not self.initial
            else 0
        )
        self.fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "files": self.files,
                    "history": self.history,
                    "rows": self.rows,
                    "values": self.expected_digest,
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()

    def audit(self) -> dict[str, Any]:
        table = self.current_table()
        rows = table.scan().to_arrow().to_pylist()
        actual = digest(rows)
        retained = all(
            digest(table.scan(snapshot_id=snapshot.snapshot_id).to_arrow().to_pylist())
            == self.expected_snapshots.get(snapshot.snapshot_id, self.expected_digest)
            for snapshot in table.snapshots()
        )
        source_paths = {str(path) for path in self.data.rglob("*.parquet")}
        return {
            "exact_values": actual == self.expected_digest,
            "expected_values_sha256": self.expected_digest,
            "actual_values_sha256": actual,
            "retained_snapshots_readable": retained,
            "source_intact": self.before == self.source_hashes(),
            "zero_copy": all(
                task.file.file_path in source_paths
                for task in table.scan().plan_files()
            ),
            "live_files": len(list(table.scan().plan_files())),
            "snapshots": len(table.snapshots()),
            "rows": len(rows),
            "shape_matches": len(rows) == self.rows + (1000 if self.delta else 0)
            and len(list(table.scan().plan_files())) == self.files + int(self.delta)
            and len(table.snapshots()) == self.history + int(self.delta),
            "noop_unchanged": self.initial
            or self.delta
            or (
                self.before_objects == self.objects()
                and self.before_glue == self.pointer()
            ),
        }


def run(opts: argparse.Namespace) -> int:
    import boto3
    from bench_sync_support import LocalHttpObserver
    from moto.server import ThreadedMotoServer

    opts.output.mkdir(parents=True, exist_ok=False)
    harness = opts.output / "harness"
    harness.mkdir()
    harness_hashes = {}
    for name in ("bench_sync_scale.py", "bench_sync_e2e.py", "bench_sync_support.py"):
        source = Path(__file__).with_name(name)
        shutil.copy2(source, harness / name)
        harness_hashes[name] = hashlib.sha256(source.read_bytes()).hexdigest()
    write_json(
        opts.output / "manifest.json",
        {
            "schema_version": 1,
            "suite": "scale-characterization",
            "revision": opts.candidate_revision,
            "profile": "local-mock",
            "samples_per_case": 1,
            "files": opts.files,
            "published_snapshots": opts.history,
            "scenario": "A" if opts.delta else "N",
            "harness_sha256": harness_hashes,
            "note": "characterization only; not a reference latency cohort",
        },
    )
    keys = {
        key
        for key in os.environ
        if key.startswith("DUCKLAKE_ICEBERG_") or key.startswith("AWS_")
    } | {"PYICEBERG_MAX_WORKERS", "DUCKLAKE_ICEBERG_STATE_DIR"}
    old = {key: os.environ.get(key) for key in keys}
    old_session = boto3.DEFAULT_SESSION
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    samples: list[dict[str, Any]] = []
    try:
        server.start()
        assert server._server is not None
        observer = LocalHttpObserver(server._server.app)
        server._server.app = observer
        host, port = server.get_host_and_port()
        endpoint = f"http://{host}:{port}"
        for key in keys:
            os.environ.pop(key, None)
        os.environ.update(
            {
                "AWS_ACCESS_KEY_ID": "testing",
                "AWS_SECRET_ACCESS_KEY": "testing",
                "AWS_DEFAULT_REGION": "us-east-1",
                "AWS_ENDPOINT_URL": endpoint,
                "AWS_ENDPOINT_URL_S3": endpoint,
                "AWS_ENDPOINT_URL_GLUE": endpoint,
                "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "false",
                "AWS_EC2_METADATA_DISABLED": "true",
                "PYICEBERG_MAX_WORKERS": "8",
            }
        )
        boto3.DEFAULT_SESSION = None
        boto3.client(
            "s3", endpoint_url=endpoint, region_name="us-east-1"
        ).create_bucket(Bucket="benchmark-metadata")
        boto3.client(
            "glue", endpoint_url=endpoint, region_name="us-east-1"
        ).create_database(DatabaseInput={"Name": "benchmark"})
        cases = [
            (files, history, 100_000, False)
            for files in opts.files
            for history in opts.history
        ]
        if opts.large_initial:
            cases.append((80, 1, 1_000_000, True))
        with tempfile.TemporaryDirectory(prefix="sync-scale-") as workspace:
            for files, history, rows, initial in cases:
                identifier = uuid.uuid4().hex
                root = Path(workspace) / identifier
                root.mkdir()
                os.environ["DUCKLAKE_ICEBERG_STATE_DIR"] = str(root / "state")
                fixture = None
                sample: dict[str, Any] = {
                    "id": identifier,
                    "files": files,
                    "history": history,
                    "rows": rows,
                    "initial": initial,
                    "scenario": "I" if initial else ("A" if opts.delta else "N"),
                }
                try:
                    fixture = ScaleFixture(
                        root,
                        endpoint,
                        opts.candidate_exe,
                        files,
                        history,
                        rows,
                        initial,
                        opts.delta and not initial,
                    )
                    fixture.prepare()
                    observer.begin()
                    sample.update(
                        measure(
                            fixture.cmd, 0 if initial or opts.delta else 4, opts.timeout
                        )
                    )
                    sample["server_http"] = observer.end()
                    if sample["correct"]:
                        audit = fixture.audit()
                        sample["validation"] = audit
                        sample["correct"] = all(
                            audit[key]
                            for key in (
                                "exact_values",
                                "retained_snapshots_readable",
                                "source_intact",
                                "zero_copy",
                                "noop_unchanged",
                                "shape_matches",
                            )
                        )
                except Exception as exc:
                    sample.update(correct=False, error=type(exc).__name__)
                finally:
                    observer.end()
                    append_sample(opts.output / "samples.jsonl", sample)
                    samples.append(sample)
                    try:
                        if fixture is not None:
                            fixture.cleanup()
                    finally:
                        shutil.rmtree(root)
                print(
                    f"files={files} snapshots={history} rows={rows}: "
                    f"{sample['correct']}",
                    flush=True,
                )
    finally:
        server.stop()
        boto3.DEFAULT_SESSION = old_session
        for key, value in old.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        write_json(opts.output / "summary.json", samples)
    return int(not samples or any(not sample["correct"] for sample in samples))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-exe", required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--files", nargs="+", type=int, default=[8, 64, 1024])
    parser.add_argument("--history", nargs="+", type=int, default=[1, 32, 256])
    parser.add_argument("--large-initial", action="store_true")
    parser.add_argument(
        "--delta", action="store_true", help="Time a 1000-row append instead of no-op"
    )
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--output", type=Path, required=True)
    opts = parser.parse_args()
    if any(value < 1 for value in opts.history) or any(
        value < 8 for value in opts.files
    ):
        parser.error("positive history and at least eight files required")
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    raise SystemExit(run(opts))


if __name__ == "__main__":
    main()
