"""Fresh-process local sync comparison with isolated reference fixtures.

This runner never provisions AWS resources. local-mock uses a loopback moto
server for Glue/S3 and real SQLite/DuckLake/Parquet/CLI processes. Neither its
latency nor its simulated S3 requests establishes a real-AWS performance gate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import platform
import random
import shutil
import tempfile
import uuid
from collections import defaultdict
from contextlib import suppress
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any

import duckdb
import yaml
from bench_sync_support import (
    LocalHttpObserver,
    append_sample,
    compare,
    measure,
    operation_budgets,
    summarize,
    write_json,
)

SCENARIOS = {
    "N": (100_000, 4),
    "I": (100_000, 0),
    "A": (101_000, 0),
    "F": (100_100, 0),
    "D": (99_900, 0),
    "U": (100_000, 0),
}


def digest(rows: list[dict[str, Any]]) -> str:
    ordered = sorted(rows, key=lambda row: row["id"])
    return hashlib.sha256(
        json.dumps(ordered, sort_keys=True, default=str).encode()
    ).hexdigest()


class Fixture:
    """Independent catalog and target for every attempt, prepared outside timing."""

    def __init__(
        self, root: Path, endpoint: str, scenario: str, executable: str
    ) -> None:
        import boto3

        self.root, self.scenario = root, scenario
        self.table = "b_" + uuid.uuid4().hex
        self.endpoint = endpoint
        self.data = root / "data"
        self.data.mkdir()
        self.config = root / "config.yaml"
        self.config.write_text(
            yaml.safe_dump(
                {
                    "catalog_dsn": f"ducklake:sqlite:{root}/catalog.sqlite",
                    "data_path": str(self.data) + "/",
                    "glue_database": "benchmark",
                    "iceberg_metadata_prefix": "s3://benchmark-metadata/" + self.table,
                    "s3_endpoint": endpoint,
                    "aws_region": "us-east-1",
                    "duckdb_threads": 2,
                    "stats_source": "catalog",
                    "max_footer_workers": 8,
                    "max_io_workers": 8,
                    "max_total_workers": 32,
                }
            )
        )
        self.cmd = [
            executable,
            "sync",
            "--table",
            "main." + self.table,
            "--config",
            str(self.config),
            "--json",
        ]
        self.glue = boto3.client("glue", region_name="us-east-1", endpoint_url=endpoint)
        self.s3 = boto3.client("s3", region_name="us-east-1", endpoint_url=endpoint)

    def prepare(self) -> None:
        scenario = self.scenario
        self._seed()
        self.base_shape = self.shape()
        self.base_digest = digest(self.query(f"SELECT * FROM lake.main.{self.table}"))
        if (
            self.base_shape["files"] != 8
            or self.base_shape["rows"] != 100_000
            or self.base_shape["records_per_file"] != [12_500] * 8
            or sorted(self.base_shape["partition_file_counts"]) != [2] * 4
        ):
            raise ValueError("fixture must have eight physical files and 100000 rows")
        if scenario != "I":
            initial = measure(self.cmd, 0)
            if not initial["correct"]:
                raise ValueError(
                    "untimed base publication failed: " + str(initial["error"])
                )
            self._mutate()
        self.expected_digest = digest(
            self.query(f"SELECT * FROM lake.main.{self.table}")
        )
        self.before = self.source_hashes()
        self.before_objects = self.objects()
        self.before_glue = self.pointer()
        self.base_manifests = (
            sum(key.endswith(".avro") for key in self.before_objects) - 1
            if scenario != "I"
            else 0
        )
        self.fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "scenario": scenario,
                    "base_shape": self.base_shape,
                    "expected_digest": self.expected_digest,
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()

    def connect(self, *, read_only: bool = False) -> Any:
        con = duckdb.connect(config={"threads": 2})
        try:
            con.execute("LOAD ducklake")
            con.execute("LOAD sqlite")
            con.execute(
                f"ATTACH 'ducklake:sqlite:{self.root}/catalog.sqlite' AS lake "
                "(METADATA_CATALOG 'lake_meta', "
                + ("READ_ONLY)" if read_only else f"DATA_PATH '{self.data}/')")
            )
        except BaseException:
            con.close()
            raise
        return con

    def query(self, sql: str) -> list[dict[str, Any]]:
        con = self.connect(read_only=True)
        try:
            return con.sql(sql).to_arrow_table().to_pylist()
        finally:
            con.close()

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
            for batch in range(8):
                lo = batch * 12_500
                con.execute(self.insert(lo, lo + 12_500, batch // 2))
        finally:
            con.close()

    def insert(self, start: int, end: int, partition: int) -> str:
        return (
            f"INSERT INTO lake.main.{self.table} SELECT id, {partition}, "
            "CASE WHEN id%17=0 THEN NULL ELSE repeat(md5(id::VARCHAR),2) END, "
            "CASE WHEN id%19=0 THEN NULL ELSE (id%10000)::DECIMAL(12,2)/100 END, "
            "CASE WHEN id%23=0 THEN NULL ELSE TIMESTAMP '2024-01-01' + "
            "id * INTERVAL 1 MICROSECOND END, "
            "CASE WHEN id%29=0 THEN NULL ELSE id%2=0 END "
            f"FROM range({start},{end}) t(id)"
        )

    def _mutate(self) -> None:
        if self.scenario == "N":
            return
        con = self.connect()
        try:
            if self.scenario == "F":
                con.execute("CALL lake.set_option('data_inlining_row_limit', 1000)")
                con.execute(self.insert(100_000, 100_100, 0))
            elif self.scenario == "A":
                con.execute(self.insert(100_000, 101_000, 0))
            elif self.scenario == "D":
                con.execute(f"DELETE FROM lake.main.{self.table} WHERE id < 100")
            elif self.scenario == "U":
                con.execute(
                    f"UPDATE lake.main.{self.table} SET payload = repeat('u',64), "
                    "amount = 42.25 WHERE id < 100"
                )
        finally:
            con.close()
        shape = self.shape()
        if self.scenario in ("D", "U") and shape["delete_files"] < 1:
            raise ValueError("delete/update fixture must use physical deletes")
        if self.scenario == "A" and shape["files"] != 9:
            raise ValueError("append fixture must add one file")

    def shape(self) -> dict[str, Any]:
        rows = self.query(
            "SELECT file_size_bytes, record_count FROM lake_meta.ducklake_data_file "
            "WHERE end_snapshot IS NULL"
        )
        deletes = self.query(
            "SELECT count(*) n FROM lake_meta.ducklake_delete_file "
            "WHERE end_snapshot IS NULL"
        )
        if any(row["file_size_bytes"] > 16 * 1024**2 for row in rows):
            raise ValueError("reference file exceeds 16 MiB")
        partitions = self.query(
            "SELECT partition_value, count(*) n "
            "FROM lake_meta.ducklake_file_partition_value "
            "GROUP BY partition_value"
        )
        return {
            "records_per_file": sorted(row["record_count"] for row in rows),
            "partition_file_counts": sorted(row["n"] for row in partitions),
            "files": len(rows),
            "rows": sum(row["record_count"] for row in rows),
            "delete_files": deletes[0]["n"],
            "sizes": sorted(row["file_size_bytes"] for row in rows),
        }

    def source_hashes(self) -> dict[str, str]:
        return {
            str(p.relative_to(self.data)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in self.data.rglob("*.parquet")
        }

    def objects(self) -> dict[str, str]:
        prefix = self.table + "/"
        objects = {}
        for page in self.s3.get_paginator("list_objects_v2").paginate(
            Bucket="benchmark-metadata", Prefix=prefix
        ):
            objects.update(
                {obj["Key"]: obj["ETag"] for obj in page.get("Contents", [])}
            )
        return objects

    def pointer(self) -> dict[str, Any] | None:
        try:
            table = self.glue.get_table(DatabaseName="benchmark", Name=self.table)[
                "Table"
            ]
            return {
                "version": table.get("VersionId"),
                "metadata": table["Parameters"].get("metadata_location"),
            }
        except self.glue.exceptions.EntityNotFoundException:
            return None

    def audit(self) -> dict[str, Any]:
        from pyiceberg.catalog.glue import GlueCatalog

        catalog = GlueCatalog(
            "benchmark",
            self.glue,
            **{
                "type": "glue",
                "glue.region": "us-east-1",
                "glue.endpoint": self.endpoint,
                "s3.endpoint": self.endpoint,
                "s3.access-key-id": "testing",
                "s3.secret-access-key": "testing",
            },
        )
        table = catalog.load_table(("benchmark", self.table))
        actual = table.scan().to_arrow().to_pylist()
        independent: str = "unverified"
        reader = duckdb.connect(
            config={
                "threads": 2,
                "autoinstall_known_extensions": False,
                "autoload_known_extensions": False,
            }
        )
        try:
            reader.execute("LOAD httpfs")
            reader.execute("LOAD iceberg")
            reader.execute(
                "CREATE SECRET bench (TYPE s3, KEY_ID 'testing', SECRET 'testing', "
                "REGION 'us-east-1', URL_STYLE 'path', "
                "USE_SSL false, ENDPOINT $endpoint)",
                {"endpoint": self.endpoint.removeprefix("http://")},
            )
            rows = (
                reader.sql(
                    "SELECT * FROM iceberg_scan($metadata)",
                    params={"metadata": table.metadata_location},
                )
                .to_arrow_table()
                .to_pylist()
            )
            independent = "pass" if digest(rows) == self.expected_digest else "fail"
        except duckdb.Error:
            independent = "unverified"
        finally:
            reader.close()
        source_fields = self.query(
            "SELECT column_name, column_id FROM lake_meta.ducklake_column "
            "WHERE end_snapshot IS NULL ORDER BY column_order"
        )
        field_ids_match = {
            row["column_name"]: row["column_id"] for row in source_fields
        } == {field.name: field.field_id for field in table.schema().fields}
        partition_matches = len(table.spec().fields) == 1 and all(
            str(field.transform) == "identity"
            and field.source_id == table.schema().find_field("p").field_id
            for field in table.spec().fields
        )
        source_uuid = str(
            self.query(
                "SELECT table_uuid FROM lake_meta.ducklake_table "
                "WHERE end_snapshot IS NULL"
            )[0]["table_uuid"]
        )
        identity_matches = (
            table.metadata.properties.get("ducklake.table-uuid") == source_uuid
        )
        after = self.source_hashes()
        source_intact = all(after.get(k) == v for k, v in self.before.items())
        retained_ok = all(
            digest(table.scan(snapshot_id=snapshot.snapshot_id).to_arrow().to_pylist())
            == self.base_digest
            for snapshot in table.snapshots()[:-1]
        )
        source_paths = {str(p) for p in self.data.rglob("*.parquet")}
        zero_copy = all(
            task.file.file_path in source_paths for task in table.scan().plan_files()
        )
        added = set(after) - set(self.before)
        source_ok = source_intact and len(added) == (1 if self.scenario == "F" else 0)
        noop_ok = self.scenario != "N" or (
            self.before_objects == self.objects() and self.before_glue == self.pointer()
        )
        shape = self.shape()
        return {
            "exact_values": digest(actual) == self.expected_digest,
            "expected_values_sha256": self.expected_digest,
            "actual_values_sha256": digest(actual),
            "source_hashes_before": {
                hashlib.sha256(name.encode()).hexdigest(): value
                for name, value in self.before.items()
            },
            "source_hashes_after": {
                hashlib.sha256(name.encode()).hexdigest(): value
                for name, value in after.items()
            },
            "artifact_before_sha256": hashlib.sha256(
                json.dumps(self.before_objects, sort_keys=True).encode()
            ).hexdigest(),
            "artifact_after_sha256": hashlib.sha256(
                json.dumps(self.objects(), sort_keys=True).encode()
            ).hexdigest(),
            "glue_before_sha256": hashlib.sha256(
                json.dumps(self.before_glue, sort_keys=True).encode()
            ).hexdigest(),
            "glue_after_sha256": hashlib.sha256(
                json.dumps(self.pointer(), sort_keys=True).encode()
            ).hexdigest(),
            "retained_snapshots_readable": retained_ok,
            "zero_copy": zero_copy,
            "field_ids_match": field_ids_match,
            "partition_matches": partition_matches,
            "source_identity_matches": identity_matches,
            "rows": len(actual),
            "expected_rows": SCENARIOS[self.scenario][0],
            "source_intact": source_ok,
            "noop_unchanged": noop_ok,
            "field_ids": [field.field_id for field in table.schema().fields],
            "format_version": table.metadata.format_version,
            "shape": shape,
            "independent_duckdb_reader": independent,
        }

    def cleanup(self) -> None:
        # Only local simulated resources, after audit, outside timing.
        with suppress(self.glue.exceptions.EntityNotFoundException):
            self.glue.delete_table(DatabaseName="benchmark", Name=self.table)
        objects = self.objects()
        for key in objects:
            self.s3.delete_object(Bucket="benchmark-metadata", Key=key)
        self.s3.close()
        self.glue.close()


def run(opts: argparse.Namespace) -> int:
    import boto3
    from moto.server import ThreadedMotoServer

    output = opts.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    samples: list[dict[str, Any]] = []
    manifest = {
        "schema_version": 1,
        "started_at": datetime.now(UTC).isoformat(),
        "scenarios": opts.scenarios,
        "credential_provider": "static simulated environment",
        "source_backend": "sqlite/local Parquet",
        "profile": opts.profile,
        "suite": opts.suite,
        "seed": opts.seed,
        "warmups": opts.warmups,
        "samples": opts.samples,
        "blocks": opts.blocks,
        "timeout_s": opts.timeout,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "dependencies": {
            name: version(name)
            for name in (
                "duckdb",
                "pyiceberg",
                "pyarrow",
                "boto3",
                "botocore",
                "moto",
                "pydantic",
                "typer",
                "fastavro",
            )
        },
        "revisions": {
            "baseline": opts.baseline_revision,
            "candidate": opts.candidate_revision,
            "corrected": opts.corrected_revision,
        },
        "physical_native_requests": "loopback server observations",
        "aws_latency": "unverified",
        "environment": "local process; real SQLite; simulated S3/Glue",
        "cpu_count": os.cpu_count(),
        "duckdb_threads": 2,
        "footer_workers": 8,
        "io_workers": 8,
        "global_workers": 32,
        "pyiceberg_workers": 8,
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "support_sha256": hashlib.sha256(
            Path(__file__).with_name("bench_sync_support.py").read_bytes()
        ).hexdigest(),
        "executables_sha256": {
            name: hashlib.sha256(Path(executable).read_bytes()).hexdigest()
            for name, executable in (
                ("baseline", opts.baseline_exe),
                ("candidate", opts.candidate_exe),
                ("corrected", opts.corrected_exe),
            )
            if executable is not None
        },
        "cache_state": "fresh child; warm OS/database caches allowed",
    }
    probe = duckdb.connect(config={"threads": 2})
    try:
        manifest["extension_builds"] = {
            name: {"installed": installed, "version": build}
            for name, installed, build in probe.execute(
                "SELECT extension_name, installed, extension_version "
                "FROM duckdb_extensions() "
                "WHERE extension_name IN ('ducklake','sqlite_scanner',"
                "'postgres_scanner',"
                "'httpfs','aws','iceberg')"
            ).fetchall()
        }
    finally:
        probe.close()
    for name, filename in (("cpu_limit", "cpu.max"), ("memory_limit", "memory.max")):
        limit = Path("/sys/fs/cgroup") / filename
        manifest[name] = limit.read_text().strip() if limit.is_file() else "unknown"
    write_json(output / "manifest.json", manifest)
    validation: dict[str, Any] = {
        f"AC-{i}": {"status": "unverified"} for i in range(1, 17)
    }
    write_json(output / "validation.json", validation)
    required = ("ducklake", "sqlite_scanner", "httpfs", "aws", "iceberg")
    if any(
        not manifest["extension_builds"].get(name, {}).get("installed")
        for name in required
    ):
        (output / "samples.jsonl").touch()
        write_json(
            output / "summary.json",
            {"groups": {}, "error": "missing_installed_extensions"},
        )
        (output / "report.md").write_text(
            "# Unverified\n\nRequired preinstalled extensions unavailable.\n"
        )
        return 1
    rng = random.Random(opts.seed)  # noqa: S311 - deterministic benchmark order
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    isolated_keys = {
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_DEFAULT_REGION",
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_GLUE",
        "AWS_ENDPOINT_URL_S3",
        "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS",
        "AWS_EC2_METADATA_DISABLED",
        "PYICEBERG_MAX_WORKERS",
        "DUCKLAKE_ICEBERG_STATE_DIR",
    } | {key for key in os.environ if key.startswith("DUCKLAKE_ICEBERG_")}
    old_env = {key: os.environ.get(key) for key in isolated_keys}
    try:
        server.start()
        assert server._server is not None
        observer = LocalHttpObserver(server._server.app)
        server._server.app = observer
        host, port = server.get_host_and_port()
        endpoint = f"http://{host}:{port}"
        for key in isolated_keys:
            os.environ.pop(key, None)
        os.environ.update(
            AWS_ENDPOINT_URL=endpoint,
            AWS_ENDPOINT_URL_S3=endpoint,
            AWS_IGNORE_CONFIGURED_ENDPOINT_URLS="false",
            PYICEBERG_MAX_WORKERS="8",
            AWS_ACCESS_KEY_ID="testing",
            AWS_SECRET_ACCESS_KEY="testing",  # noqa: S106 - loopback moto only
            AWS_DEFAULT_REGION="us-east-1",
            AWS_ENDPOINT_URL_GLUE=endpoint,
            AWS_EC2_METADATA_DISABLED="true",
        )
        os.environ.pop("AWS_SESSION_TOKEN", None)
        os.environ.pop("AWS_PROFILE", None)
        boto3.client(
            "s3", endpoint_url=endpoint, region_name="us-east-1"
        ).create_bucket(Bucket="benchmark-metadata")
        boto3.client(
            "glue", endpoint_url=endpoint, region_name="us-east-1"
        ).create_database(DatabaseInput={"Name": "benchmark"})
        with tempfile.TemporaryDirectory(prefix="sync-bench-") as workspace:
            for scenario in opts.scenarios:
                for index in range(-opts.warmups, opts.samples):
                    revisions = ["baseline", "candidate"]
                    if opts.corrected_exe is not None:
                        revisions.append("corrected")
                    rng.shuffle(revisions)
                    for revision in revisions:
                        sample = {
                            "id": uuid.uuid4().hex,
                            "timestamp": datetime.now(UTC).isoformat(),
                            "expected_exit": SCENARIOS[scenario][1],
                            "scenario": scenario,
                            "revision": revision,
                            "warmup": index < 0,
                            "block": max(index, 0) // (opts.samples // opts.blocks),
                            "index": index,
                            "excluded": False,
                        }
                        root = Path(workspace) / sample["id"]
                        root.mkdir()
                        os.environ["DUCKLAKE_ICEBERG_STATE_DIR"] = str(root / "state")
                        fixture = None
                        try:
                            fixture = Fixture(
                                root,
                                endpoint,
                                scenario,
                                getattr(opts, revision + "_exe"),
                            )
                            fixture.prepare()
                            sample["config_sha256"] = hashlib.sha256(
                                fixture.config.read_bytes()
                            ).hexdigest()
                            sample["fixture_fingerprint"] = fixture.fingerprint
                            observer.begin()
                            sample.update(
                                measure(
                                    fixture.cmd, SCENARIOS[scenario][1], opts.timeout
                                )
                            )
                            sample["server_http"] = observer.end()
                            server_s3 = sum(
                                count
                                for name, count in sample["server_http"].items()
                                if name.startswith("s3.")
                            )
                            sdk_s3 = sum(
                                count
                                for name, count in sample.get("counters", {}).items()
                                if name.startswith("aws.http_attempts.s3.")
                            )
                            sample["native_http_attempts"] = (
                                server_s3 - sdk_s3
                                if sample.get("sdk_http_attempts") is not None
                                and server_s3 >= sdk_s3
                                else None
                            )
                            sample["budget"] = operation_budgets(
                                sample,
                                fixture.base_manifests,
                                fixture.shape()["delete_files"]
                                if scenario in ("D", "U")
                                else 0,
                            )
                            if sample["correct"]:
                                audit = fixture.audit()
                                sample["validation"] = audit
                                sample["correct"] = (
                                    all(
                                        audit[k]
                                        for k in (
                                            "exact_values",
                                            "source_intact",
                                            "noop_unchanged",
                                            "retained_snapshots_readable",
                                            "zero_copy",
                                            "field_ids_match",
                                            "partition_matches",
                                            "source_identity_matches",
                                        )
                                    )
                                    and audit["rows"] == audit["expected_rows"]
                                    and audit["independent_duckdb_reader"] != "fail"
                                )
                                if not sample["correct"]:
                                    sample["error"] = (
                                        "reader_or_side_effect_audit_failed"
                                    )
                        except KeyboardInterrupt:
                            sample.update(correct=False, error="interrupted")
                            sample.setdefault("actual_exit", None)
                            sample.setdefault("timeout", False)
                            raise
                        except Exception as exc:
                            # Exceptions may contain credentials or payloads.
                            sample.update(
                                correct=False,
                                error="fixture_or_audit_" + type(exc).__name__,
                            )
                            sample.setdefault("actual_exit", None)
                            sample.setdefault("timeout", False)
                        finally:
                            observer.end()
                            append_sample(output / "samples.jsonl", sample)
                            samples.append(sample)
                            try:
                                if fixture is not None:
                                    fixture.cleanup()
                            finally:
                                shutil.rmtree(root)
                        status = "ok" if sample["correct"] else sample["error"]
                        print(f"{scenario} {revision} {index}: {status}", flush=True)
    finally:
        server.stop()
        for key, value in old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for sample in samples:
            groups[sample["scenario"] + "." + sample["revision"]].append(sample)
        summary = {name: summarize(group, opts.seed) for name, group in groups.items()}
        comparison = {
            scenario: compare(
                summary[scenario + ".baseline"],
                summary[scenario + ".candidate"],
                scenario,
            )
            for scenario in opts.scenarios
            if scenario + ".baseline" in summary and scenario + ".candidate" in summary
        }
        corrected_comparison = {
            scenario: compare(
                summary[scenario + ".corrected"],
                summary[scenario + ".candidate"],
                scenario,
            )
            for scenario in opts.scenarios
            if scenario + ".corrected" in summary and scenario + ".candidate" in summary
        }
        write_json(
            output / "summary.json",
            {
                "groups": summary,
                "comparison": comparison,
                "corrected_comparison": corrected_comparison,
            },
        )
        (output / "report.md").write_text(
            "# Local benchmark\n\nSimulated S3/Glue; "
            "AWS and native request budgets unverified.\n\n"
            + "\n".join(
                f"- {name}: {value['correct']}/{value['admitted']} correct; "
                "mean/p95 ms: "
                f"{value['latency_ms']['mean']}/{value['latency_ms']['p95']}"
                for name, value in summary.items()
            )
            + "\n\n## Local comparison\n\n"
            + "\n".join(
                f"- {scenario}: mean gain {value['mean'].get('gain_percent')}%; "
                f"p95 gain {value['p95'].get('gain_percent')}%"
                for scenario, value in comparison.items()
            )
            + "\n"
        )
    return int(any(not s["correct"] for s in samples) or not samples)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=["local-mock"], default="local-mock")
    parser.add_argument("--suite", choices=["reference"], default="reference")
    parser.add_argument("--baseline-exe", required=True)
    parser.add_argument("--candidate-exe", required=True)
    parser.add_argument("--corrected-exe")
    parser.add_argument("--corrected-revision")
    parser.add_argument("--baseline-revision", required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument(
        "--scenarios", nargs="+", choices=list(SCENARIOS), default=list(SCENARIOS)
    )
    parser.add_argument("--warmups", type=int, default=5)
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--blocks", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--output", type=Path, required=True)
    opts = parser.parse_args()
    if (
        opts.samples < 1
        or opts.blocks < 1
        or opts.samples % opts.blocks
        or opts.warmups < 0
        or opts.timeout <= 0
    ):
        parser.error(
            "positive samples/blocks/timeout, divisible samples, "
            "nonnegative warmups required"
        )
    if bool(opts.corrected_exe) != bool(opts.corrected_revision):
        parser.error("corrected executable and revision must be supplied together")
    for executable in filter(
        None, (opts.baseline_exe, opts.candidate_exe, opts.corrected_exe)
    ):
        if not Path(executable).is_absolute() or not os.access(executable, os.X_OK):
            parser.error("executables must be absolute executable paths")
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    raise SystemExit(run(opts))


if __name__ == "__main__":
    main()
