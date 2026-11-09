"""Write amplification of pipeline A, measured locally.

Simulates the shape of the PoC S1 CDC load (table ``pedidos``, 85/13/2 mix of
inserts/updates/deletes, updates concentrated on recent ids, batches applied
with DELETE + MERGE like ``lambda/consumer.py``, inlining on) against a local
DuckLake (SQLite catalog, local ``data_path``) and publishes with the real
``iceduck sync`` to a local Iceberg prefix, using moto's Glue (as in
``tests/integration/test_sync_e2e.py``). No call goes out to AWS.

Each variant runs in its own process (clean process caches and mock) and
combines:

- ``parquet_compression``: ``snappy`` or ``zstd`` (option persisted in the
  catalog before the initial load; it also applies to flush/merge/rewrite);
- maintenance: ``sync --compact`` (expire/merge/rewrite, like the PoC's hourly
  checkpoint) or ``sync --compact --materialize-deletes
  --delete-threshold 0`` (periodic materialization, gives up zero-copy on the
  rewritten files).

The bytes counted are those of every new file that shows up under the
``data_path`` and the Iceberg prefix after the initial load and the first sync
(nothing is deleted during the run: there is no ``cleanup_old_files`` or
``iceduck clean``), by class: DuckLake Parquet data, DuckLake delete files,
Iceberg metadata (``*.metadata.json`` and Avro) and Iceberg position deletes.
The denominator is the number of generated changes (rows affected by the
generator's intent).

At the end, the final state is rewritten outside DuckLake as a plain ZSTD
Parquet (pyarrow, no internal columns) as a **local stand-in** for the Spark
batch baseline: it is not Spark, it only gives the order of magnitude of bytes
per row of a ZSTD Parquet of the same rows.

    python scripts/bench_write_amplification.py --initial-rows 200000 \\
        --batches 240 --changes-per-batch 2000

To quickly see the variants and the output format: ``--batches 30
--initial-rows 20000``. ``--json`` prints the raw result.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

TABLE = "raw.pedidos"
INTERNAL_PREFIX = "_ducklake_internal_"
STATUS = ("pago", "separado", "enviado", "entregue")
#: name -> (parquet_compression, periodic materialization, parquet_version)
VARIANTS: dict[str, tuple[str, bool, int | None]] = {
    "snappy": ("snappy", False, None),
    "zstd": ("zstd", False, None),
    "snappy+materialize": ("snappy", True, None),
    "zstd+materialize": ("zstd", True, None),
    # informational: DELTA_BINARY_PACKED on integers; reading from Athena,
    # Snowflake and Redshift Spectrum not verified
    "zstd+parquet_v2": ("zstd", False, 2),
}
START = datetime(2026, 11, 1)
CLASSES = ("data", "ducklake_deletes", "iceberg_metadata", "iceberg_deletes")


@dataclass(slots=True)
class Params:
    initial_rows: int
    batches: int
    changes_per_batch: int
    mix: tuple[float, float, float]
    batches_per_sync: int
    syncs_per_checkpoint: int
    inlining_row_limit: int
    seed: int


@dataclass(slots=True)
class Result:
    variant: str
    compression: str
    materialize: bool
    parquet_version: int | None = None
    changes: int = 0
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    bytes: dict[str, int] = field(default_factory=lambda: dict.fromkeys(CLASSES, 0))
    files: dict[str, int] = field(default_factory=lambda: dict.fromkeys(CLASSES, 0))
    #: compressed bytes of the ``_ducklake_internal_*`` columns in new data files
    internal_column_bytes: int = 0
    #: new data files carrying at least one internal column
    data_files_with_internal: int = 0
    final_rows: int = 0
    live_data_bytes: int = 0
    live_internal_bytes: int = 0
    live_delete_files: int = 0
    standin_zstd_bytes: int = 0
    duckdb_zstd_bytes: int = 0
    syncs: int = 0
    checkpoints: int = 0
    exit_codes: dict[str, int] = field(default_factory=dict)
    elapsed_s: float = 0.0


# ---------------------------------------------------------------- DuckLake


class Lake:
    """Local DuckLake writer, playing the role of the PoC consumer."""

    def __init__(self, root: Path) -> None:
        import duckdb

        self.dsn = f"ducklake:sqlite:{root / 'catalog.sqlite'}"
        self.data_path = f"{root / 'data'}/"
        (root / "data").mkdir(parents=True, exist_ok=True)
        self._duckdb = duckdb

    @contextlib.contextmanager
    def connect(self) -> Iterator[Any]:
        con = self._duckdb.connect(":memory:")
        try:
            for ext in ("ducklake", "sqlite"):
                con.execute(f"INSTALL {ext}")
                con.execute(f"LOAD {ext}")
            con.execute(
                f"ATTACH '{self.dsn}' AS lake "
                f"(METADATA_CATALOG 'lake_meta', DATA_PATH '{self.data_path}')"
            )
            yield con
        finally:
            con.close()

    def setup(
        self,
        compression: str,
        parquet_version: int | None,
        inlining_row_limit: int,
        rows: int,
    ) -> None:
        with self.connect() as con:
            con.execute(f"CALL lake.set_option('parquet_compression', '{compression}')")
            if parquet_version is not None:
                con.execute(
                    f"CALL lake.set_option('parquet_version', {parquet_version})"
                )
            con.execute("CREATE SCHEMA lake.raw")
            con.execute(
                """CREATE TABLE lake.raw.pedidos (
                    id BIGINT, cliente_id BIGINT, status VARCHAR,
                    valor DECIMAL(18,2), versao BIGINT,
                    created_at TIMESTAMP, updated_at TIMESTAMP)"""
            )
            # initial load (DMS full load): a single Parquet, no inlining
            con.execute("CALL lake.set_option('data_inlining_row_limit', 0)")
            con.execute(
                """INSERT INTO lake.raw.pedidos
                SELECT i + 1, (hash(i) % 10000)::BIGINT, 'novo',
                       ((hash(i * 7) % 100000) / 100.0)::DECIMAL(18,2), 1,
                       TIMESTAMP '2026-10-01' + to_seconds(i),
                       TIMESTAMP '2026-10-01' + to_seconds(i)
                FROM range($n) r(i)""",
                {"n": rows},
            )
            con.execute(
                f"CALL lake.set_option('data_inlining_row_limit', {inlining_row_limit})"
            )

    def apply_batch(self, lote: list[tuple[Any, ...]]) -> None:
        """DELETE + MERGE in one transaction, like ``lambda/consumer.py``."""
        with self.connect() as con:
            con.execute(
                """CREATE TEMP TABLE lote (
                    id BIGINT, op VARCHAR, cliente_id BIGINT, status VARCHAR,
                    valor DECIMAL(18,2), created_at TIMESTAMP, updated_at TIMESTAMP)"""
            )
            con.executemany("INSERT INTO lote VALUES (?, ?, ?, ?, ?, ?, ?)", lote)
            con.execute("BEGIN")
            con.execute(
                "DELETE FROM lake.raw.pedidos WHERE id IN "
                "(SELECT id FROM lote WHERE op = 'D')"
            )
            con.execute(
                """MERGE INTO lake.raw.pedidos AS t
                USING (SELECT * FROM lote WHERE op <> 'D') AS s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET
                    status = s.status, valor = t.valor + 1,
                    versao = t.versao + 1, updated_at = s.updated_at
                WHEN NOT MATCHED THEN INSERT
                    (id, cliente_id, status, valor, versao, created_at, updated_at)
                    VALUES (s.id, s.cliente_id, s.status, s.valor, 1,
                            s.created_at, s.updated_at)"""
            )
            con.execute("COMMIT")

    def final_state(self, out_dir: Path) -> dict[str, int]:
        """Final rows, live files and the ZSTD stand-in for the baseline."""
        import pyarrow.parquet as pq

        with self.connect() as con:
            con.execute("CALL ducklake_flush_inlined_data('lake')")
            rows = int(
                con.execute("SELECT count(*) FROM lake.raw.pedidos").fetchone()[0]
            )
            files = con.execute(
                "SELECT data_file, delete_file "
                "FROM ducklake_list_files('lake', 'pedidos', schema => 'raw')"
            ).fetchall()
            arrow = con.sql(
                "SELECT * FROM lake.raw.pedidos ORDER BY id"
            ).to_arrow_table()
            duck_path = out_dir / "duckdb-zstd.parquet"
            # COPY does not accept a parameter as the target; the path is our workdir
            con.execute(
                f"COPY (SELECT * FROM lake.raw.pedidos ORDER BY id) TO '{duck_path}' "  # noqa: S608
                "(FORMAT parquet, COMPRESSION zstd)"
            )
        live_bytes = live_internal = 0
        for data_file, _ in files:
            live_bytes += os.path.getsize(data_file)
            live_internal += internal_bytes(data_file)
        standin = out_dir / "standin-zstd.parquet"
        # close to Spark's parquet-mr: v1 pages, dictionary, ZSTD level 3 and
        # DECIMAL(18,2) como INT64
        pq.write_table(
            arrow,
            standin,
            compression="zstd",
            compression_level=3,
            store_decimal_as_integer=True,
        )
        return {
            "final_rows": rows,
            "live_data_bytes": live_bytes,
            "live_internal_bytes": live_internal,
            "live_delete_files": sum(1 for _, d in files if d),
            "standin_zstd_bytes": standin.stat().st_size,
            "duckdb_zstd_bytes": duck_path.stat().st_size,
        }


def internal_bytes(path: str) -> int:
    """Compressed bytes of the ``_ducklake_internal_*`` columns of a Parquet."""
    import pyarrow.parquet as pq

    meta = pq.ParquetFile(path).metadata
    total = 0
    for rg in range(meta.num_row_groups):
        group = meta.row_group(rg)
        for col in range(group.num_columns):
            chunk = group.column(col)
            if chunk.path_in_schema.startswith(INTERNAL_PREFIX):
                total += chunk.total_compressed_size
    return total


# ---------------------------------------------------------------- generator


class Generator:
    """Batches in the shape of the PoC generator's output."""

    def __init__(self, params: Params, start_id: int) -> None:
        self.rng = random.Random(params.seed)  # noqa: S311 - synthetic load
        self.params = params
        self.max_id = start_id
        self.clock = 0

    def batch(self) -> tuple[list[tuple[Any, ...]], tuple[int, int, int]]:
        p = self.params
        n_ins = round(p.changes_per_batch * p.mix[0] / 100)
        n_upd = round(p.changes_per_batch * p.mix[1] / 100)
        n_del = round(p.changes_per_batch * p.mix[2] / 100)
        self.clock += 1
        ts = START + timedelta(minutes=self.clock)
        lote: dict[int, tuple[Any, ...]] = {}
        # updates skewed toward recent ids: power(random(), 4)
        for _ in range(n_upd):
            key = self.max_id - int(self.rng.random() ** 4 * self.max_id)
            if key > 0:
                lote[key] = (key, "U", None, self.rng.choice(STATUS), None, None, ts)
        for _ in range(n_del):
            key = 1 + int(self.rng.random() * self.max_id)
            lote[key] = (key, "D", None, None, None, None, None)
        for _ in range(n_ins):
            self.max_id += 1
            lote[self.max_id] = (
                self.max_id,
                "I",
                self.rng.randrange(10000),
                "novo",
                round(self.rng.random() * 1000, 2),
                ts,
                ts,
            )
        return list(lote.values()), (n_ins, n_upd, n_del)


# ---------------------------------------------------------------- files


def classify(path: Path, data_root: Path, iceberg_root: Path) -> str | None:
    if path.is_relative_to(data_root):
        if path.suffix != ".parquet":
            return None
        return "ducklake_deletes" if path.name.endswith("-delete.parquet") else "data"
    if path.is_relative_to(iceberg_root):
        return "iceberg_deletes" if path.suffix == ".parquet" else "iceberg_metadata"
    return None


def walk(roots: tuple[Path, ...]) -> dict[Path, int]:
    found: dict[Path, int] = {}
    for root in roots:
        if not root.exists():
            continue
        for dirpath, _, names in os.walk(root):
            for name in names:
                full = Path(dirpath) / name
                found[full] = full.stat().st_size
    return found


# ---------------------------------------------------------------- variant


def run_variant(name: str, params: Params, root: Path) -> Result:
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    from unittest import mock

    from moto import mock_aws
    from typer.testing import CliRunner

    from iceduck.cli import app

    compression, materialize, parquet_version = VARIANTS[name]
    result = Result(
        variant=name,
        compression=compression,
        materialize=materialize,
        parquet_version=parquet_version,
    )
    started = time.perf_counter()
    lake = Lake(root)
    iceberg = root / "iceberg"
    config = root / "iceduck.yaml"
    config.write_text(
        f"catalog_dsn: '{lake.dsn}'\n"
        f"data_path: '{lake.data_path}'\n"
        f"iceberg_metadata_prefix: '{iceberg}'\n"
        "glue_database: analytics\n"
        "aws_region: us-east-1\n"
    )
    data_root = Path(lake.data_path)
    roots = (data_root, iceberg)
    runner = CliRunner()

    def iceduck(*args: str) -> int:
        cmd = [*args[:1], "--table", TABLE, "--config", str(config), "--json"]
        out = runner.invoke(app, [*cmd, *args[1:]])
        if out.exit_code not in (0, 4):
            raise SystemExit(
                f"iceduck {' '.join(args)} exited with {out.exit_code}:\n{out.stdout}"
            )
        key = f"{' '.join(args)}={out.exit_code}"
        result.exit_codes[key] = result.exit_codes.get(key, 0) + 1
        return out.exit_code

    with mock_aws(), mock.patch("iceduck.glue._validate_metadata_location"):
        lake.setup(
            compression, parquet_version, params.inlining_row_limit, params.initial_rows
        )
        iceduck("sync")
        seen = walk(roots)

        def account() -> None:
            for path, size in walk(roots).items():
                if path in seen:
                    continue
                seen[path] = size
                kind = classify(path, data_root, iceberg)
                if kind is None:
                    continue
                result.bytes[kind] += size
                result.files[kind] += 1
                if kind == "data":
                    internal = internal_bytes(str(path))
                    result.internal_column_bytes += internal
                    result.data_files_with_internal += internal > 0

        gen = Generator(params, params.initial_rows)
        maintenance = ["sync", "--compact"]
        if materialize:
            maintenance += ["--materialize-deletes", "--delete-threshold", "0"]
        for i in range(1, params.batches + 1):
            lote, (ins, upd, dele) = gen.batch()
            lake.apply_batch(lote)
            result.inserts += ins
            result.updates += upd
            result.deletes += dele
            if i % params.batches_per_sync:
                continue
            result.syncs += 1
            if result.syncs % params.syncs_per_checkpoint == 0:
                result.checkpoints += 1
                iceduck(*maintenance)
            else:
                iceduck("sync")
            account()
        if params.batches % params.batches_per_sync:
            iceduck("sync")
            result.syncs += 1
            account()

    result.changes = result.inserts + result.updates + result.deletes
    final_dir = root / "final"
    final_dir.mkdir()
    for key, value in lake.final_state(final_dir).items():
        setattr(result, key, value)
    result.elapsed_s = round(time.perf_counter() - started, 1)
    return result


# ---------------------------------------------------------------- report


def render(results: list[Result], params: Params) -> str:
    def per(value: int, denom: int) -> str:
        return f"{value / denom:.1f}" if denom else "—"

    lines = [
        f"Load: {params.initial_rows} initial rows, {params.batches} batches of "
        f"{params.changes_per_batch} changes (mix {params.mix}), sync every "
        f"{params.batches_per_sync} batches, maintenance every "
        f"{params.syncs_per_checkpoint} syncs, inlining {params.inlining_row_limit}.",
        "",
        "| Variant | Data | DuckLake deletes | Iceberg metadata | Iceberg deletes "
        "| **Total B/change** | Internal columns (% of data bytes) "
        "| Live B/row | Live delete files | Plain ZSTD B/row* |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for r in results:
        total = sum(r.bytes.values())
        share = (
            100 * r.internal_column_bytes / r.bytes["data"] if r.bytes["data"] else 0.0
        )
        lines.append(
            f"| {r.variant} | {per(r.bytes['data'], r.changes)} "
            f"| {per(r.bytes['ducklake_deletes'], r.changes)} "
            f"| {per(r.bytes['iceberg_metadata'], r.changes)} "
            f"| {per(r.bytes['iceberg_deletes'], r.changes)} "
            f"| **{per(total, r.changes)}** | {share:.0f}% "
            f"| {per(r.live_data_bytes, r.final_rows)} | {r.live_delete_files} "
            f"| {per(r.standin_zstd_bytes, r.final_rows)} / "
            f"{per(r.duckdb_zstd_bytes, r.final_rows)} |"
        )
    lines += [
        "",
        "B/change columns: bytes of new files per generated change, after the "
        "initial load. Live B/row: data files current at the end / final rows. "
        "Live delete files: data files with a delete file in the final state. "
        "*Local stand-in for the Spark batch baseline: the final rows in a ZSTD "
        "Parquet without internal columns, via pyarrow (v1 pages, level 3) / via "
        "DuckDB COPY; it is not Spark.",
    ]
    return "\n".join(lines)


def _parse_params(ns: argparse.Namespace) -> Params:
    mix = tuple(float(x) for x in ns.mix.split(","))
    if len(mix) != 3:
        raise SystemExit("--mix expects three percentages: inserts,updates,deletes")
    return Params(
        initial_rows=ns.initial_rows,
        batches=ns.batches,
        changes_per_batch=ns.changes_per_batch,
        mix=(mix[0], mix[1], mix[2]),
        batches_per_sync=ns.batches_per_sync,
        syncs_per_checkpoint=ns.syncs_per_checkpoint,
        inlining_row_limit=ns.inlining_row_limit,
        seed=ns.seed,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--initial-rows", type=int, default=200_000)
    parser.add_argument(
        "--batches", type=int, default=240, help="batches (1 per minute)"
    )
    parser.add_argument("--changes-per-batch", type=int, default=2000)
    parser.add_argument("--mix", default="85,13,2", help="inserts,updates,deletes (%%)")
    parser.add_argument("--batches-per-sync", type=int, default=15)
    parser.add_argument("--syncs-per-checkpoint", type=int, default=4)
    parser.add_argument("--inlining-row-limit", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=11)
    parser.add_argument(
        "--variants",
        default=",".join(VARIANTS),
        help=f"subset of {list(VARIANTS)}",
    )
    parser.add_argument("--workdir", type=Path, help="default: temporary directory")
    parser.add_argument("--keep", action="store_true", help="do not delete the workdir")
    parser.add_argument("--json", action="store_true", help="raw result as JSON")
    parser.add_argument("--run-one", help=argparse.SUPPRESS)
    ns = parser.parse_args()
    params = _parse_params(ns)

    if ns.run_one:
        res = run_variant(ns.run_one, params, ns.workdir)
        print(json.dumps(asdict(res)))
        return

    names = [v.strip() for v in ns.variants.split(",") if v.strip()]
    unknown = set(names) - set(VARIANTS)
    if unknown:
        raise SystemExit(f"unknown variants: {sorted(unknown)}")
    workdir = ns.workdir or Path(tempfile.mkdtemp(prefix="iceduck-wa-"))
    results: list[Result] = []
    forwarded = [
        f"--initial-rows={params.initial_rows}",
        f"--batches={params.batches}",
        f"--changes-per-batch={params.changes_per_batch}",
        f"--mix={ns.mix}",
        f"--batches-per-sync={params.batches_per_sync}",
        f"--syncs-per-checkpoint={params.syncs_per_checkpoint}",
        f"--inlining-row-limit={params.inlining_row_limit}",
        f"--seed={params.seed}",
    ]
    try:
        for name in names:
            root = workdir / name.replace("+", "-")
            if root.exists():
                shutil.rmtree(root)
            root.mkdir(parents=True)
            sys.stderr.write(f"[{name}] running in {root}\n")
            proc = subprocess.run(  # noqa: S603 - this very script, same args
                [
                    sys.executable,
                    __file__,
                    *forwarded,
                    "--run-one",
                    name,
                    "--workdir",
                    str(root),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                sys.stderr.write(proc.stdout + proc.stderr)
                raise SystemExit(f"variant {name} failed (exit {proc.returncode})")
            data = json.loads(proc.stdout.strip().splitlines()[-1])
            results.append(Result(**data))
            sys.stderr.write(f"[{name}] {data['elapsed_s']} s\n")
    finally:
        if not ns.keep and ns.workdir is None:
            shutil.rmtree(workdir, ignore_errors=True)

    if ns.json:
        print(
            json.dumps(
                {"params": asdict(params), "results": [asdict(r) for r in results]}
            )
        )
    else:
        print(render(results, params))


if __name__ == "__main__":
    main()
