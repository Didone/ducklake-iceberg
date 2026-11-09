# ducklake-iceberg

[![CI](https://github.com/Didone/ducklake-iceberg/actions/workflows/ci.yml/badge.svg)](https://github.com/Didone/ducklake-iceberg/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/ducklake-iceberg.svg)](https://pypi.org/project/ducklake-iceberg/)
[![Python](https://img.shields.io/pypi/pyversions/ducklake-iceberg.svg)](https://pypi.org/project/ducklake-iceberg/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**ducklake-iceberg** is a command-line tool that publishes
[DuckLake](https://ducklake.select/) tables as **Apache Iceberg v2** tables in
the **AWS Glue Data Catalog**, **without copying any data**. The Iceberg
manifests point directly at the Parquet files DuckLake already wrote, so
engines that speak Iceberg (Amazon Athena, Redshift Spectrum, Snowflake, Trino,
Spark, ...) can query tables that are managed by DuckDB.

```text
┌──────────┐      ┌──────────┐      ┌───────────────┐      ┌──────────┐
│ DuckLake │─────▶│ ducklake-iceberg  │─────▶│ S3 (Iceberg   │─────▶│   Glue   │
│ catalog  │      │   CLI    │      │   metadata)   │      │ catalog  │
└──────────┘      └──────────┘      └───────────────┘      └──────────┘
      │                                                          │
      └──── Parquet data files (never copied) ◀── Athena, Trino, Spark, ...
```

## Features

- **Zero-copy.** Only Iceberg metadata is written. Data Parquet files are
  never copied, moved or deleted by ducklake-iceberg.
- **Incremental sync.** Each `sync` publishes one new Iceberg snapshot
  (`append`, `delete`, `overwrite` or `replace`) and reuses the manifests that
  did not change. When nothing changed, it exits with code 4 and writes
  nothing.
- **Deletes.** DuckLake `DELETE`, `UPDATE` and `MERGE` become Iceberg v2
  *position delete files*. `--materialize-deletes` rewrites the affected data
  files instead, when that is preferable.
- **Field IDs preserved.** Iceberg field IDs equal DuckLake `column_id`s,
  including nested `STRUCT`/`LIST`/`MAP` fields, renames and dropped columns,
  so readers never see silently-null columns.
- **Pre-flight checks.** `doctor` verifies Iceberg v2 compatibility before
  anything is exported and reports blockers with a dedicated exit code.
- **Safe by design.** The Iceberg table is a read-only mirror. External writes
  (for example from Athena) are detected and block the sync (exit code 3).
- **Automation friendly.** Stable exit codes, a versioned `--json` envelope,
  structured logs on stderr and credential redaction.

## Requirements

- Python 3.11 to 3.14
- AWS credentials with access to S3 and Glue
- A DuckLake catalog (SQLite or PostgreSQL backend)

`duckdb` (2.0.0) and `pyiceberg` (0.12.0) are pinned to exact versions because
DuckDB extensions are published per DuckDB release and the metadata writer
relies on internal PyIceberg constructors.

## Installation

```bash
pip install ducklake-iceberg
# or, as an isolated command-line tool
pipx install ducklake-iceberg
uv tool install ducklake-iceberg
```

Check the installation:

```bash
ducklake-iceberg --version
```

## Configuration

Settings are resolved in this order: **CLI flag > `DUCKLAKE_ICEBERG_*` environment
variable > YAML file > default**. The YAML file is the one given by `--config`,
then `DUCKLAKE_ICEBERG_CONFIG_FILE`, then `./ducklake-iceberg.yaml` or the user config
directory (`~/.config/ducklake-iceberg/config.yaml`).

```yaml
# DuckLake catalog. Never put credentials in the DSN: use the libpq PG*
# environment variables (PGUSER, PGPASSWORD, ...) for PostgreSQL.
catalog_dsn: "ducklake:postgres:dbname=lake host=pg.internal"
catalog_alias: "lake"             # optional, default "lake"
metadata_catalog: "lake_meta"     # optional, default "lake_meta"
data_path: "s3://my-bucket/ducklake/data/"

# Iceberg metadata prefix. It must live OUTSIDE data_path, otherwise DuckLake's
# orphan-file cleanup would delete it.
iceberg_metadata_prefix: "s3://my-bucket/iceberg/"

# AWS
glue_database: "my_database"
aws_region: "us-east-1"
aws_profile: "default"            # optional
log_level: "INFO"                 # optional

# Optional tuning
stats_source: "catalog"           # "catalog" (default, no S3 GET) or "footer"
snapshot_retention_hours: 24      # expire older Iceberg snapshots (default: keep all)
max_table_workers: 4              # concurrent tables with --all-tables
max_footer_workers: 8             # concurrent Parquet footer reads
max_io_workers: 8                 # parallel I/O operations
max_total_workers: 32             # global concurrency ceiling
duckdb_threads: null              # DuckDB `SET threads`
duckdb_memory_limit: null         # DuckDB memory limit
duckdb_extension_directory: null  # pre-installed extensions (read-only HOME)
```

The same keys are available as environment variables with the `DUCKLAKE_ICEBERG_`
prefix:

```bash
export DUCKLAKE_ICEBERG_CATALOG_DSN="ducklake:postgres:dbname=lake host=pg.internal"
export DUCKLAKE_ICEBERG_DATA_PATH="s3://my-bucket/ducklake/data/"
export DUCKLAKE_ICEBERG_ICEBERG_METADATA_PREFIX="s3://my-bucket/iceberg/"
export DUCKLAKE_ICEBERG_GLUE_DATABASE="my_database"
export DUCKLAKE_ICEBERG_AWS_REGION="us-east-1"
```

## Quick start

```bash
# Run the whole pipeline for one table: doctor + checkpoint + export + register
ducklake-iceberg sync --table sales.orders

# Inspect the result
ducklake-iceberg status --table sales.orders
```

Or step by step:

```bash
ducklake-iceberg doctor     --table sales.orders   # Iceberg v2 compatibility pre-flight
ducklake-iceberg checkpoint --table sales.orders   # flush inlined data (DuckLake)
ducklake-iceberg export     --table sales.orders   # write Iceberg metadata to S3
ducklake-iceberg register   --table sales.orders   # create/update the table in Glue
```

## Commands

Every command accepts `--table schema.table` or `--all-tables` (optionally
restricted with `--schema`), `--json`, `--profile`, `--config`, `--log-level`
and `--log-format`. Use `ducklake-iceberg <command> --help` for the full option list.

| Command      | What it does |
| ------------ | ------------ |
| `doctor`     | Compatibility pre-flight: identity, connectivity and Iceberg v2 rules. A blocker exits with code 2. |
| `checkpoint` | Per-table DuckLake checkpoint: flush inlined data, expire snapshots, merge and rewrite files. The only command that **writes to DuckLake**. |
| `export`     | Reads the catalog and writes Iceberg manifests, the manifest list and `vN.metadata.json` to S3. Incremental after the first export. |
| `register`   | Registers the table in Glue, or advances its metadata pointer with optimistic locking. Exits with code 3 if Glue points at metadata written by someone else. |
| `sync`       | `doctor` + `checkpoint` + `export` + `register` under a per-table process lock. |
| `status`     | Compares DuckLake with Glue: snapshots, pending changes and divergence. |
| `validate`   | Reconciles DuckLake and Iceberg: row counts, column bounds and samples (`--deep` reads the Iceberg files). |
| `clean`      | Garbage-collects Iceberg metadata no retained snapshot can reach, or scheduled DuckLake files. Reports only, unless `--apply` is given. |

### Examples

```bash
# Dry run: show what would happen without writing anything
ducklake-iceberg sync --table sales.orders --dry-run

# All tables of a schema, no confirmation prompt
ducklake-iceberg sync --all-tables --schema sales --yes

# Full checkpoint (expire + merge + rewrite) before exporting
ducklake-iceberg sync --table sales.orders --compact

# Publish Glue column statistics for the query planner
ducklake-iceberg sync --table sales.orders --glue-stats on

# Concurrency limits and fail-fast
ducklake-iceberg sync --all-tables --yes --max-table-workers 4 --fail-fast

# Remove unreachable Iceberg metadata older than 48 hours
ducklake-iceberg clean --table sales.orders --grace 48h --apply
```

By default `sync` only flushes pending inlined data to DuckLake. The full
compaction (expire + merge + rewrite) runs with `ducklake-iceberg checkpoint` or
`ducklake-iceberg sync --compact`.

With `--glue-stats on`, ducklake-iceberg scans the published DuckLake snapshot and
sends column statistics to Glue. The scan can read all the data; use
`--glue-stats-sample 5` to sample 5% of it. Distinct-value counts come from
`approx_count_distinct` (HyperLogLog), so they are **estimates**.

## Exit codes

Exit codes are part of the public contract.

| Code | Meaning |
| ---- | ------- |
| `0`  | Success |
| `1`  | Error |
| `2`  | Blocked by the compatibility pre-flight (a command-line usage error also exits with 2; use `--json` to tell them apart) |
| `3`  | Diverged: the Iceberg table was modified outside of ducklake-iceberg |
| `4`  | Nothing to do |

## JSON output and logs

Any command accepts `--json` and prints a versioned envelope on stdout. The
schema ships with the package (`ducklake-iceberg/schemas/envelope-v1.json`). Consumers
should ignore unknown keys, should not rely on the order of `results[]` or
`problems[]`, and should decide on `status`, `exit_code` and `problems[].code`
instead of parsing message text.

- Adding optional keys, new problem codes or new commands keeps the envelope
  version.
- Renaming or removing keys, or changing a key's type or meaning, bumps the
  version. The previous version stays available for at least one minor
  release through `--json-version N`.

Logs go to stderr, one JSON object per event with `--log-format json`.
`--metrics` adds per-phase timings and counters (DuckDB connections, boto3
sessions, AWS calls and retries) to each result, and `sync` includes them by
default. Credentials, full DSNs and encryption keys are redacted from logs,
envelopes and terminal output. Setting `DUCKLAKE_ICEBERG_TIMINGS=1` adds a `timings` key
to the `--json` envelope.

## How it works

1. **doctor** checks Iceberg v2 compatibility. Iceberg v2 is the target
   because Athena only supports v2: no deletion vectors, variant, geometry or
   nanosecond timestamps.
2. **checkpoint** flushes inlined data (and optionally compacts) in DuckLake.
3. **export** computes the delta against the last exported snapshot and writes
   Avro manifests (via `fastavro`) plus `vN.metadata.json`. One DuckLake table
   checkpoint becomes one Iceberg snapshot.
4. **register** calls Glue `register_table` the first time and `UpdateTable`
   afterwards, never `create_table` (which would reassign field IDs).

The sync state lives in the Iceberg table properties, not in local files. The
catalog is accessed only through DuckDB, attached read-only except for
`checkpoint`.

## Development

```bash
git clone https://github.com/Didone/ducklake-iceberg.git
cd ducklake-iceberg
poetry install
poetry run pytest -m "not postgres and not e2e and not minio"
poetry run ruff check .
poetry run ruff format --check .
poetry run mypy
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the architecture notes, test markers
and the release process.

## Releasing

Releases are published to [PyPI](https://pypi.org/project/ducklake-iceberg/) by the
`release` GitHub Actions workflow when a `vX.Y.Z` tag is pushed. The tag must
match the version in `pyproject.toml`, and `CHANGELOG.md` must contain a
`## [X.Y.Z]` section. See [CONTRIBUTING.md](CONTRIBUTING.md#releasing).

## License

[MIT](LICENSE) © Tiago Didone

## Acknowledgments

- [Apache Iceberg](https://iceberg.apache.org/) for the table format specification
- [DuckDB](https://duckdb.org/) and [DuckLake](https://ducklake.select/)
- [PyIceberg](https://py.iceberg.apache.org/) for the Python Iceberg library
- AWS Glue Data Catalog
