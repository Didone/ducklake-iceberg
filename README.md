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
- **One command.** `ducklake-iceberg sync --table schema.table` is meant to run
  from cron, Airflow or any scheduler. It is idempotent: when nothing changed,
  it exits with code 4 and writes nothing.
- **Incremental.** Each run publishes one new Iceberg snapshot (`append`,
  `delete`, `overwrite` or `replace`) and reuses the manifests that did not
  change.
- **Deletes.** DuckLake `DELETE`, `UPDATE` and `MERGE` become Iceberg v2
  *position delete files*.
- **Field IDs preserved.** Iceberg field IDs equal DuckLake `column_id`s,
  including nested `STRUCT`/`LIST`/`MAP` fields, renames and dropped columns,
  so readers never see silently-null columns.
- **Pre-flight checks.** Before exporting, `sync` verifies Iceberg v2
  compatibility and stops with a dedicated exit code (2) and an explanation when
  the table cannot be published.
- **Safe by design.** The Iceberg table is a read-only mirror. External writes
  (for example from Athena) are detected and stop the sync (exit code 3).
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

Settings are resolved in this order: **CLI flag > `DUCKLAKE_ICEBERG_*`
environment variable > YAML file > default**. The YAML file is the one given by
`--config`, then `DUCKLAKE_ICEBERG_CONFIG_FILE`, then `./ducklake-iceberg.yaml`
or `~/.config/ducklake-iceberg/config.yaml`.

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
max_footer_workers: 8             # concurrent Parquet footer reads
max_io_workers: 8                 # parallel I/O operations
max_total_workers: 32             # global concurrency ceiling
duckdb_threads: null              # DuckDB `SET threads`
duckdb_memory_limit: null         # DuckDB memory limit
duckdb_extension_directory: null  # pre-installed extensions (read-only HOME)
```

The same keys are available as environment variables with the
`DUCKLAKE_ICEBERG_` prefix:

```bash
export DUCKLAKE_ICEBERG_CATALOG_DSN="ducklake:postgres:dbname=lake host=pg.internal"
export DUCKLAKE_ICEBERG_DATA_PATH="s3://my-bucket/ducklake/data/"
export DUCKLAKE_ICEBERG_ICEBERG_METADATA_PREFIX="s3://my-bucket/iceberg/"
export DUCKLAKE_ICEBERG_GLUE_DATABASE="my_database"
export DUCKLAKE_ICEBERG_AWS_REGION="us-east-1"
```

## Usage

```bash
ducklake-iceberg sync --table sales.orders
```

| Option                | Description |
| --------------------- | ----------- |
| `--table schema.table` | Table to publish (required). |
| `--dry-run`           | Write nothing: no flush of inlined data, no export. Still runs the pre-flight checks. |
| `--json`              | Print the JSON envelope on stdout. |
| `--profile`           | AWS profile. |
| `--config`            | YAML configuration file. |
| `--log-level`, `--log-format` | Log verbosity, and `auto`/`json`/`text` format on stderr. |

`sync` runs these stages for the table, under a per-table process lock:

1. **doctor**: Iceberg v2 compatibility pre-flight (exit 2 on a blocker).
2. **flush**: pending *inlined* data is written to Parquet in DuckLake. This is
   the only step that writes to DuckLake; it never compacts or deletes files.
3. **export**: computes the delta against the last published snapshot and writes
   the Iceberg manifests and `vN.metadata.json` to S3.
4. **register**: `register_table` in Glue the first time, then `UpdateTable`
   with optimistic locking.

### Running from cron

```cron
# every 15 minutes, one line per table
*/15 * * * * ducklake-iceberg sync --table sales.orders --config /etc/ducklake-iceberg.yaml --json >> /var/log/ducklake-iceberg/orders.json 2>/dev/null
```

Logs go to stderr and the envelope to stdout, so they can be sent to different
files. Alert on exit codes 1, 2 and 3 (see below).

## Exit codes

Exit codes are part of the public contract.

| Code | Meaning | Typical action |
| ---- | ------- | -------------- |
| `0`  | Published a new snapshot | none |
| `1`  | Error (AWS, catalog, configuration, ...) | alert; safe to retry |
| `2`  | Blocked by the compatibility pre-flight (a command-line usage error also exits with 2; use `--json` to tell them apart) | fix the table; see the `blocked` problem |
| `3`  | Diverged: the Iceberg table was modified outside of ducklake-iceberg | stop the external writer, then investigate |
| `4`  | Nothing to do | none (treat as success) |

## JSON output and logs

With `--json`, stdout carries only a versioned envelope. The schema ships with
the package (`ducklake_iceberg/schemas/envelope-v1.json`). Consumers should
ignore unknown keys and decide on `status`, `exit_code` and `problems[].code`
instead of parsing message text. Adding optional keys keeps the envelope
version; renaming or removing keys, or changing a key's type or meaning, bumps
it.

Logs go to stderr, one JSON object per event with `--log-format json`.
Per-phase timings and counters (DuckDB connections, boto3 sessions, AWS calls
and retries) are part of each result, and `DUCKLAKE_ICEBERG_TIMINGS=1` adds a
`timings` key to the envelope. Credentials, full DSNs and encryption keys are
redacted from logs, envelopes and terminal output.

## What ducklake-iceberg does not do

DuckLake maintenance is yours to schedule with DuckLake itself
(`CALL ducklake_merge_adjacent_files`, `ducklake_expire_snapshots`,
`ducklake_cleanup_old_files`, ...). Keep DuckLake's file-cleanup retention
longer than `snapshot_retention_hours`, otherwise a retained Iceberg snapshot
may reference Parquet files DuckLake has already deleted.

Tables with deletion vectors (puffin delete files), nested `VARIANT` columns or
other types that Iceberg v2 cannot represent are reported as blockers (exit 2).
Iceberg v2 is the target because Athena only supports v2.

## Development

```bash
git clone https://github.com/Didone/ducklake-iceberg.git
cd ducklake-iceberg
poetry install
poetry run pytest -m "not postgres and not e2e"
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
