# Getting started

[Documentation home](README.md) | [Concepts](concepts.md) | [Configuration](configuration.md)

## Requirements and installation

Use Python 3.11-3.14 on a POSIX host (Linux, macOS, or WSL). For a real
publication you also need an existing DuckLake catalog, reachable data files,
an S3 metadata prefix, and access to AWS Glue. The tool does not provision
these resources or ingest your original source data.

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install ducklake-iceberg
ducklake-iceberg --version
ducklake-iceberg sync --help
```

If only a release candidate is available, request it explicitly:

```bash
python -m pip install "ducklake-iceberg==0.1.0rc1"
```

Normal pip resolution excludes pre-releases unless requested. Updating pip
also avoids the dependency-resolution failure observed with pip 25.0.1
when combining the Glue and S3 extras of PyIceberg.

The repository pins DuckDB and PyIceberg exactly. Do not independently
upgrade them in a production installation; extensions are version/platform
specific and the metadata writer uses version-sensitive APIs.

For repository development instead:

```bash
git clone https://github.com/Didone/ducklake-iceberg.git
cd ducklake-iceberg
poetry install
poetry run ducklake-iceberg --version
```

The following local exercise uses that Poetry environment.

## Local exercise without AWS

The simplest real CLI exercise is the existing SQLite/moto test. It creates
a real DuckLake table, writes Iceberg metadata, simulates Glue, verifies the
rows through PyIceberg, changes rows, and checks incremental/idempotent sync:

```bash
poetry run pytest \
  tests/integration/test_sync_e2e.py::test_sync_registers_updates_and_is_idempotent \
  -m "not postgres" --no-cov -v
```

No AWS account or Docker is required. Initial extension installation needs
access to the DuckDB extension repository or a populated extension cache.
The test uses temporary local files and moto instead of real AWS. It
explicitly relaxes Glue registration's S3-only location validation for this
test; do not copy that patch into production.

For the networked S3-compatible path without a real AWS account:

```bash
poetry run pytest -m minio --cov-fail-under=0 -v
```

This needs Docker, starts MinIO with testcontainers, uses simulated Glue,
and verifies that existing source data files were not changed or copied.
Run the whole marked suite: its lifecycle tests intentionally share state.
See [Testing](testing.md) for fixtures and limitations.

### Inspect the source format yourself

This independent Python exercise creates a tiny DuckLake on local disk. Run
it in a new directory, not against an existing production catalog:

```python
from pathlib import Path

import duckdb

root = Path("learning-lake").resolve()
root.mkdir(exist_ok=True)
con = duckdb.connect(":memory:")
try:
    for extension in ("ducklake", "sqlite"):
        con.execute(f"INSTALL {extension}")
        con.execute(f"LOAD {extension}")
    con.execute(
        "ATTACH $dsn AS lake (DATA_PATH $data, METADATA_CATALOG 'lake_meta')",
        {
            "dsn": f"ducklake:sqlite:{root}/catalog.sqlite",
            "data": f"{root}/data/",
        },
    )
    con.execute("CREATE SCHEMA lake.sales")
    con.execute("CREATE TABLE lake.sales.orders (id INTEGER, amount DECIMAL(12,2))")
    con.execute("INSERT INTO lake.sales.orders VALUES (1, 25.50), (2, 10.00)")
    print(con.sql("SELECT * FROM lake.sales.orders").fetchall())
    con.execute(
        "CALL ducklake_flush_inlined_data("
        "'lake', schema_name => 'sales', table_name => 'orders')"
    )
    print(con.sql("SELECT * FROM lake_meta.ducklake_snapshot").to_arrow_table())
finally:
    con.close()
```

The explicit flush makes the rows available as files. This is a learning
script, not an idempotent provisioning script: running it again fails on the
already-existing schema/table. The production sync invokes the flush itself
when needed.

## First real publication

### 1. Prepare the source and permissions

Use a DuckLake already created by your writer. For production, the source
files should be in S3 if distributed readers such as Athena must access them.
A local absolute data path is supported by internal export/test workflows,
but Athena cannot read your laptop's filesystem.

Choose non-overlapping prefixes, for example:

```text
data_path:                s3://example-lake/ducklake/
iceberg_metadata_prefix:  s3://example-lake/iceberg/
```

Both contain an object key prefix, not just a bucket root. Configure the
[sync IAM policy](deploy/iam/sync-policy.json) with your resource names.
Account for the flush's ability to write new source files and catalog
metadata. Protect the mirror from external writes.

### 2. Configure credentials separately

For a local AWS profile, authenticate with your normal AWS tooling. For
example, if it is an SSO profile:

```bash
aws sso login --profile lake-publisher
```

Production jobs should normally use a workload/task role instead. For
PostgreSQL, supply the existing database credentials via libpq environment
variables or the scheduler's secret injection; never add `user=` or
`password=` to `catalog_dsn`.

### 3. Create the YAML file

Save as `ducklake-iceberg.yaml`:

```yaml
catalog_dsn: "ducklake:postgres:dbname=lake host=pg.internal port=5432"
data_path: "s3://example-lake/ducklake/"
iceberg_metadata_prefix: "s3://example-lake/iceberg/"
glue_database: "analytics"
aws_region: "us-east-1"
aws_profile: "lake-publisher"
```

These are illustrative host/bucket/profile names. Required settings have no
invented fallback. Use [Configuration](configuration.md) before tuning
retention, threads or memory.

### 4. Perform a read-only check

With `PGUSER`/`PGPASSWORD` provided by your secret mechanism:

```bash
ducklake-iceberg sync --table sales.orders --dry-run --json
```

Exit 0 means changes are pending, not that data was published. Exit 4 means
the mirror is already current. Exit 1, 2 or 3 requires investigation.
A dry run can still read catalogs, S3 and known publication state in Glue,
and install/load extensions. It skips the table lock and does not flush rows
or write remote table state.

### 5. Publish and verify

```bash
ducklake-iceberg sync --table sales.orders --json
aws glue get-table --region us-east-1 --database-name analytics --name orders \
  --query 'Table.Parameters'
```

Use the same AWS identity/profile for the verification if your shell does
not default to the publisher identity (`aws ... --profile lake-publisher`).
Check `table_type=ICEBERG` and `metadata_location`. In an Athena workgroup
already configured for your account, run:

```sql
SELECT * FROM analytics.orders LIMIT 10;
```

Compare row counts and representative values with DuckLake. For tables with
deletes, include the deleted/updated keys in the comparison.

Repeat the sync without source changes: it should exit 4. Make a controlled
source change and repeat: the mirror should advance without copying
existing Parquet files.

The source schema name is **not** automatically included in the Glue table
name: `sales.orders` becomes `analytics.orders`. Do not send two source
tables with the same normalized name to the same Glue database/prefix.
See [naming and collision handling](usage.md#table-names-and-destination-layout).

### 6. Schedule it

Only after verifying the initial publication, choose one scheduler owner per
table and configure exit 4 as success. The
[deployment guide](deploy/README.md) includes cron, container, ECS and Airflow
examples; [Operations](operations.md) explains monitoring and recovery.
