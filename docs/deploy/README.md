# Deploying ducklake-iceberg

`ducklake-iceberg sync --table schema.table` is a short-lived, idempotent
command: schedule one run per table with whatever you already use. Runs of
different tables can overlap; two runs of the same table on the same host are
serialized by a per-table lock (the second one exits with code 1 and is safe to
retry).

Exit codes to alert on: **1** (error, safe to retry), **2** (blocked by the
pre-flight: the table needs a change at the source) and **3** (the Iceberg
mirror was written by someone else). **0** and **4** are success.

## Permissions

[`iam/sync-policy.json`](iam/sync-policy.json) is the policy of the role that
runs the sync. Replace the `<PLACEHOLDERS>`: `<DATA_BUCKET>`/`<DATA_PREFIX>`
(DuckLake's `data_path`), `<METADATA_BUCKET>`/`<METADATA_PREFIX>`
(`iceberg_metadata_prefix`), `<REGION>`, `<ACCOUNT>` and `<GLUE_DATABASE>`.

| Statement | Why |
| --------- | --- |
| `s3:ListBucket` on both buckets | S3 answers *404* (not *403*) for a missing key only when the caller may list the bucket; the tool relies on it to detect the first export |
| `s3:GetObject` on the data prefix | Parquet footers and DuckLake delete files are read to build the manifests and the position deletes |
| `s3:PutObject` on the data prefix | the flush of inlined rows writes new Parquet files through DuckLake. Drop this statement if you disable data inlining (`data_inlining_row_limit = 0`) |
| **Deny** delete/rewrite on the data prefix | zero copy, enforced: the tool can never delete or rewrite a DuckLake data file |
| `s3:GetObject`/`s3:PutObject` on the metadata prefix | Iceberg manifests, `metadata.json`, position delete files and the export pointer |
| Glue `GetDatabase`, `CreateDatabase`, `GetTable`, `CreateTable`, `UpdateTable` | register the table the first time, then advance its pointer. `CreateDatabase` is only needed if the database does not exist |

Add `kms:Decrypt`/`kms:GenerateDataKey` for SSE-KMS buckets, and
`lakeformation:GetDataAccess` if Lake Formation governs the database. The
DuckLake catalog database (PostgreSQL) needs its own credentials, passed
through the libpq `PG*` environment variables, never in the DSN.

The end-to-end test (`pytest -m minio`) runs the sync with exactly these S3
statements enforced by MinIO and checks that every Glue operation the tool
calls is granted by the Glue statement.

[`iam/metadata-bucket-policy.json`](iam/metadata-bucket-policy.json) is a
bucket policy for the metadata prefix: nobody but the sync role
(`<SYNC_ROLE>`) may write there, which makes the Iceberg table a read-only
mirror for Athena, Spark and every other engine. Readers need `s3:GetObject`
on both prefixes and Glue `GetDatabase`/`GetTable`.

## cron

```cron
# m h dom mon dow   command
*/15 * * * *  ducklake-iceberg sync --table sales.orders --config /etc/ducklake-iceberg.yaml --json >> /var/log/ducklake-iceberg/sales.orders.json 2>> /var/log/ducklake-iceberg/sales.orders.log
```

stdout carries only the JSON envelope (one line per run) and stderr the
structured log, so they can go to different files.

## Docker

The image embeds the DuckDB extensions, so it runs without network access to
the extension repository:

```bash
docker build -t ducklake-iceberg .
docker run --rm \
  -e DUCKLAKE_ICEBERG_CATALOG_DSN="ducklake:postgres:dbname=lake host=pg.internal" \
  -e DUCKLAKE_ICEBERG_DATA_PATH="s3://lake-bucket/ducklake/" \
  -e DUCKLAKE_ICEBERG_ICEBERG_METADATA_PREFIX="s3://lake-bucket/iceberg/" \
  -e DUCKLAKE_ICEBERG_GLUE_DATABASE=analytics \
  -e DUCKLAKE_ICEBERG_AWS_REGION=us-east-1 \
  -e PGUSER -e PGPASSWORD \
  ducklake-iceberg sync --table sales.orders --json
```

## Amazon ECS (Fargate) with EventBridge Scheduler

Task definition (excerpt): the task role carries `iam/sync-policy.json`; the
PostgreSQL password comes from Secrets Manager.

```json
{
  "family": "ducklake-iceberg",
  "requiresCompatibilities": ["FARGATE"],
  "networkMode": "awsvpc",
  "cpu": "1024",
  "memory": "2048",
  "taskRoleArn": "arn:aws:iam::<ACCOUNT>:role/ducklake-iceberg-sync",
  "executionRoleArn": "arn:aws:iam::<ACCOUNT>:role/ecsTaskExecutionRole",
  "containerDefinitions": [
    {
      "name": "ducklake-iceberg",
      "image": "<ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/ducklake-iceberg:0.1.0",
      "essential": true,
      "environment": [
        {"name": "DUCKLAKE_ICEBERG_CATALOG_DSN", "value": "ducklake:postgres:dbname=lake host=pg.internal"},
        {"name": "DUCKLAKE_ICEBERG_DATA_PATH", "value": "s3://lake-bucket/ducklake/"},
        {"name": "DUCKLAKE_ICEBERG_ICEBERG_METADATA_PREFIX", "value": "s3://lake-bucket/iceberg/"},
        {"name": "DUCKLAKE_ICEBERG_GLUE_DATABASE", "value": "analytics"},
        {"name": "DUCKLAKE_ICEBERG_AWS_REGION", "value": "<REGION>"},
        {"name": "PGUSER", "value": "ducklake_iceberg"}
      ],
      "secrets": [
        {"name": "PGPASSWORD", "valueFrom": "arn:aws:secretsmanager:<REGION>:<ACCOUNT>:secret:ducklake-pg"}
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ducklake-iceberg",
          "awslogs-region": "<REGION>",
          "awslogs-stream-prefix": "sync"
        }
      }
    }
  ]
}
```

One schedule per table (EventBridge Scheduler target input):

```json
{
  "containerOverrides": [
    {"name": "ducklake-iceberg", "command": ["sync", "--table", "sales.orders", "--json"]}
  ]
}
```

The per-table lock lives in the local filesystem (`DUCKLAKE_ICEBERG_STATE_DIR`),
so it does not serialize two Fargate tasks of the same table. Do not run the
same table from two places at once: keep a single schedule per table, with an
interval longer than its sync takes. Glue's optimistic locking rejects a
conflicting pointer update, but the two runs could still race on the metadata
files.

## Apache Airflow

```python
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

TABLES = ["sales.orders", "sales.events"]

with DAG(
    "ducklake_iceberg_sync",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,  # never two runs of the same table at once
) as dag:
    for table in TABLES:
        BashOperator(
            task_id=f"sync_{table.replace('.', '_')}",
            # exit 4 (nothing to do) is a success, like 0
            bash_command=(
                f"ducklake-iceberg sync --table {table} --json; "
                "rc=$?; [ $rc -eq 4 ] && exit 0 || exit $rc"
            ),
            env={"DUCKLAKE_ICEBERG_CONFIG_FILE": "/opt/airflow/ducklake-iceberg.yaml"},
            append_env=True,
        )
```

## Monitoring

`ducklake-iceberg sync --table sales.orders --dry-run --json` is read-only and
exits 4 when the mirror is up to date, 0 when there are changes to publish, 3
when the Iceberg table was written by someone else (see the README). Run it from
a separate, more frequent check if you want to alert on lag.
