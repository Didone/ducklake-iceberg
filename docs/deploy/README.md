# Deploying ducklake-iceberg

[Documentation home](../README.md) | [Configuration](../configuration.md) |
[Operations](../operations.md) | [Testing](../testing.md)

`ducklake-iceberg sync --table schema.table` is a short-lived, idempotent
command: schedule one run per table with whatever you already use. Runs of
different tables can overlap. A non-blocking per-table lock rejects a second
run of the same destination sharing the same local lock directory with exit
code 1; retry after the first run finishes.

Exit codes to alert on: **1** (error, retry after checking the cause), **2** (blocked by the
pre-flight: the table needs a change at the source) and **3** (the Iceberg
mirror was written by someone else). **0** and **4** are success.

## Deployment checklist

1. Select the existing source catalog, source root, destination prefix,
   Glue database/region and exact package/image version.
2. Verify [compatibility](../compatibility.md), including column defaults,
   deletes, source cleanup retention and supported partition semantics.
3. Configure catalog access, AWS task/profile credentials, S3/Glue permissions
   and network routes. Application configuration does not provision them.
4. Ensure the source and metadata prefixes do not overlap. If there are
   same-named source tables in different schemas, choose separate destinations.
5. Provide writable local lock state and readable matching DuckDB extensions.
6. Run a dry-run, then one controlled normal sync. Verify Glue and reader rows.
7. Configure one schedule owner per destination and prevent cross-host overlap.
8. Route stdout/stderr separately, normalize exit 4 for your scheduler and
   alert on 1/2/3. Test retries and a publication failure before broad rollout.

The application settings, environment names, defaults, validation and source
consumers are exhaustively listed in [Configuration](../configuration.md).
The examples below deliberately do not embed passwords or AWS keys.

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
| **Deny** deletion and ACL/tag/retention mutations on the data prefix | prevents source deletion and related mutations. `PutObject` remains allowed for new flush files; IAM does not by itself prohibit every overwrite. Existing-file immutability also relies on the implementation and zero-copy tests. |
| `s3:GetObject`/`s3:PutObject` on the metadata prefix | Iceberg manifests, `metadata.json`, position delete files and the export pointer |
| Glue `GetDatabase`, `CreateDatabase`, `GetTable`, `CreateTable`, `UpdateTable` | register the table the first time, then advance its pointer. `CreateDatabase` is only needed if the database does not exist |

Add `kms:Decrypt`/`kms:GenerateDataKey` for SSE-KMS buckets, and
`lakeformation:GetDataAccess` if Lake Formation governs the database. The
DuckLake catalog database (PostgreSQL) needs its own credentials, passed
through the libpq `PG*` environment variables, never in the DSN.

Do not grant routine readers the sync role. Readers need Glue discovery and
read access to both source data and Iceberg metadata/delete prefixes, plus
their own KMS/Lake Formation permissions when applicable.
Database users must be able to read DuckLake metadata and perform the
table-scoped inlined-data flush if pending rows exist; a read-only PostgreSQL
user is not sufficient for that path.

S3 buckets, PostgreSQL users, Glue governance and encryption keys are
provisioned outside this repository. Replace every policy placeholder before
attaching a policy. The JSON files are templates, not deployable credentials.
Bucket-policy explicit denies can affect administrators/other services too:
review the sync-role exception and recovery procedure before application.

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

Cron does not reinterpret exit 4 as success for external monitors. Use this
shell wrapper if your supervisor treats every nonzero code as failure:

```bash
rc=0
ducklake-iceberg sync --table sales.orders --json || rc=$?
case "$rc" in
  0|4) exit 0 ;;
  *) exit "$rc" ;;
esac
```

Set a predictable PATH/config/credential context for cron and create the log
directory with appropriate ownership. Never discard stderr in production.
The lock is non-blocking: overlapping same-host jobs fail explicitly rather
than queuing indefinitely.

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

For a local Docker run, environment variables do not automatically provide
AWS credentials. Use the host's approved workload mechanism or mount a
read-only AWS config/credential directory and select a profile. Do not copy
credential files into the image. In ECS, prefer task-role credentials instead.

To pass a YAML file:

```bash
docker run --rm \
  --mount type=bind,src=/absolute/path/ducklake-iceberg.yaml,dst=/config/config.yaml,readonly \
  -e PGUSER -e PGPASSWORD \
  ducklake-iceberg sync --table sales.orders --config /config/config.yaml --json
```

This still needs your workload's AWS credentials and network. A host's
`localhost` is not generally the same endpoint as a container's `localhost`.
For MinIO use the service DNS address reachable from the container and set
`DUCKLAKE_ICEBERG_S3_ENDPOINT` plus that store's credential inputs; Glue still
needs separate real/simulated configuration.

### Image build and runtime settings

The repository supplies a Dockerfile, not a workflow publishing a prebuilt
container registry image. Build/push your own image:

```bash
docker build \
  --build-arg VERSION=0.1.0rc1 \
  --build-arg REVISION="$(git rev-parse HEAD)" \
  --build-arg SOURCE=https://github.com/Didone/ducklake-iceberg \
  --build-arg CREATED="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -t ducklake-iceberg:0.1.0rc1 .
docker run --rm --network none ducklake-iceberg:0.1.0rc1 --version
docker run --rm --network none ducklake-iceberg:0.1.0rc1 sync --help
```

| Build/runtime item | Meaning |
| --- | --- |
| `VERSION` build arg | OCI version label, default `0.0.0`; does not change `pyproject.toml` or the installed package version |
| `REVISION`, `SOURCE`, `CREATED` build args | OCI provenance labels; defaults `unknown` |
| `POETRY_VERSION` build arg | Builder's Poetry version, default as declared in Dockerfile; not a runtime setting |
| Runtime base | Python 3.12 slim Debian image; builder creates `/opt/venv` and installs locked main dependencies |
| User | UID/GID `10001:10001`, not root; mounted files/directories must be accessible |
| `HOME` | `/home/ducklake-iceberg`; embedded extensions live below it |
| Extension setting | Image sets `DUCKLAKE_ICEBERG_DUCKDB_EXTENSION_DIRECTORY` to the root of the embedded version/platform tree |
| Entrypoint / default command | `ducklake-iceberg` / `--help`; pass `sync ...` to run a publication |

Build installs `ducklake`, `postgres`, `httpfs` and `aws`, checks loading
without network, verifies dependency/extension pins and smoke-tests the CLI.
The runtime still needs network to PostgreSQL, S3, Glue and credential
providers; "offline smoke" does not mean an offline production sync.
The image does not preinstall the SQLite extension: a SQLite deployment
needs that matching extension installed in a derived image/cache as well.

For a read-only root filesystem, provide a writable state mount or tmpfs and
set `DUCKLAKE_ICEBERG_STATE_DIR` to it. Keep the extension directory readable.
Temporary files or local source/catalog access may require additional
writable mounts according to your workload. The local lock volume is not
a replacement for distributed scheduling coordination.

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
      "image": "<ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/ducklake-iceberg:0.1.0rc1",
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

Replace the account/region/image/version and resource names; the JSON is a
task-definition excerpt, not a complete infrastructure deployment.

| ECS setting | Why it is needed |
| --- | --- |
| `family` | Names task-definition revisions |
| `requiresCompatibilities`, `networkMode` | Fargate and per-task networking (`awsvpc`) |
| `cpu`, `memory` | Task capacity; reconcile with DuckDB threads/memory and Python/Arrow pools |
| `taskRoleArn` | Runtime application's S3/Glue/KMS/Lake Formation permissions |
| `executionRoleArn` | ECS pulling image, retrieving injected secrets and delivering logs; distinct from the task role |
| `environment` | Non-secret application and connection values |
| `secrets` | Injects PostgreSQL password from Secrets Manager; execution role needs appropriate secret/KMS access |
| `logConfiguration` | CloudWatch delivery; provision the log group and retention/access policy |
| Container `command` override | Arguments following the image's `ducklake-iceberg` entrypoint |

Push the built image to ECR using your approved registry workflow. Configure
task subnets/security groups for PostgreSQL, S3, Glue, ECR, CloudWatch,
Secrets Manager and required credential endpoints using VPC endpoints/NAT as
appropriate. The scheduler needs its own execution role allowing `ecs:RunTask`
and constrained `iam:PassRole` for the task/execution roles.
Do not confuse that scheduler role with the application's task role.

In EventBridge Scheduler configure cluster/task-definition ARN, launch type,
network settings, `FlexibleTimeWindow`/schedule, retry age/count and a DLQ
where appropriate, in addition to the container override input below.
One interval longer than average runtime alone is not hard mutual exclusion:
bound retries, handle delayed deliveries and enforce one in-flight execution
per table in the orchestration design.

EventBridge successfully starting an ECS task is not evidence that the task
later synced successfully. Monitor ECS stopped-task/container exit events.
Exit 4 is expected for an up-to-date mirror; normalize it with a wrapper
entrypoint if your external monitor cannot distinguish it from a failure.

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

This is an Airflow 2-style DAG example; adapt DAG/BashOperator imports for
your Airflow/provider version. Airflow is not a package dependency. Install
the application in the task execution environment, supply its configuration
file and inject catalog/AWS credentials using the platform's secret mechanism.

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
                "rc=0; "
                f"ducklake-iceberg sync --table {table} --json || rc=$?; "
                'case "$rc" in 0|4) exit 0 ;; *) exit "$rc" ;; esac'
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

See [Operations](../operations.md) for dry-run limitations, failure-boundary
recovery, retained-file safety, metric fields and diagnosis by problem code.
