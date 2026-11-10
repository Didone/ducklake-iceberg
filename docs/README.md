# Documentation

`ducklake-iceberg` publishes a **read-only Apache Iceberg v2 mirror of a
DuckLake table in AWS Glue**, referencing the original Parquet files rather
than copying them. DuckLake remains the owner of the data and the write path.

## Choose a learning path

| Your goal | Read in this order |
| --- | --- |
| Understand the project without prior lakehouse knowledge | [Concepts](concepts.md), [Getting started](getting-started.md), [Usage](usage.md) |
| Configure and operate a real installation | [Configuration](configuration.md), [Compatibility](compatibility.md), [Deployment](deploy/README.md), [Operations](operations.md) |
| Understand or change the implementation | [Architecture](architecture.md), [Implementation and class reference](implementation.md), [Testing](testing.md) |
| Build and publish a release | [Testing and delivery](testing.md#continuous-integration-and-releases), [Release policy](../CONTRIBUTING.md#releasing) |

## Guide map

- [Concepts](concepts.md): files, table formats, catalogs, snapshots, field
  IDs, partitioning and position deletes, starting from first principles.
- [Getting started](getting-started.md): installation, a local learning
  exercise without AWS, and the first real publication.
- [Usage](usage.md): all CLI options, examples, exit codes and JSON output.
- [Configuration](configuration.md): every setting, defaults, validation,
  environment variables, credentials and the implementation that consumes it.
- [Compatibility](compatibility.md): supported types, schema/partition
  evolution, pre-flight rules and explicit blockers.
- [Architecture](architecture.md): the complete read/write lifecycle,
  incremental algorithm, artifacts, failure boundaries and invariants.
- [Implementation](implementation.md): every production class, its role,
  important attributes/methods, collaborators and related tests; also the
  function-oriented modules and internal extension points.
- [Testing](testing.md): setup, test layers, fixtures, selective commands,
  coverage, dependency pins, CI and release validation.
- [Deployment](deploy/README.md): permissions, containers, cron,
  ECS/EventBridge and Airflow.
- [Operations](operations.md): monitoring, incident diagnosis, recovery,
  retention, concurrency and performance.

Examples use `sales.orders`, Glue database `analytics`, region `us-east-1`,
and clearly illustrative bucket names. Replace them with your own resources.
Never paste credentials into configuration files, source control or tickets.

## Scope and support boundaries

The public command is `ducklake-iceberg sync --table schema.table`.
`doctor`, `checkpoint`, `export` and `register` are **implementation stages**,
not independent CLI commands. Some internal docstrings retain historical
command names; use the [CLI reference](usage.md) for executable syntax.

Python 3.11 through 3.14 are declared in the package. The process lock uses
POSIX `fcntl.flock`; use Linux, macOS or WSL/container execution rather than
assuming native Windows support. SQLite and PostgreSQL are the tested
DuckLake catalog backends. Recognition of other backend names in connection
code is not a tested support guarantee.

Iceberg format version is 2. Compatibility of individual engines and their
versions still needs validation with your workload; publishing metadata is
not a guarantee that every reader supports every represented type.

The source of truth for runtime behavior is
[`src/ducklake_iceberg`](../src/ducklake_iceberg); package constraints and
validation commands are in [`pyproject.toml`](../pyproject.toml).
See [CHANGELOG](../CHANGELOG.md) for version history,
[CONTRIBUTING](../CONTRIBUTING.md) for contributor conventions and
[SECURITY](../SECURITY.md) for vulnerability reporting.
