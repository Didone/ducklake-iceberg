# Contributing to ducklake-iceberg

This utility publishes a read-only Iceberg v2 mirror of a DuckLake table in
AWS Glue. It references the source Parquet files; it does not copy or own them.
Contributions are licensed under the [MIT License](LICENSE).

## Start locally

Use Python 3.11–3.14 and Poetry 2.x:

```bash
git clone https://github.com/Didone/ducklake-iceberg.git
cd ducklake-iceberg
poetry install
poetry run ducklake-iceberg --help
poetry run pytest tests/unit/test_config.py --no-cov
```

Follow the [local walkthrough](docs/getting-started.md) to create a source table.
Configuration loading and unit tests need no AWS account. SQLite integration
uses real local catalogs; PostgreSQL and MinIO tests require Docker. Extensions
need network access once unless already installed. Test setup, markers and
troubleshooting live in [docs/testing.md](docs/testing.md).

## Understand one sync

A **snapshot** identifies a committed table state. **Field IDs** identify columns
across renames. A **position delete** hides specific rows without rewriting source
data. The **handoff** points to a completed export that Glue publication can retry.

`cli.py` loads settings and calls `pipeline.sync_table`. Under a local table lock:

1. **Doctor** checks one consistent source view. Unsupported data blocks sync.
2. **Flush**, only when needed, writes pending inline rows/deletes to source
   files. The same DuckDB root is reattached RO → RW → RO, with identity checks
   before writing and fresh validation afterward.
3. **Export** translates schema/partitions, validates the live files and writes
   Iceberg metadata and necessary delete files, followed by the handoff.
4. **Register** checks lineage and updates Glue optimistically. Even a no-op
   export reaches registration so an earlier failed publication can recover.

A blocker, divergence or error stops publication. `--dry-run` checks without
flushing or publishing. No transaction spans DuckLake, S3 and Glue. The full
failure/recovery contract is in [operations](docs/operations.md); the read/write
boundaries are in [architecture](docs/architecture.md).

## Find the code to change

Paths below are under `src/ducklake_iceberg/`.

| Responsibility | Start here | Relevant tests |
| --- | --- | --- |
| Flags and configuration | `cli.py`, `config.py` | `tests/unit/test_config.py`, `tests/integration/test_cli.py` |
| Source view and files | `catalog/`, `checkpoint/` | `tests/catalog/`, connection/flush/transaction integration tests |
| Compatibility rules | `doctor/` | `tests/doctor/`, `tests/integration/test_rules.py` |
| Types, columns and partitions | `mapping/` | `tests/mapping/`, type/partition integration tests |
| Artifacts, deletes and recovery | `export.py`, `writer/`, `deletes/`, `handoff.py` | writer/delete/incremental/recovery tests |
| Glue publication | `register.py`, `glue.py` | `tests/test_glue.py`, `tests/test_export_register.py` |
| Errors, JSON, logs and credentials | `errors.py`, `output.py`, `obs/`, `aws.py`, `storage.py` | envelope/redaction/credential tests |

The [implementation reference](docs/implementation.md) describes individual
classes/functions. Each module should own a coherent responsibility; prefer a
direct call to an unnecessary forwarding layer or new generic abstraction.

## Make a small change

For a configuration-validation fix:

1. Read the setting and validators in `config.py`, then the corresponding
   examples in [configuration](docs/configuration.md).
2. Add a regression case to `tests/unit/test_config.py`. For example, extend
   the invalid-endpoint cases if fixing endpoint validation. Run the focused
   test and confirm the new case fails for the intended reason before fixing it.
3. Make the smallest correction; preserve valid inputs, defaults and precedence
   (CLI > environment > YAML > default). Run the focused tests again.
4. Check CLI/envelope effects, update the relevant documentation and changelog,
   then run quality and applicable integration checks.

Use American English and Conventional Commit messages. Test observable behavior,
not a copy of the implementation. For persisted artifacts, check exact reader
values, IDs and deletes, not just file existence. Type/schema changes must retain
SQLite/PostgreSQL parity and supported evolution behavior.

## Preserve the contracts

- Never copy, overwrite or delete source Parquet. Only table-scoped inline flush
  may write source; no source compaction, expiry or cleanup.
- Preserve field IDs and Iceberg v2, snapshot visibility, delete applicability,
  validated enumeration and immutable metadata outside source paths. Use
  `register_table`, not an API that reassigns IDs. Reject unsupported inputs.
- Keep source identity, lineage, optimistic publication, bounded retries,
  recovery and read-only mirror protection. Local locks are not distributed locks.
- Preserve exit codes 0 success, 1 error, 2 blocked, 3 diverged, 4 nothing to do;
  public exception names; stdout JSON/stderr logs; envelope/log version rules.
  See [usage](docs/usage.md) and [CHANGELOG](CHANGELOG.md) for public contracts.
- Bind SQL values; validate/quote identifiers. Keep credentials out of SQL DSNs
  and logs; PostgreSQL uses libpq `PG*`. No AWS credentials in repository files.
- Route worker limits through `concurrency.py`; reset process caches, credentials,
  metrics and logging in test fixtures. Do not share DuckDB connections across threads.
- Keep exact dependency/extension pins. Pin changes need explicit compatibility
  validation, including round trips, as described in [testing](docs/testing.md#dependency-pins-and-extension-changes).

## Validate

```bash
poetry check --lock
poetry run ruff check .
poetry run ruff format --check .
poetry run mypy
poetry run pytest -m "not postgres and not e2e and not minio"
poetry run pytest -m postgres --cov-fail-under=0
poetry run pytest -m minio --cov-fail-under=0
```

The fast suite enforces >=85% coverage; focused runs can use `--no-cov`.
The subset coverage override does not weaken the full-suite gate. Warnings are
errors. Real-AWS `e2e` runs need explicit authorization. Benchmark tools and
measurement limits are documented in [testing](docs/testing.md#fresh-process-local-sync-benchmark).

## Releasing

Release only a commit whose CI passed. Choose the version, update
`pyproject.toml` and move Unreleased entries into the dated version section of
`CHANGELOG.md`. Validate the clean distributions and container, then create/push
the matching `vX.Y.Z` tag. The [release checklist](docs/testing.md#continuous-integration-and-releases)
has the exact commands, Trusted Publishing setup and post-publication checks.
Candidates such as `vX.Y.ZrcN` also publish to real PyPI and are marked GitHub
pre-releases. Existing PyPI versions/files cannot be overwritten.
