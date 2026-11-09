# Contributing to iceduck

Thanks for your interest in improving `iceduck`! Bug reports, feature requests
and pull requests are welcome.

By participating you agree that your contributions are licensed under the
[MIT License](LICENSE).

## Development setup

The project uses [Poetry](https://python-poetry.org/) (2.x), a `src/` layout and
supports Python 3.11 to 3.14.

```bash
git clone https://github.com/Didone/iceduck.git
cd iceduck
poetry install
```

### Quality checks

These are the same checks that CI runs:

```bash
poetry check --lock                 # poetry.lock must match pyproject.toml
poetry run ruff check .
poetry run ruff format --check .
poetry run mypy                     # strict; checks src and tests
```

### Tests

```bash
# Fast path: SQLite catalog and moto (what CI runs on every Python version)
poetry run pytest -m "not postgres and not e2e and not minio"

# Postgres catalog through testcontainers (needs Docker)
poetry run pytest -m postgres --cov-fail-under=0

# DuckLake file cleanup against MinIO (needs Docker)
poetry run pytest -m minio --cov-fail-under=0

# A single test (disable the coverage gate or the run fails)
poetry run pytest tests/sync/test_delta.py::test_name --no-cov
poetry run pytest -k "some_pattern" --no-cov
```

pytest is configured with `--strict-markers`, a coverage gate of 85% and
`filterwarnings = ["error"]`, so any warning fails a test.

Markers:

| Marker        | Meaning |
| ------------- | ------- |
| `integration` | Needs a real DuckLake catalog (SQLite by default) |
| `postgres`    | Needs a Postgres container (testcontainers) |
| `minio`       | Needs a MinIO container (testcontainers + Docker) |
| `aws`         | Uses simulated S3/Glue (moto) |
| `e2e`         | Needs a real AWS account; never runs in CI |
| `roundtrip`   | Writer output is read back by the PyIceberg reader |
| `pins`        | Checks the pinned DuckDB, extension and PyIceberg versions |

The `tests/integration` `catalog` fixture runs the **same** suite against both
the SQLite and the Postgres backends; that parity is an acceptance requirement.

### Conventions

- Code comments, docstrings, messages and documentation are written in
  American English (en-US).
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)
  (`feat:`, `fix:`, `docs:`, `test:`, `chore:`, ...).
- Add or update tests for every behavior change, and an entry under
  `## [Unreleased]` in [CHANGELOG.md](CHANGELOG.md) for user-visible changes.

## Architecture notes

`iceduck` is a Typer CLI that publishes DuckLake tables as Iceberg v2 tables in
the AWS Glue Data Catalog **without copying data**. Sync is one-way
(DuckLake to Iceberg).

### Pipeline

`sync` chains the per-table steps, which also exist as standalone commands,
under a per-table process lock (`sync/lock.py`):

1. **doctor** (`doctor/`): Iceberg v2 compatibility pre-flight. Rules live in
   `doctor/rules/` and are registered in `doctor/registry.py`. A blocker exits
   with code 2.
2. **checkpoint** (`checkpoint/`): the only code that **writes to DuckLake**.
   In `sync` it only flushes pending inlined data; the full expire + merge +
   rewrite runs only with `checkpoint` or `sync --compact`.
3. **export** (`export.py`): reads the catalog, computes the delta against the
   last exported snapshot (`sync/delta.py`, `sync/schema_evolution.py`) and
   writes the Iceberg metadata through `writer/` (Avro manifests and manifest
   lists via fastavro, then `vN.metadata.json`). DuckLake deletes become v2
   position delete files (`deletes/`). Exits with code 4 when nothing changed.
4. **register** (`register.py`, `glue.py`): `register_table` the first time,
   then a Glue `UpdateTable` with optimistic locking. If Glue points at a
   `metadata.json` that is not an ancestor of ours (an external write), it
   exits with code 3.

The sync state (the last exported snapshot, etc.) is stored in the Iceberg
table properties (`sync/state.py`), not in local files. The remaining commands
are `status`, `validate` (DuckLake and Iceberg reconciliation) and `clean`
(garbage collection of unreachable Iceberg metadata, see
`maintenance/orphans.py` and `retention.py`).

### Exit codes and output contract

`ExitCode` in `errors.py` is a public contract: 0 success, 1 error, 2 blocked
(compatibility), 3 diverged, 4 nothing to do. Exceptions carry their exit code
(`IceduckError` subclasses). `--json` emits a versioned envelope (`output.py`,
schema `src/iceduck/schemas/envelope-v1.json`). Optional keys may be added;
renaming or removing keys, or changing a key's type or meaning, requires a new
envelope version. Logs go to stderr (`obs/`, with credential redaction in
`obs/redaction.py`).

Exception names such as `NothingToDo`, `RecordCountMismatch` and
`TableAlreadyRegistered` are part of the public contract. Do not rename them.

### Design decisions that must not be broken

- **Zero-copy.** Data Parquet files are never copied or deleted by iceduck.
  Only delete files are rewritten. Physical cleanup of data files is DuckLake's
  job.
- **Own metadata writer.** PyIceberg is used as the data model and reader, but
  its `add_files` fails on DuckLake Parquet files
  (`_ducklake_internal_row_id`), so manifests are written by hand. Every Avro
  file written must round-trip through PyIceberg's reader in tests.
- **`register_table`, never `create_table`.** `create_table` reassigns field
  IDs. Iceberg field IDs must equal DuckLake `column_id`s (including nested
  types, renames and drops), otherwise readers silently return null columns.
- **Read-only mirror.** External writes to the Iceberg table are detected and
  block the sync.
- **Metadata location.** Iceberg metadata lives under `iceberg_metadata_prefix`,
  **outside** DuckLake's `data_path`; otherwise DuckLake's orphan cleanup would
  delete it.
- **Catalog access only through DuckDB** (`catalog/connection.py`), never
  through psycopg. The catalog is attached with an explicit
  `METADATA_CATALOG` so that `lake_meta.ducklake_*` tables are queryable.
  Connections are `READ_ONLY` except for checkpoint, which also prevents a
  mistyped DSN from silently creating an empty lake. Reads run inside a
  transaction for a consistent view. File enumeration uses
  `ducklake_list_files(...)`, cross-checked against `ducklake_data_file`; any
  divergence aborts.
- **One DuckLake table checkpoint is one Iceberg snapshot.**
- **Iceberg v2 only**, because Athena only supports v2: no deletion vectors,
  variant, geometry or nanosecond timestamps.

### SQL safety

DuckDB cannot bind identifiers, so SQL interpolates only identifiers validated
at configuration load time (`metadata_catalog` and `catalog_alias`, see
`config.py`) or passed through `quote_ident`. All values go through named
parameters (`$table_id`, `$snapshot_id`, ...). Each module that interpolates SQL
has a per-file `S608` ignore in `pyproject.toml` with a justification comment;
adding one requires the same justification. Credentials never go in the DSN;
PostgreSQL uses the libpq `PG*` environment variables.

### Process-level caches

boto3 sessions and clients (`aws.py`), the PyIceberg FileIO cache
(`register.py`) and the `timing.py` counters are process-global.
`tests/conftest.py` resets them around every test; a new process-level cache
needs the same reset. Concurrency limits are centralized in `concurrency.py`.

Configuration (`config.py`, pydantic-settings) is resolved as: CLI flag >
`ICEDUCK_*` environment variable > `iceduck.yaml` > default.

### Performance measurements

`ICEDUCK_TIMINGS=1` adds a `timings` key (per-phase milliseconds and counters
for DuckDB connections, boto3 sessions and AWS calls) to the `--json` envelope.
`scripts/bench_sync.py` runs a command N times and reports p50/p95. On WSL,
benchmark from the Linux filesystem (not `/mnt/c`) and call the virtualenv
executable directly instead of `poetry run`.

## Pinned dependencies

`duckdb` and `pyiceberg` are pinned to exact versions in `pyproject.toml`,
and the extension builds (`ducklake`, `postgres`, `httpfs`, `aws`) are pinned
in `src/iceduck/deps.py`. `python -m iceduck.deps` compares the environment
with those pins (the Docker image build runs it), and the weekly,
non-blocking `compat-latest` workflow runs the suite against the newest
versions.

To bump them:

1. Update the `==` pin in `pyproject.toml` and run `poetry lock`.
2. Update `src/iceduck/deps.py` with the new extension hashes.
3. Run `poetry run pytest -m "pins or roundtrip"` and the full suite.

## Releasing

Versions follow [Semantic Versioning](https://semver.org/): `X.Y.Z` with an
optional pre-release suffix `aN`, `bN` or `rcN`. Before 1.0.0, breaking changes
to the CLI, exit codes or envelope bump the minor version.

1. Move the `## [Unreleased]` entries of [CHANGELOG.md](CHANGELOG.md) under a
   new `## [X.Y.Z] - YYYY-MM-DD` section and update the compare links.
2. Set `version = "X.Y.Z"` in `pyproject.toml`.
3. Verify locally:

   ```bash
   poetry check --lock
   poetry build
   poetry run twine check --strict dist/*
   python scripts/check_release_version.py vX.Y.Z --changelog CHANGELOG.md
   ```

4. Merge to `main`, then tag and push: `git tag vX.Y.Z && git push origin vX.Y.Z`.

The `release` workflow validates that the tag matches `pyproject.toml` and the
changelog, builds the sdist and wheel, smoke-tests the wheel and the container
image, publishes to PyPI with
[Trusted Publishing](https://docs.pypi.org/trusted-publishers/) (no API token
is stored) and creates the GitHub release.

One-time setup for maintainers:

- On PyPI, add a *trusted publisher* for the repository `Didone/iceduck`,
  workflow `release.yml`, environment `pypi`.
- In GitHub, create the `pypi` environment (optionally with required
  reviewers).
