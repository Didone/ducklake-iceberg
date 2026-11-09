# Changelog

All notable changes to `ducklake-iceberg` are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Public contracts covered by the version number: the CLI (commands and flags),
the exit codes (`ExitCode`: 0, 1, 2, 3, 4), the `--json` envelope (its
`version` field and the schema in `src/ducklake_iceberg/schemas/envelope-v1.json`) and
the log schema (`log_schema_version`). The envelope and the log have their
**own** integer, monotonic version numbers: adding an optional key keeps the
version and is listed here under `Added`; renaming or removing a key, or
changing a key's type, meaning or the exit code to `status` map, bumps the
envelope version, is listed under `Changed` and requires at least a minor
release of the package (a major release from 1.0.0 on).

Every envelope or log entry must name the affected contract version, for
example: "envelope v1: optional key `timings`".

## [Unreleased]

## [0.1.0] - 2026-11-09

First public release.

### Added

- `ducklake-iceberg sync --table schema.table`: publishes a DuckLake table as an
  Apache Iceberg v2 table in the AWS Glue Data Catalog, without copying data.
  Designed to run from cron: it is idempotent, takes a per-table lock and
  exits with code 4 when there is nothing to do.
- Stages: Iceberg v2 compatibility pre-flight, flush of inlined data (the only
  write to DuckLake), incremental export of manifests and `vN.metadata.json`,
  and Glue registration with optimistic locking.
- Exit codes: 0 published, 1 error, 2 blocked by the pre-flight, 3 diverged
  (the Iceberg table was modified outside of ducklake-iceberg), 4 nothing to do.
- DuckLake `DELETE`, `UPDATE` and `MERGE` exported as Iceberg v2 position
  delete files.
- Iceberg field IDs equal to DuckLake `column_id`s, including nested types,
  renames and dropped columns.
- `--dry-run`, `--json` envelope v1 with a published JSON schema, and a
  structured log on stderr with credential redaction.
- Per-phase timings and counters in each result; `DUCKLAKE_ICEBERG_TIMINGS=1`
  adds a `timings` key to the envelope.
- Concurrency limits (`max_footer_workers`, `max_io_workers`,
  `max_total_workers`), `snapshot_retention_hours` to expire old Iceberg
  snapshots, and `duckdb_extension_directory` to run with a read-only `HOME`.
- Multi-stage Docker image with the DuckDB extensions embedded, and a
  `python -m ducklake_iceberg.deps` check of the pinned dependency and
  extension builds.
- Typed package (`py.typed`).

[Unreleased]: https://github.com/Didone/ducklake-iceberg/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Didone/ducklake-iceberg/releases/tag/v0.1.0
