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

### Fixed

- `checkpoint` no longer warns about other tables' inlined-data tables when
  detecting inlined rows (the introspection is catalog-wide).

## [0.1.0] - 2026-11-09

First public release.

### Added

- Commands: `doctor`, `checkpoint`, `export`, `register`, `sync`, `status`,
  `validate` and `clean`.
- Zero-copy publication of DuckLake tables as Apache Iceberg v2 tables in the
  AWS Glue Data Catalog: Iceberg manifests reference the Parquet files DuckLake
  already wrote.
- Incremental sync with one Iceberg snapshot per DuckLake table checkpoint;
  exit code 4 when there is nothing to do and exit code 3 when the Iceberg
  table was modified outside of ducklake-iceberg.
- DuckLake `DELETE`, `UPDATE` and `MERGE` exported as Iceberg v2 position
  delete files, and `--materialize-deletes` to rewrite data files instead.
- Iceberg field IDs equal to DuckLake `column_id`s, including nested types,
  renames and dropped columns.
- Iceberg v2 compatibility pre-flight (`doctor`) with a rule registry.
- `--json` envelope v1 with a published JSON schema, `--json-version`, and a
  structured log on stderr with credential redaction.
- `--metrics` per-phase timings and counters; `DUCKLAKE_ICEBERG_TIMINGS=1` adds a
  `timings` key to the envelope.
- Concurrency controls: `max_table_workers`, `max_footer_workers`,
  `max_io_workers` and `max_total_workers`.
- Glue column statistics publication (`sync --glue-stats on`).
- `duckdb_extension_directory` setting (`DUCKLAKE_ICEBERG_DUCKDB_EXTENSION_DIRECTORY`)
  to run with a read-only `HOME`.
- Multi-stage Docker image with the DuckDB extensions embedded, and a
  `python -m ducklake_iceberg.deps` check of the pinned dependency and extension builds.
- Typed package (`py.typed`).

[Unreleased]: https://github.com/Didone/ducklake-iceberg/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Didone/ducklake-iceberg/releases/tag/v0.1.0
