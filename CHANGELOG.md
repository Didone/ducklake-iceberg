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

### Added

- A fresh-process local benchmark harness with isolated fixtures, exact reader
  audits, durable samples, mean/p50/p95 comparisons and loopback HTTP counts.
- Envelope v1: optional timing counters for attachment modes, transaction read
  reuse, batched reads and SDK HTTP attempts, separate from logical API calls.

### Changed

- Reuse one DuckDB root during sync, including flush through read-only/write/
  read-only attachments; freshly validate the source after each write.
- Use single GETs for metadata reads and conditional single PUTs for small
  generated objects; large outputs use bounded multipart uploads. Source
  Parquet reads keep native ranged I/O.
- Share a consistent read transaction across preflight and export, materialize
  file statistics with enumeration and batch independent compatibility reads.

### Fixed

- Validate the source UUID and data path before the unchanged-export shortcut;
  older handoffs without these fields take the full identity-validation path.
- Reject credentials in DSNs with whitespace around assignments or URL user
  information, and redact complete quoted secrets in logs and envelope v1.
- Recover a completed first export after a failed handoff write, and repair
  recovered successor handoffs without rewriting immutable artifacts. Recovery
  verifies source provenance and manifests; dry-run remains read-only.
- Preserve metadata ancestry when expiring snapshots so pending Glue updates
  remain recoverable across the retention window.
- Resolve historical partition specs with historical schemas after a partition
  source column is dropped, including first exports and subsequent deletes.
- Reject metadata prefixes overlapping actual source locations from the catalog,
  even when the configured data path differs from the source.
- Preserve quoted schema/table identifiers throughout sync, flush and export.

## [0.1.1rc1] - 2026-11-10

### Added

- Envelope v1: optional `report` in doctor steps preserves rule diagnostics,
  including errors detected after flushing inlined data.
- Envelope v1: detailed catalog connection/query, inlining, flush, delete and
  S3 operation timings and query/cache counters in the existing `timings`
  field, without recording SQL text or parameters.

### Fixed

- Stop with exit 1 when post-flush compatibility revalidation errors instead
  of continuing to export and register.
- Envelope v1: no-op results now identify the existing completed metadata
  file/version rather than an unwritten next version.
- Retry transient Glue pointer updates using the shared bounded policy,
  reconciling uncertain outcomes and rechecking ancestry and `VersionId`.
- Share partition-transform classification between doctor and export.
- Validate published sync-state format, version and source identity before
  writing an incremental export.

### Changed

- Reuse inlining detection and physical-table discovery within a consistent
  read, without retaining results across transactions or after a flush.
- Share one live-delete-file query between the Parquet and Puffin doctor
  rules, with a fresh query cache for each doctor invocation.
- Read the catalog fingerprint in one query instead of three, and skip it
  when there is no pending inlined data to flush.
- Load installed DuckDB extensions directly; install only when missing.
- Probe catalog availability without counting the entire snapshot history.
- Separate shared handoff/storage contracts from Glue registration and remove
  disconnected internal state-loading/reconciliation APIs. CLI flags and
  exit-code meanings are unchanged.

## [0.1.0rc1] - 2026-11-10

First release candidate, published to PyPI to rehearse the release process.

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
- `--dry-run` as a read-only monitor: plans the export, reports pending
  inlined rows and checks Glue (exit 3 on an external write).
- `--json` envelope v1 with a published JSON schema, and a
  structured log on stderr with credential redaction.
- Per-phase timings and counters in each result; `DUCKLAKE_ICEBERG_TIMINGS=1`
  adds a `timings` key to the envelope.
- `s3_endpoint` setting for S3-compatible stores such as MinIO (Iceberg metadata
  I/O and DuckDB S3 secret, path-style addressing).
- Concurrency limits (`max_footer_workers`, `max_io_workers`,
  `max_total_workers`), `snapshot_retention_hours` to expire old Iceberg
  snapshots, and `duckdb_extension_directory` to run with a read-only `HOME`.
- Multi-stage Docker image with the DuckDB extensions embedded, and a
  `python -m ducklake_iceberg.deps` check of the pinned dependency and
  extension builds.
- Typed package (`py.typed`).
- Works with Typer 0.27 to 0.29 (no direct import of Click, which Typer 0.29
  vendors).

[Unreleased]: https://github.com/Didone/ducklake-iceberg/compare/v0.1.1rc1...HEAD
[0.1.1rc1]: https://github.com/Didone/ducklake-iceberg/compare/v0.1.0rc1...v0.1.1rc1
[0.1.0rc1]: https://github.com/Didone/ducklake-iceberg/releases/tag/v0.1.0rc1
