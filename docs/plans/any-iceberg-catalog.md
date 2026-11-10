# Plan: publish to any Iceberg catalog, not only AWS Glue

Status: approved direction, spike in progress (branch
`feat/any-iceberg-catalog`).

Decisions (2026-11-10):

* Glue moves to commit mode in 0.2: one publication path for every catalog,
  no pointer mode. The pointer file and the raw boto3 `UpdateTable` go away.
* Priority catalog: Databricks Unity Catalog. No Databricks workspace is
  available yet, so the spike validates the protocol against other servers and
  lists what only a real workspace can confirm.

## 1. Goal

`ducklake-iceberg sync --table schema.table` should publish the DuckLake table
to any Iceberg catalog that PyIceberg can talk to: the Iceberg REST catalog
(Apache Polaris, Snowflake Open Catalog, Nessie, Lakekeeper, Gravitino,
Databricks Unity Catalog's Iceberg REST endpoint, ...), the SQL catalog, Hive,
and AWS Glue as today. Zero copy stays the core rule: the tool writes only
Iceberg metadata (manifests, manifest lists and position delete files) and never
copies, rewrites or deletes a DuckLake data file.

Non-goals for this work: new storage backends beyond what PyIceberg's FileIO
already supports, Iceberg v3, and catalog administration (creating warehouses,
principals or grants).

## 2. Where Glue is wired in today

| Concern | Where | Notes |
| --- | --- | --- |
| Registration and publication | `glue.py` (`register`, `_handle_existing`, `_update_table`) | First sync: `GlueCatalog.register_table`. Later syncs: raw boto3 `UpdateTable` with `VersionId`, pointing Glue at a `metadata.json` **written by the tool** |
| External-write detection | `glue.py` `_handle_existing` | Glue's current file must be in the new file's `metadata_log` |
| "Already published" check | `register.py` `_raise_if_published` | Compares Glue `Parameters.metadata_location` with the pointer |
| Handoff between export and register | `register.py` `ExportPointer` (`_ducklake_iceberg_last_export.json`) | Has `glue_database`/`glue_table` fields; the export reads its base from the pointer, not from the catalog |
| Configuration | `config.py` | `glue_database`, `aws_region`, `aws_profile`; AWS region regex; no catalog type |
| Naming | `glue.normalize_table_name` | Glue rules (`[a-z0-9_]`); the namespace is the configured `glue_database` |
| Lock key | `sync/lock.py` `lock_key(profile, glue_database, glue_table)` | |
| Errors | `glue.py` (`GlueError`, `LakeFormationBlocked`, `RegisterDiverged`, `TableAlreadyRegistered`), `retry.py` (botocore codes) | Exit codes are the public contract and stay |
| Public contract names | envelope `register.glue`, metric phase `glue`, log events `glue.register`/`glue.conflict` | Renames need aliases |
| Observability | botocore `before-call` hook counts `aws.glue.*` | Does not see REST HTTP calls |
| Storage | `aws.py`, `register.build_file_io`, `_validate_metadata_location` (`s3://` only) | Storage, not catalog: kept, extended later |
| Dependencies | `pyiceberg[glue,s3fs]==0.12.0`, boto3 | |
| Private PyIceberg APIs | `pyiceberg.catalog.glue._construct_table_input`, `Catalog._load_file_io` | |

Unused code worth reusing: `glue.load_table_state`, `reconcile_commit`,
`guard_external_write` and `retry.commit_with_retry` were built for a
`commit_table` flow and have no production caller today.

## 3. The design decision: who writes `metadata.json`

Glue lets the tool write the whole `metadata.json` and then swap the catalog's
pointer to it with a compare-and-swap (`UpdateTable` + `VersionId`). This is
not a portable operation:

* The Iceberg REST protocol has no "set metadata location" update. Its
  standard write path is `commit_table`: the client sends updates plus
  requirements, and **the server writes and names the new `metadata.json`**.
* `register_table(..., overwrite=True)` is rejected client-side by PyIceberg's
  Glue, SQL and Hive catalogs, and on REST it is server-dependent and has no
  "expected current location", so it cannot detect a concurrent writer.

So there are two publication modes:

**A. Pointer mode (today, Glue only).** The tool owns the metadata files; the
catalog stores a pointer. Possible on Glue (`UpdateTable`), and on the SQL and
Hive catalogs with catalog-specific code. Not possible on REST.

**B. Commit mode (new, every catalog).**

1. First sync: the tool writes `v1.metadata.json` (field IDs equal to DuckLake
   `column_id`s) and calls `catalog.register_table(identifier, location)`.
   `create_table` stays forbidden because it reassigns field IDs.
2. Later syncs: the base comes from `catalog.load_table(identifier).metadata`
   (not from the pointer). The tool still writes the manifests, the manifest
   list and the position delete files, then calls `catalog.commit_table` with:
   * updates: `AddSchemaUpdate` + `SetCurrentSchemaUpdate` (schema change),
     `AddPartitionSpecUpdate` + `SetDefaultSpecUpdate` (partition evolution),
     `AddSnapshotUpdate` (manifest list written by the tool),
     `SetSnapshotRefUpdate(main)`, `SetPropertiesUpdate` (the `ducklake.*`
     sync state, so state and snapshot stay one atomic commit) and
     `RemoveSnapshotsUpdate` (`snapshot_retention_hours`);
   * requirements: `AssertTableUUID`, `AssertRefSnapshotId(main, <last
     published snapshot>)`, `AssertCurrentSchemaId`,
     `AssertLastAssignedFieldId`, `AssertDefaultSpecId`.
3. External-write detection moves from `metadata_log` ancestry to the
   requirements (a writer in between gives HTTP 409, `CommitFailedException`)
   plus the existing check of the `ducklake_iceberg.iceberg-snapshot-id`
   property against `current_snapshot_id`. Both give exit 3, as today.
4. `CommitStateUnknownException` (HTTP 5xx after the request was sent) is
   resolved by reading the table back and checking whether our snapshot is
   current (the unused `reconcile_commit` logic).

Zero copy is the same in both modes: data files are referenced in place by the
manifests the tool writes. Commit mode is also more portable for Glue itself
(`GlueCatalog.commit_table` does the same `VersionId` swap) and would remove the
pointer file and the raw boto3 calls.

**Decision.** Commit mode is the only publication path in 0.2, Glue included
(`GlueCatalog.commit_table` performs the same `VersionId` compare-and-swap).
Existing Glue tables need no migration: the next sync loads the table through
the catalog and commits on top of the current metadata.

## 4. Configuration

A `catalog` section passed almost verbatim to PyIceberg's `load_catalog`, so
every PyIceberg catalog and option works without code changes:

```yaml
catalog:
  name: lake               # any label; used in logs and in the lock key
  type: rest               # glue | rest | sql | hive (PyIceberg catalog type)
  namespace: analytics     # "a.b" or ["a", "b"] for multi-level namespaces
  properties:              # passed to load_catalog(name, **properties)
    uri: https://polaris.example.com/api/catalog
    warehouse: lake
    scope: PRINCIPAL_ROLE:ALL
```

* Secrets never go in the YAML: they come from PyIceberg's own environment
  variables (`PYICEBERG_CATALOG__LAKE__CREDENTIAL`, `__TOKEN`, ...) or from a
  `DUCKLAKE_ICEBERG_CATALOG__PROPERTIES__*` variable, and are redacted from the
  envelope and the logs like the DSN.
* Backward compatibility: without `catalog`, the current `glue_database` +
  `aws_region` (+ `aws_profile`) keys mean `type: glue`. They keep working in
  0.2 with a deprecation warning.
* `iceberg_metadata_prefix` stays: it is where the tool writes manifests and
  delete files. It must sit inside the table location the catalog allows.
* The table name: the DuckLake table name, normalized by a per-catalog policy
  (Glue keeps today's `[a-z0-9_]` rule; REST only rejects what the server
  rejects). The schema-collision check in `_check_identity` stays.

## 5. Code structure

```text
src/ducklake_iceberg/catalogs/
    __init__.py      # build_publisher(settings) -> Publisher
    base.py          # Publisher protocol, PublishedState, capabilities
    commit.py        # generic PyIceberg Catalog: register_table + commit_table
    glue.py          # Glue specifics only: Lake Formation detection, IAM hints
```

* `Publisher.state(identifier) -> PublishedState | None` (metadata location,
  current snapshot id, table uuid, properties): used by the export base, the
  dry run (`in_sync` / `behind` / `unpublished`) and divergence detection.
* `Publisher.register(identifier, metadata_location)` for the first sync.
* `Publisher.publish(identifier, plan)` for later syncs: `commit_table`.
* The export no longer imports Glue code: it plans against the catalog's
  metadata and returns the updates; it writes `metadata.json` itself only for
  the first registration.
* `register.py` becomes the stage orchestration only; the boto3 `UpdateTable`
  path and the private `_construct_table_input` import are removed.
* `lock_key` takes the catalog name/URI, the namespace and the table.

## 6. Storage and credentials

* The tool writes metadata with PyIceberg's FileIO. In commit mode it should
  use the FileIO of the loaded table (`catalog.load_table(...).io`) so that
  REST **vended credentials** for the table location are used.
* Zero-copy constraint: with credential vending, readers get credentials scoped
  to the table location. If DuckLake's `data_path` is outside that location,
  readers can see the metadata but not the data files. The table location must
  therefore cover both `data_path` and `iceberg_metadata_prefix`, or the catalog
  must be configured so that readers have their own access to `data_path`
  (Polaris `allowedLocations`, static credentials). `doctor` gets a rule for
  this, and the docs a section per catalog.
* `_validate_metadata_location` (`s3://` only) is relaxed to the schemes the
  FileIO supports (`s3://`, `gs://`, `abfss://`). DuckDB already reads those
  through its extensions. GCS/ADLS end-to-end tests are a later phase.

## 7. Public contract (envelope, logs, metrics)

* Envelope: the `register` step gets `catalog: unpublished | in_sync | behind`
  and keeps `glue` with the same value for Glue in envelope v1. The `config`
  block shows the catalog type, name and namespace, never secrets.
* Metric phase `catalog` added; `glue` kept as an alias in v1.
* Log events `catalog.register` / `catalog.conflict` added; `glue.*` still
  emitted for Glue until envelope/log v2.
* Exit codes do not change: 1 error, 2 blocked (now also "catalog does not
  support registering an existing table", see section 8), 3 diverged, 4
  nothing to do.
* Pointer file: no longer written. Existing pointers are ignored; the catalog
  is the only source of the published state.
* Dependencies: `pyiceberg[glue,s3fs]` stays the default; extras
  `ducklake-iceberg[sql]` (`pyiceberg[sql-sqlite,sql-postgres]`) and
  `ducklake-iceberg[hive]`. The REST client is in PyIceberg core.

## 8. Catalog support matrix and risks

| Catalog | Mode | Status / risk |
| --- | --- | --- |
| AWS Glue | commit | Supported today in pointer mode; moves to commit mode |
| SQL catalog (SQLite/Postgres) | commit | Expected to work; fast in-process tests |
| Hive | commit | Expected to work; no purge support in PyIceberg |
| Generic REST (`apache/iceberg-rest-fixture`) | commit | Expected to work; reference for the protocol |
| Apache Polaris / Snowflake Open Catalog | commit | Needs `register_table` (V1 register endpoint) and `allowedLocations` covering `data_path`. Spike |
| Nessie | commit | Branch semantics (`main`), REST API. Spike |
| Databricks Unity Catalog | commit | **High risk.** Databricks documents external writes only to *managed* Iceberg tables; foreign tables are read-only. Registering an existing `metadata.json` and referencing data files outside UC-managed storage may be rejected, which would make zero copy impossible there. Spike in a real workspace before any promise |

A catalog that cannot register an existing table, or cannot reference data
files outside its managed location, is reported by `doctor`/`sync` as a
blocker (exit 2) with an explanation, never worked around by copying data.

## 9. Testing

* Unit: config parsing and backward compatibility, update/requirement
  building from an export plan, error classification (409, 5xx, missing
  register endpoint), redaction of catalog secrets.
* Fast integration (no Docker): PyIceberg `SqlCatalog` on SQLite. Parametrize
  the existing sync suites (`test_sync_e2e`, `test_incremental`,
  `test_second_reader`, envelope contract) over `glue (moto)` and `sql`.
* Glue upgrade path: a table published by 0.1 (pointer mode) keeps syncing
  under 0.2.
* Docker integration: the REST fixture (`apache/iceberg-rest-fixture`) and
  Apache Polaris with MinIO, reusing the zero-copy audit of
  `test_e2e_minio.py`: create/insert/delete/update/merge, schema and partition
  evolution, external write gives exit 3, interrupted commit recovers.
* Readers: PyIceberg and DuckDB `iceberg_scan` through the catalog.
* Manual: Unity Catalog and Snowflake Open Catalog in real accounts
  (credentials needed from you).

## 10. Phases

| Phase | Content | Done when |
| --- | --- | --- |
| 0. Spike (1-2 days) | Script: `register_table` of a tool-written v1, then `commit_table` with `AddSnapshot` pointing at a tool-written manifest list, on SQL, the REST fixture and Polaris; field IDs preserved; read back by PyIceberg and DuckDB. Unity Catalog check | Go/no-go per catalog written down here |
| 1. Abstraction | `catalogs/` package, `Publisher` protocol, neutral lock key, `catalog` config with the legacy keys mapped | Whole suite green |
| 2. Commit mode | Export base from the catalog, update/requirement building, `commit_table` with conflict and unknown-state handling, dry-run check through `Publisher.state`, Glue switched over, pointer removed | SQL and Glue suites green, 0.1 Glue tables keep syncing, zero-copy audit green |
| 3. REST and Polaris | Vended credentials FileIO, location rule in `doctor`, REST fixture and Polaris e2e, docs per catalog | Docker e2e green |
| 4. Contract and release | `catalog` envelope key, phase and events with Glue aliases, extras, CHANGELOG, `docs/` | Release 0.2.0 |

## 11. Open questions

1. ~~Glue in commit mode in 0.2?~~ Yes.
2. ~~Priority?~~ Unity Catalog.
3. ~~Databricks workspace?~~ Not yet: the Unity Catalog findings that need a
   real workspace stay open (section 12).
4. Multi-level namespaces (`catalog.schema` in Unity Catalog): map the DuckLake
   schema to the namespace (one namespace per DuckLake schema) or keep one
   configured namespace as today?
