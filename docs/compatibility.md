# Compatibility and representation rules

[Documentation home](README.md) | [Architecture](architecture.md) | [Operations](operations.md)

The target is **Iceberg v2**, not a generic coercion into whatever a reader
accepts. Unknown or unsafe representations are rejected rather than silently
altering data. The defaults are conservative, and reader-specific limitations
still apply.

## Types

The source strings below are DuckLake catalog type names, which can differ
from SQL aliases such as `INTEGER` or `DOUBLE`. The single source of truth
is [`mapping/types.py`](../src/ducklake_iceberg/mapping/types.py).

| DuckLake catalog type | Iceberg type / outcome |
| --- | --- |
| `boolean` | boolean |
| `int8`, `int16`, `uint8`, `uint16` | int; safe domain widening |
| `int32` | int |
| `int64` | long |
| `float32`, `float64` | float, double |
| `decimal(p,s)` | decimal; precision 1-38, scale 0-p, explicit parameters required |
| `date`, `time` | date, time in microseconds |
| `timestamp`, `timestamptz` | timestamp / timestamptz in microseconds; Athena may truncate to milliseconds on read |
| `varchar`, `varchar(n)` | string; length restriction is not preserved |
| `blob` | binary |
| `uuid` | UUID; no column bounds published, so no bound-based pruning on it |
| `json` | string; Parquet JSON logical annotation stays and strict readers may reject it |
| `struct`, `list`, `map` | Nested schema tree, recursively mapped with source field IDs |

Blocked types:

| Source type | Why it is not relabeled; possible source-side action |
| --- | --- |
| `uint32` | Physical unsigned INT32 can be misread as signed; explicitly rewrite/cast to BIGINT |
| `uint64` | No matching signed 64-bit domain; consider decimal or a proven-fitting BIGINT |
| `int128`, `uint128` | Iceberg integer width is at most 64 bits; consider decimal within its domain |
| `interval` | No Iceberg interval type; split into numeric components |
| `time_ns`, `timetz` | No v2 nanosecond time/time-with-zone equivalent |
| `timestamp_s`, `timestamp_ms` | Relabeling units as microseconds changes values; explicitly convert the source |
| `timestamp_ns`, `timestamptz_ns` | Nanosecond timestamp support requires v3 |
| `variant`, `geometry` | Require representations outside the supported v2 contract; materialize typed fields or explicit string/binary representation |
| `null` | An untyped column is not an Iceberg v2 type; assign a type |
| Unknown names or malformed type parameters | No safe inferred representation |

Changing the source may rewrite files and is **not** performed by this tool.
Review semantic loss, permissions and retention before applying a suggested
conversion. A remediation is not an automatic migration.

## Schema evolution

[`mapping/schema.py`](../src/ducklake_iceberg/mapping/schema.py) builds the
tree; [`mapping/evolution.py`](../src/ducklake_iceberg/mapping/evolution.py)
checks it against published schemas.

- IDs are DuckLake `column_id`s, including nested fields; they are not
  reassigned when columns are renamed.
- List/map shapes and parent-child relationships must be coherent. Orphans,
  cycles and invalid arity are errors.
- Dropped IDs are not reused. `last_column_id` preserves the high-water mark.
- Valid primitive promotions are int to long, float to double, and decimal
  precision widening without changing scale.
- A previously optional field stays optional even if the source becomes
  `NOT NULL`. A late-added required field must remain readable for older
  files; schema construction accounts for this.
- A schema/spec-only change can create a new `metadata.json` **without a new
  data snapshot**. Do not equate metadata version with snapshot count.

### Added columns with defaults

DuckLake can read a non-null `initial_default` for older files that do not
physically contain a newly added column. Iceberg v2 would read null instead.
[`catalog.view.check_initial_defaults()`](../src/ducklake_iceberg/catalog/view.py)
checks candidate older-file footers and blocks this silent mismatch.

Materialize the default into source files only through a deliberate
source-side operation, then resync. The tool does not rewrite those rows for
you. A `SET DEFAULT` for future writes is not interchangeable with an
initial default on historical files.

## Partition evolution

[`mapping/partition.py`](../src/ducklake_iceberg/mapping/partition.py)
preserves historical specs because older manifests still reference them.

| DuckLake transform | Mapping |
| --- | --- |
| `identity` | Iceberg identity |
| `epoch_year`, `epoch_month`, `epoch_day`, `epoch_hour` | Corresponding Iceberg epoch-based transform |
| `bucket(N)` with positive N | Iceberg bucket; typed/materialized values are validated |
| Calendar `year`, `month`, `day`, `hour` | Degrade to unpartitioned with warning; same names do not imply same semantics |
| Other unmappable transforms | Degrade rather than publish unsafe pruning metadata |

Partition field IDs start at 1000. Their deterministic assignment considers
the table's complete partition history, including degraded specs; an
equivalent source-column/transform reuses its earlier field ID. Published
names are retained when source columns are renamed.

Per-file partition values are decoded into typed `Record`s. Missing,
out-of-range or undecodable values are divergence, not guessed defaults.
Degradation affects pruning, not which data rows belong to the table.

## Pre-flight rule catalog

Rules implement `Rule.check(DoctorContext)` and emit `Finding`s. Severity is
`INFO`, `DEGRADED` or `BLOCKER`; a rule exception is `ERROR`. "Failed" in a
rule result means it emitted findings, not necessarily a fatal sync result.
The [implementation reference](implementation.md#doctor-framework-and-rules)
explains the dependency plan and every rule class.

| Rule ID / class | Condition and normal pipeline behavior |
| --- | --- |
| `inlined-data` / `InlinedDataRule` | Pending SQL-backed data/deletes need a flush. This gate is auto-remediable before a normal flush, then checked again. Dry-run reports pending work instead of flushing. |
| `unsupported-type` / `UnsupportedTypeRule` | A type cannot be represented safely; blocker. Checks nested columns too. |
| `calendar-transform` / `CalendarTransformRule` | Calendar partition semantics differ; degraded, exported without the unsafe spec. |
| `parquet-compression` / `ParquetCompressionRule` | Source compression outside the preferred choice; degraded diagnostic, not source rewriting. |
| `delete-parquet` / `DeleteParquetRule` | DuckLake Parquet deletes need conversion; degraded when position-delete capability exists (the default), blocked without it. Unknown formats remain blocked. |
| `delete-puffin` / `DeletePuffinRule` | Deletion vectors/Puffin are not supported for the v2 mirror; blocker. |
| `encryption-key` / `EncryptionKeyRule` | DuckLake per-file encryption key is set; unsupported representation, blocker. This differs from S3 SSE-KMS. |
| `mapping-divergence` / `MappingDivergenceRule` | Per-file name mappings cannot be reproduced by the current exporter; blocker under default capabilities, including a single mapped ID. |
| `partial-files` / `PartialFilesRule` | `partial_max > target snapshot` could expose future rows; blocker. `partial_max <= target` is routine and produces INFO rather than rejection. |

Additional guards outside doctor validate enumeration, identities, defaults,
type evolution, manifest sequences, summary counters, pointer lineage and
Glue publication. Passing doctor alone is not a promise that later I/O and
consistency checks cannot fail.

## Deletes and reader compatibility

The export uses validated source data/delete pairs, filters source delete
positions by target snapshot, validates bounds, and writes Iceberg position
deletes grouped by partition/spec. Existing data files are not materialized
into new data copies.

Puffin, unknown formats, encrypted delete files, invalid positions and
incoherent file/count/schema metadata are not silently ignored. Lower-level
delete readers expose additional policies for dangling files and historical
visibility; those are Python APIs, not CLI options.

The supported representation still needs an engine capable of reading
Iceberg v2 position deletes. Tests include PyIceberg round trips and an
independent DuckDB Iceberg-reader path. See
[`test_second_reader.py`](../tests/integration/test_second_reader.py).

## Explicit non-goals

There is no bidirectional synchronization, distributed lock service, source
compaction, automatic cleanup command, Iceberg v3 export, cross-table atomic
publication, or automatic migration of unsupported source types. Writing
through Athena/Spark/PyIceberg into the mirror violates its ownership model.
