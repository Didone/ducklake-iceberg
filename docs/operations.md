# Operations, monitoring and recovery

[Documentation home](README.md) | [Usage](usage.md) | [Configuration](configuration.md) | [Deployment](deploy/README.md)

## Operational model

Run a short-lived job per table. Ensure only one scheduler/host is responsible
for a given destination at a time. Store JSON stdout and stderr logs
separately. Treat exit 0 and 4 as success; route 1, 2 and 3 to different
remediation paths.

Start with a successful initial publication and recorded source/destination
identity. A job that ran but failed to update Glue did not publish its new
view, even if it generated metadata.

## Monitoring signals

| Signal | Interpretation/action |
| --- | --- |
| Run exit/status | 1: infrastructure/config/error; 2: unsupported source/access blocker; 3: divergence requiring investigation |
| `sync.end`, `command.end` | Correlate outcome/duration with run ID/table |
| `phase.end` and per-result metrics | Separate catalog/doctor/checkpoint/delta/I/O/Glue time, calls and retries |
| Exported DuckLake snapshot time | Source checkpoint represented, not automatically the last successful Glue publication on a failed run |
| `files_added`, `files_removed`, delete additions | Workload changes, not necessarily logical inserted/deleted row totals |
| `bytes_referenced` versus `bytes_written` | Existing data bytes exposed versus metadata/delete artifacts written; zero-copy does not mean zero output bytes |
| `compat.blocked`, `problems` | Actionable compatibility constraints |
| `glue.conflict` / retry counts | Possible overlapping schedulers or service pressure |
| `workers_capped` | Resource request was reduced to configured cap; warning, not failure |
| `redaction_failed` | Unsafe log was dropped; investigate without disabling the secret filter |

Do not alert by matching free-text `log` messages: stable event names,
exit codes and structured problem codes are the machine contract.
Log schema version and envelope version are independent.

### Read-only health checks

```bash
ducklake-iceberg sync --table sales.orders --dry-run --json --log-format json
```

Exit 4 means no source work is pending and known export/publication is current.
Exit 0 means changes are pending; pending inlined rows cannot yet be expressed
as a precise output-file delta. A `behind` register state means the next
normal run can repair a prior unregistered export.

If no handoff pointer exists, the dry-run check reports `unpublished` without
probing a hypothetical Glue table. Therefore it is not an exhaustive audit
for unrelated same-named Glue tables before the first publication.
Dry-run skips the process lock, so it is a point-in-time read, not scheduler
serialization.

## Troubleshooting matrix

| Symptom | Likely cause and safe next step |
| --- | --- |
| `config_invalid` | Check required fields, uppercase enum values, YAML mapping/unknown keys, config-file precedence, DSN credentials and overlapping paths |
| `table_not_found` | Confirm qualified source schema/table and selected catalog identity, not just whether a similarly named Glue table exists |
| Could not load extension | Confirm pinned DuckDB/platform, network/cache permissions and `DUCKLAKE_ICEBERG_DUCKDB_EXTENSION_DIRECTORY`; preinstall correct extensions |
| Metadata catalog missing/unreachable | Verify DSN, PostgreSQL connectivity/PG variables, alias and metadata catalog; do not let repair create a replacement empty lake |
| `profile_not_found`, expired SSO | Authenticate/renew correct profile or workload role; remember app profile overrides standard `AWS_PROFILE` |
| S3 403 where a first-export key should be absent | Check `ListBucket` as well as object access; lack of list permission can turn absence into access denied |
| Cannot write source data during flush | Grant only the needed source new-file/catalog writes or deliberately disable inlining at the writer; sync does not skip pending rows |
| Type/default/mapping/partial-file blocker | Follow [Compatibility](compatibility.md), assess a deliberate source-side migration, then retry |
| Lake Formation blocker | IAM alone may be insufficient; review Lake Formation database/table/data-location grants and reader permissions |
| Lock held | Another same-host job is running; inspect holder diagnostics and scheduler overlap. A leftover lock file is not proof of a held kernel lock. |
| Read-only root cannot create lock | Provide a writable **local** `DUCKLAKE_ICEBERG_STATE_DIR`; extensions need their own readable preinstalled directory |
| Glue pointer unrelated to export | Stop external writers and duplicate schedules; compare identities/lineage before recovery |
| File-set/count/sequence/counter divergence | Stop publication. Preserve evidence and inspect source/catalog consistency; do not delete the guard or union the inconsistent sets |
| Old Iceberg snapshots cannot read data | Source cleanup may have removed files still referenced by retained snapshots; metadata cannot reconstruct deleted data |

## Recovery by failure boundary

### Failure before any export writes

Configuration, doctor, target resolution and consistency failures do not
produce a partially published metadata graph. Correct the cause and rerun
normally. A flush that already completed is a separate source commit; do
not assume the source was unchanged just because later export failed.

### Failure during artifacts or before the handoff pointer

Some immutable outputs may exist but remain unreferenced. Never list
`metadata/` and manually publish the numerically largest JSON file.
The exporter can adopt a completed successor only after proving ancestry
and source coherence; otherwise it stops.

Preserve the error, pointer and metadata locations. Retry the normal command
after addressing transient I/O/credentials. If it detects an inconsistent
successor, investigate rather than overwriting it.

### Export completed, Glue not advanced

Rerun the same normal `sync`. An export no-op still proceeds to registration
using the completed handoff. Glue at a valid ancestor can advance without
recopying data. The dry run can show `behind`.

### Request timed out with unknown Glue outcome

First determine whether Glue already points at the intended metadata.
Registration/reconciliation checks can distinguish a successful-but-unacknowledged
request from a genuine conflict. Do not blindly create another table or
force an unrelated pointer.

### External writer or source identity recreation

Exit 3 is intentional protection. Stop writers to the Iceberg mirror,
identify whether DuckLake was recreated/renamed/redirected, and compare
source UUID/ID, persisted state, pointer and Glue metadata ancestry.

Restoring the intended state or publishing to a **new destination
database/prefix** is a deliberate operator decision requiring validation.
There is no `--force` flag. Do not edit source metadata, handoff IDs or Glue
state merely to bypass identity checks.

## Retention and maintenance

DuckLake maintenance is scheduled separately by the source owner. This tool
never calls source compaction, snapshot expiry or orphan-file cleanup.
It only flushes pending inlined rows.

`snapshot_retention_hours` trims Iceberg history in newly written metadata;
it does not delete old metadata files, manifests, delete files or source
Parquet. Unchanged syncs do not perform a retention-only maintenance pass.
There is no metadata `clean` command.

Before configuring source cleanup or S3 lifecycle deletion:

1. Decide how much Iceberg time-travel history must remain readable.
2. Retain every source file referenced by that history and any running query.
3. Add a margin for scheduling lag, delayed retries and reader duration.
4. Ensure lifecycle rules do not age-delete currently referenced objects
   just because their creation dates are old.
5. Validate retention changes in a non-production lake.

The exported mirror is not a backup. Back up the source catalog and storage
under your normal recovery policy.

## Concurrency

The local lock key is derived from profile, Glue database and normalized
table name. It lives in the configured state directory and is released by
the kernel if the process dies. It is not a distributed lease or a
cross-account/global uniqueness guarantee.

Do not overlap the same destination across Fargate tasks, pods, machines or
different local lock roots. Optimistic Glue `VersionId` checks protect pointer
updates, not all writes to the shared metadata prefix. Different destination
tables may run in parallel, subject to database/S3/service capacity.

Set schedule intervals and scheduler concurrency/retry limits using measured
worst-case runtime, not only an average successful run.

## Performance and capacity

Start with defaults. Measure first:

```bash
DUCKLAKE_ICEBERG_TIMINGS=1 ducklake-iceberg sync --table sales.orders --json
poetry run python scripts/bench_sync.py \
  --config ./ducklake-iceberg.yaml --table sales.orders -n 5 \
  --args "sync --dry-run"
```

The benchmark runs fresh processes and reports wall/in-process/startup
distributions. `--prepare` can change the source before each run; use it only
with a trusted command and a non-production workload.

Common levers:

- `stats_source=catalog` reduces footer reads where catalog metrics suffice.
- Footer/I/O pools overlap independent reads/writes; increasing them can
  increase service pressure and memory use.
- `max_total_workers` caps individual pools, not the sum of all process
  threads; size DuckDB threads separately.
- Cached sessions/FileIO preserve credential and HTTP connection reuse
  within a process; this does not remove startup cost for scheduled jobs.
- Manifest reuse makes writes proportional to changes, but reading/validating
  base metadata still has a cost that grows with retained structure.
- Do not remove field-ID/default/visibility/reconstruction checks to meet a
  latency target.

On WSL, use the Linux filesystem for benchmarks rather than `/mnt/c` and
invoke the environment's executable directly to separate tooling overhead.

## Security and incident handling

Use short-lived workload credentials, protect profile caches and local
config/lock directories, restrict write access to the metadata prefix, and
grant readers only the data/metadata reads they need.

The supplied source policy denies deletion/ACL/tag/retention operations but
allows `PutObject` for flushes. IAM alone cannot distinguish every new object
from an overwrite at the same key; the zero-copy implementation/tests are
also part of the safety model.

Do not expose debug local variables, frozen S3 credential maps, raw boto3
requests or DuckDB secret contents. Follow [SECURITY.md](../SECURITY.md) for
reporting vulnerabilities; preserve redacted run IDs, versions, problem
codes and failing-stage information for normal bug reports.
