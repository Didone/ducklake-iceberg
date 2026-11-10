# CLI and output reference

[Documentation home](README.md) | [Configuration](configuration.md) | [Operations](operations.md)

## Command surface

```text
ducklake-iceberg --version
ducklake-iceberg --help
ducklake-iceberg sync --table schema.table [OPTIONS]
```

Only `sync` is a public subcommand. There is no `--all-tables`, `--metrics`,
`--compact`, `--yes`, `--no-lock`, or standalone `doctor`, `export`,
`checkpoint`, `register`, `status`, `validate`, or `clean` command.
Schedule one invocation per table. Put command options **after `sync`**.

| Option | Default | Behavior |
| --- | --- | --- |
| `--table TEXT` | required | Qualified source table such as `sales.orders`; resolves identity at the selected DuckLake snapshot |
| `--dry-run` | false | Checks compatibility, detects pending inlined rows, plans export where possible, and reads Glue state without source/metadata/Glue writes |
| `--json` | false | One JSON envelope on stdout instead of Rich human-readable output |
| `--profile TEXT` | settings/env | Explicit override of `aws_profile` |
| `--log-level TEXT` | settings, default `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`; sets application logging threshold |
| `--log-format TEXT` | `auto` | `auto`, `json` or `text` for stderr. Auto chooses text for a terminal and JSON otherwise. Not a YAML setting. |
| `--config PATH` | config search | Explicit YAML path; a missing file is an error |
| `--help` | n/a | Show help and exit without syncing |

The root `--version` displays the tool, DuckDB and PyIceberg versions.
`--json` does not suppress stderr logs or override the log format.
Settings fields without corresponding flags are configured through YAML/env,
not invented `--data-path` or `--region` flags.

## Typical invocations

```bash
# Inspect what would happen.
ducklake-iceberg sync --table sales.orders --dry-run --json

# Publish with an explicit file and profile.
ducklake-iceberg sync --table sales.orders \
  --config /etc/ducklake-iceberg.yaml --profile lake-publisher --json

# Separate machine-readable result from structured diagnostics.
ducklake-iceberg sync --table sales.orders --json --log-format json \
  > orders.result.json 2> orders.events.jsonl

# Detailed diagnostics without changing the table selection.
DUCKLAKE_ICEBERG_TIMINGS=1 ducklake-iceberg sync --table sales.orders \
  --dry-run --json --log-level DEBUG --log-format json
```

These commands have real exit codes; inspect them even when stdout looks
plausible. Shell automation must treat 4 as an expected successful no-op:

```bash
rc=0
ducklake-iceberg sync --table sales.orders --json || rc=$?
case "$rc" in
  0|4) exit 0 ;;
  *) exit "$rc" ;;
esac
```

The wrapper preserves errors and also works under a shell's `set -e`.
Do not use `|| true`, which hides blocked/diverged/error results.

## Exit codes and dry-run semantics

| Code | Envelope `status` | Normal run | Dry run |
| --- | --- | --- | --- |
| 0 | `ok` | Work completed: new metadata and/or pending Glue publication | Changes would be published, or pending inlined data would need flushing |
| 1 | `error` | Configuration, catalog, I/O, credentials, lock or other failure | Same categories of failure while checking |
| 2 | `blocked` | Source state cannot be represented safely, or Lake Formation blocks publication | Same detected blocker |
| 3 | `diverged` | Identity, file-set, metadata/Glue ancestry or consistency mismatch | Same detected divergence; stop and investigate |
| 4 | `nothing-to-do` | Already exported and registered | Already current with no pending work |

Parser usage errors also exit 2, but are **not** a compatibility finding and
may produce no JSON envelope. Invalid/unknown commands and missing required
flags fail before `sync()` constructs an envelope. Consumers must handle
missing/invalid stdout and inspect stderr instead of assuming every failure
has the application schema.

### What a dry run actually reads

The pre-flight runs in a read transaction. Pending inlined rows are reported
in a `checkpoint` step (`pending_inlined_rows`, `pending_inlined_deletions`) without a
flush. In that case the exporter cannot predict the physical files produced
by the future flush, so a complete file delta is not claimed.

Otherwise, the normal export planning path reads the catalog, previous
metadata and possibly file footers. The `register` check reports Glue as:

| `glue` state | Meaning |
| --- | --- |
| `unpublished` | No published table/export to advance yet |
| `in_sync` | Glue points at the known export |
| `behind` | A prior export completed but Glue still points at an ancestor |

Dry-run reads can still require credentials, network and file permissions.
It skips the local table lock and may install/load DuckDB extensions.
It does not create metadata artifacts, flush rows, create a Glue database,
or advance a Glue table. Without a known handoff pointer it reports
`unpublished` without looking up an unrelated same-named Glue table.

## Table names and destination layout

The source is identified by schema and table, not by an arbitrary SQL query.
The destination table name is the **table component only**, normalized with
trim, lowercase, and replacement of non-ASCII-alphanumeric/non-underscore
characters by `_`.

For `sales.orders`, `glue_database=analytics` and
`iceberg_metadata_prefix=s3://example-lake/iceberg/`:

```text
Glue identifier: analytics.orders
Table location:  s3://example-lake/iceberg/analytics/orders/
```

`archive.orders` would map to the same destination; so can names that
normalize identically. Use a different Glue database/metadata prefix or
rename the source tables. Identity checks reject adoption of another source
table; they do not automatically disambiguate names.

## JSON envelope v1

The authoritative schema is
[`schemas/envelope-v1.json`](../src/ducklake_iceberg/schemas/envelope-v1.json).
`output.Envelope` serializes it and `output.Problem` structures diagnostics.
The following is an illustrative subset, **not an exact complete run**:

```json
{
  "version": 1,
  "command": "sync",
  "status": "nothing-to-do",
  "exit_code": 4,
  "dry_run": false,
  "results": [
    {
      "table": "sales.orders",
      "exit_code": 4,
      "steps": [
        {"step": "doctor", "exit_code": 0},
        {"step": "checkpoint", "exit_code": 4},
        {"step": "export", "exit_code": 4},
        {"step": "register", "exit_code": 4}
      ]
    }
  ],
  "problems": []
}
```

Real envelopes also include `started_at`, `duration_ms`, redacted `config`,
and per-table operational details/metrics. Stage lists vary: a failed doctor
stops later stages; flushing can add a second doctor; dry runs can stop at
pending inlined data. Do not hard-code a four-element array.

Important fields:

| Field | How to use it |
| --- | --- |
| `version` | Select a supported envelope parser; currently 1 |
| `status`, `exit_code` | Determine outcome; do not parse human output |
| `config` | Effective redacted settings, including capped pool settings |
| `results[].steps` | Stage outcomes, export metadata path, operation, delta and registration status |
| Export step's `ducklake_snapshot_id`, `ducklake_snapshot_time` | Source checkpoint represented by the export; inspect the actual successful stage/run before treating it as published. The table result also carries `ducklake_snapshot_id`. |
| `results[].files_added`, `files_removed`, `bytes_referenced` | Work/size indicators; referenced bytes are not bytes copied |
| `results[].metrics` | Per-run/per-phase work, calls, retries, durations and bytes |
| `problems[].code` | Stable category such as `config_invalid`, `blocked`, `catalog_diverged`, `profile_not_found`, `workers_capped` |
| `problems[].message`, `hint`, `table` | Redacted explanation, remediation and affected table |
| `problems[].level` | Optional `warning` for non-fatal diagnostics; absence is treated as an error-level problem |
| `timings` | Optional detailed instrumentation when `DUCKLAKE_ICEBERG_TIMINGS` is enabled |

Ignore unknown optional keys. New optional fields do not change envelope
version. Changes of meaning/type, removals and renames require a contract
version change. Warnings do not by themselves imply a nonzero failure exit.

## Reading the mirror

Athena SQL uses the configured Glue database and normalized table name.
An independent PyIceberg reader can use:

```python
from pyiceberg.catalog.glue import GlueCatalog

catalog = GlueCatalog("reader", **{"glue.region": "us-east-1"})
table = catalog.load_table(("analytics", "orders"))
print(table.scan().to_arrow())
```

This is a read operation using the reader's own credentials/configuration,
not an alternative writer. In a separate process, configure any S3 endpoint,
profile and FileIO credentials yourself; application settings are not
automatically inherited by arbitrary PyIceberg programs.
