# Security Policy

## Supported versions

Security fixes are released for the latest minor version of `iceduck`.

## Reporting a vulnerability

Please **do not** open a public issue for security problems. Report them
privately through GitHub's
[private vulnerability reporting](https://github.com/Didone/iceduck/security/advisories/new)
or by e-mail to <didone@live.com>. You can expect an acknowledgment within a
few days.

## Handling of credentials

- iceduck never needs credentials in the catalog DSN. PostgreSQL credentials
  are read from the libpq `PG*` environment variables, and AWS credentials come
  from the standard AWS credential chain.
- Logs, `--json` envelopes and terminal output redact credentials, full DSNs
  and encryption keys.
- iceduck never copies or deletes the Parquet data files that DuckLake wrote.
  It only writes Iceberg metadata under the configured
  `iceberg_metadata_prefix`; use a bucket policy that denies writes to that
  prefix for every principal except the sync executor.
