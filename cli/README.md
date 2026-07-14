# dwhctl

`dwhctl` is a universal command-line interface over the SYNQ
[`dwhsupport`](../) **Scrapper** interface. It bundles every supported
warehouse driver into a single binary and exposes catalog extraction and
metadata metrics as scriptable commands — so any team, in any language, can
reuse the same machinery without importing the Go library or writing
warehouse-specific code.

Supported warehouses: **Snowflake, BigQuery, Databricks, Postgres, Redshift,
ClickHouse, MySQL, Trino, Oracle, MSSQL, Athena, Fabric** (and DuckDB in
CGO-enabled builds).

## Why a separate module?

`dwhctl` lives in its own Go module (`github.com/getsynq/dwhsupport/cli`) with
its own `go.mod`. The CLI-only dependencies (cobra, gojq, tablewriter, TOON)
are **not** pulled into `github.com/getsynq/dwhsupport`, so library consumers
stay lean. The module uses a local `replace` to build against the parent.

## Install / build

```bash
# from the repo
cd cli && go build -o dwhctl .

# released binaries are attached to each dwhsupport vX.Y.Z GitHub release
# (linux/darwin, amd64/arm64)
```

## Design principles

`dwhctl` follows [axi.md](https://axi.md/) — it is built to be driven equally
well by humans, shell pipelines, and AI agents:

- **Automation-first.** No interactive prompts. Diagnostics/logs go to
  **stderr**, results go to **stdout** (or `--output-file`), so output can be
  piped or redirected cleanly.
- **Structured output.** `-o table|json|yaml|toon|tsv|wide`, plus `--jq` for
  server-side filtering and `--columns`/`--wide` to shape tables. Defaults to a
  human table at a terminal and to **TOON** (token-efficient) when an AI-agent
  environment is detected.
- **Explicit empty states.** An empty result prints `0 <things>` to stderr so a
  caller can tell "no data" from a silent failure. Errors exit non-zero.
- **Config as data.** The connection config can be a file, an inline
  string, or stdin — no file required.

## Configuration

Every command needs a connection config, provided (in precedence order) via:

1. `--config-inline '<yaml-or-json>'`
2. `--config <path>` (`-` reads stdin)
3. `$DWHCTL_CONFIG` (a file path)

The document is a single connection keyed by its dialect, optionally with a
top-level `scope` filter. It is compatible with a `connections:` entry from a
[synq-dwh](../../synq-dwh) config, so configs are portable between the two.
`${VAR}` references are expanded from the environment (disable with
`--no-expand-env`), and `*_file` fields are read relative to the config file.

```yaml
snowflake:
  account: ab12345.eu-west-1
  username: SVC_SYNQ
  private_key_file: ./rsa_key.p8
  warehouse: COMPUTE_WH
  role: SYNQ_RO
scope:
  include:
    - database: ANALYTICS
  exclude:
    - schema: STAGING
      table: tmp_*
```

Run `dwhctl dialects` to see the config key and identifying fields for each
warehouse.

## Scope filtering

Every command accepts a **ScopeFilter**, from the config's `scope:` block and/or
repeatable CLI flags:

```bash
dwhctl tables -c conn.yaml \
  --include 'ANALYTICS.PUBLIC.*' \
  --exclude 'ANALYTICS.PUBLIC.tmp_*'
```

Patterns are `database[.schema[.table]]`; omitted trailing levels and `*` match
anything. Config scope and CLI flags are AND-ed together.

## Commands

| Command | Description |
| --- | --- |
| `validate` | Validate the connection and report warnings |
| `catalog` | Full catalog: tables, columns, SQL definitions, constraints (one object) |
| `tables` | Tables and views with type, description, tags |
| `columns` | Column-level catalog metadata |
| `schemas` | Schemas visible to the connection |
| `databases` | Databases visible to the connection |
| `sql-definitions` | SQL of views / materialized views |
| `constraints` | Keys, indexes, partitioning, clustering |
| `table-metrics` | Metadata metrics: row count, size bytes, freshness (`--since`) |
| `query` | Run an arbitrary SELECT and stream rows (`--limit`) |
| `shape` | Column shape of a SELECT without running it |
| `estimate` | Pre-execution scan estimate (bytes / rows) without running it |
| `dialects` | List supported warehouses and their config keys |
| `completion` | Generate a shell completion script |

## Examples

```bash
# Validate a connection, machine-readable
dwhctl validate --config conn.yaml -o json

# Full catalog to a file as JSON (logs still go to the terminal via stderr)
dwhctl catalog -c conn.yaml -o json --output-file catalog.json

# Only tables updated in the last day, filtered with jq
dwhctl table-metrics -c conn.yaml --since 24h -o json --jq '.[] | select(.row_count > 0)'

# Preview 20 rows, config from an env var
dwhctl query 'SELECT * FROM analytics.public.orders' --config-inline "$CONN" --limit 20

# Shell completion (zsh)
source <(dwhctl completion zsh)
```
