# dwhctl

`dwhctl` is a universal command-line interface over the Coalesce Quality
[`dwhsupport`](../) **Scrapper** interface. It bundles every supported
warehouse driver into a single binary and exposes catalog extraction, metadata
metrics, and lightweight query tooling as scriptable commands — so any team, in
any language, can reuse the same machinery without importing the Go library or
writing warehouse-specific code.

It is designed to be driven equally well by **humans**, **shell pipelines**, and
**AI agents** (following [axi.md](https://axi.md/)): every command is
non-interactive, logs go to stderr, results go to stdout (or a file), and output
can be a human table or a machine format (JSON, YAML, TOON, TSV).

**Supported warehouses:** Snowflake, BigQuery, Databricks, Postgres, Redshift,
ClickHouse, MySQL/MariaDB, Trino/Starburst, Oracle, MSSQL / Azure SQL, Athena,
Microsoft Fabric — and DuckDB/MotherDuck in CGO-enabled builds.

---

## Contents

- [Install](#install)
- [Quickstart](#quickstart)
- [Configuration](#configuration)
  - [Where the config comes from](#where-the-config-comes-from)
  - [YAML or JSON](#yaml-or-json)
  - [Environment variable expansion](#environment-variable-expansion)
  - [File references](#file-references)
- [Warehouse config reference](#warehouse-config-reference)
- [Scope filtering](#scope-filtering)
- [Output & automation](#output--automation)
- [Commands](#commands)
- [Shell completion](#shell-completion)
- [Calling from other languages](#calling-from-other-languages)
- [Exit codes](#exit-codes)
- [Environment variables](#environment-variables)
- [Troubleshooting](#troubleshooting)

---

## Install

**Pre-built binaries.** Each `dwhsupport` `vX.Y.Z` GitHub release ships
`dwhctl` binaries for linux/darwin × amd64/arm64. Download the one for your
platform, make it executable, and put it on your `PATH`:

```bash
curl -L -o dwhctl https://github.com/getsynq/dwhsupport/releases/latest/download/dwhctl-darwin-arm64
chmod +x dwhctl
./dwhctl version
```

**From source** (requires Go 1.25+):

```bash
cd cli && go build -o dwhctl .
```

> **DuckDB / MotherDuck** requires a CGO-enabled build (`CGO_ENABLED=1 go build`
> with a C toolchain). The released binaries are pure-Go and therefore do not
> include DuckDB; all other warehouses work in every build.

---

## Quickstart

```bash
# 1. Describe a connection in a small YAML (or JSON) file
cat > conn.yaml <<'YAML'
postgres:
  host: db.internal
  port: 5432
  database: analytics
  username: readonly
  password: ${PGPASSWORD}
YAML

# 2. Check it connects
dwhctl validate --config conn.yaml

# 3. List tables (human-readable table)
dwhctl tables --config conn.yaml

# 4. Same, as JSON for a script
dwhctl tables --config conn.yaml -o json

# 5. Full catalog straight to a file
dwhctl catalog --config conn.yaml -o json --output-file catalog.json
```

Run `dwhctl --help` for the command list, `dwhctl <command> --help` for a
command's flags, and `dwhctl dialects` to see the config key and fields for each
warehouse.

---

## Configuration

Every command (except `dialects`, `version`, `completion`) needs a **connection
config**: a single connection keyed by its warehouse type, with an optional
top-level `scope` filter. The document is compatible with a single entry from a
[`synq-dwh`](../../synq-dwh) `connections:` map, so configs are portable between
the two tools.

### Where the config comes from

Resolved in this precedence order:

| Source | Flag / var | Notes |
| --- | --- | --- |
| Inline string | `--config-inline '<yaml-or-json>'` | Highest precedence. Great for passing a whole config from an env var. |
| File | `--config <path>` / `-c <path>` | Use `-` to read from **stdin**. |
| Environment | `$DWHCTL_CONFIG` | A file path, used when neither flag is set. |

```bash
dwhctl tables --config-inline "$WAREHOUSE_CONFIG"     # from an env var
dwhctl tables --config conn.yaml                      # from a file
cat conn.json | dwhctl tables --config -              # from stdin
export DWHCTL_CONFIG=~/.config/dwhctl/prod.yaml
dwhctl tables                                         # from $DWHCTL_CONFIG
```

### YAML or JSON

**Both formats are fully supported** — YAML is a superset of JSON, so any of the
following work identically, whether inline, in a file, or on stdin:

```yaml
# conn.yaml
snowflake:
  account: ab12345.eu-west-1
  username: SVC_READONLY
  password: ${SF_PASSWORD}
  warehouse: COMPUTE_WH
  role: READONLY
```

```json
{
  "snowflake": {
    "account": "ab12345.eu-west-1",
    "username": "SVC_READONLY",
    "password": "${SF_PASSWORD}",
    "warehouse": "COMPUTE_WH",
    "role": "READONLY"
  }
}
```

The file extension is irrelevant — the content is parsed the same way. Inline
JSON is handy when a caller already has the config as a JSON object:

```bash
dwhctl schemas --config-inline '{"postgres":{"host":"db","database":"analytics","username":"ro","password":"'"$PW"'"}}'
```

### Environment variable expansion

`${VAR}` references in the config are expanded from the process environment,
which keeps secrets out of the file:

- `${VAR}` — replaced with the variable's value (empty if unset).
- `${VAR:-default}` — value if set, otherwise `default`.
- `${VAR:-}` — value if set, otherwise empty string.

Expansion works in both YAML and JSON. Disable it entirely with
`--no-expand-env` (useful if a real value legitimately contains `${...}`).

### File references

Fields that end in `_file` are read from disk and inlined at load time, with
relative paths resolved against the config file's directory:

- Snowflake `private_key_file` → populates `private_key`
- BigQuery `service_account_key_file` → populates `service_account_key`

```yaml
snowflake:
  account: ab12345.eu-west-1
  username: SVC_READONLY
  private_key_file: ./keys/rsa_key.p8   # read relative to conn.yaml
  warehouse: COMPUTE_WH
  role: READONLY
```

---

## Warehouse config reference

Each config sets exactly **one** top-level key. Required fields are marked ✱; all
others are optional. Run `dwhctl dialects` for a quick reminder.

<details>
<summary><b>postgres</b> — PostgreSQL</summary>

```yaml
postgres:
  host: db.internal          # ✱
  port: 5432
  database: analytics        # ✱
  username: readonly         # ✱
  password: ${PGPASSWORD}    # ✱
  allow_insecure: false      # disable TLS cert verification
```
</details>

<details>
<summary><b>snowflake</b> — Snowflake</summary>

```yaml
snowflake:
  account: ab12345.eu-west-1   # ✱
  warehouse: COMPUTE_WH        # ✱
  role: READONLY                # ✱
  username: SVC_READONLY           # ✱
  # authentication — one of:
  password: ${SF_PASSWORD}
  private_key_file: ./rsa_key.p8      # or private_key: <PEM>
  private_key_passphrase: ${SF_KEY_PASS}
  auth_type: externalbrowser          # SSO browser flow
  databases: [ANALYTICS, RAW]         # empty = all accessible
  use_get_ddl: true                   # fetch DDL via GET_DDL()
  account_usage_db: SNOWFLAKE
```
</details>

<details>
<summary><b>bigquery</b> — Google BigQuery</summary>

```yaml
bigquery:
  project_id: my-gcp-project              # ✱
  region: EU                              # ✱
  service_account_key_file: ./sa.json     # or service_account_key: <JSON>
  datasets: [analytics, raw]              # empty = all; set to skip datasets.list
```
</details>

<details>
<summary><b>redshift</b> — Amazon Redshift</summary>

```yaml
redshift:
  host: cluster.abc.eu-west-1.redshift.amazonaws.com  # ✱
  port: 5439                                          # ✱
  database: analytics                                 # ✱
  username: readonly                                  # ✱
  password: ${RS_PASSWORD}                            # ✱
  freshness_from_query_logs: false
```
</details>

<details>
<summary><b>databricks</b> — Databricks SQL</summary>

```yaml
databricks:
  workspace_url: https://dbc-xxxx.cloud.databricks.com  # ✱
  warehouse: 0123456789abcdef                           # SQL warehouse ID
  # authentication — one of:
  auth_token: ${DBX_TOKEN}
  auth_client: ${DBX_CLIENT_ID}          # + auth_secret for OAuth M2M
  auth_secret: ${DBX_CLIENT_SECRET}
  fetch_table_tags: true
  use_show_create_table: false
```
</details>

<details>
<summary><b>clickhouse</b> — ClickHouse</summary>

```yaml
clickhouse:
  host: clickhouse.internal   # ✱
  port: 9440
  database: default           # empty = all databases
  username: readonly          # ✱
  password: ${CH_PASSWORD}    # ✱
  allow_insecure: false
```
</details>

<details>
<summary><b>mysql</b> — MySQL / MariaDB</summary>

```yaml
mysql:
  host: mysql.internal   # ✱
  port: 3306             # ✱
  database: analytics
  username: readonly     # ✱
  password: ${MY_PASSWORD}  # ✱
  allow_insecure: false
  params: { tls: preferred }
```
</details>

<details>
<summary><b>trino</b> — Trino / Starburst</summary>

```yaml
trino:
  host: trino.internal   # ✱
  port: 443
  use_plaintext: false   # true for plain HTTP
  username: readonly
  password: ${TRINO_PASSWORD}
  catalogs: [hive, iceberg]   # usually required
  fetch_table_comments: true
```
</details>

<details>
<summary><b>oracle</b> — Oracle Database</summary>

```yaml
oracle:
  host: oracle.internal      # ✱
  port: 1521
  service_name: ORCLPDB1     # ✱
  username: readonly
  password: ${ORA_PASSWORD}
  ssl: true
  ssl_verify: true
  wallet_path: ./wallet
  use_diagnostics_pack: false   # AWR/ASH (licensed)
```
</details>

<details>
<summary><b>mssql</b> — Microsoft SQL Server / Azure SQL</summary>

```yaml
mssql:
  host: sqlserver.internal   # ✱
  port: 1433
  database: analytics        # ✱
  username: readonly
  password: ${MSSQL_PASSWORD}
  trust_cert: false
  encrypt: "true"
  fed_auth: ActiveDirectoryDefault      # Azure AD
  application_client_id: <app-id>
```
</details>

<details>
<summary><b>athena</b> — Amazon Athena</summary>

```yaml
athena:
  region: eu-west-1          # ✱
  workgroup: primary         # must have an output location configured
  catalog: AwsDataCatalog
  # authentication (priority order): static keys, then profile, then default chain
  access_key_id: ${AWS_ACCESS_KEY_ID}
  secret_access_key: ${AWS_SECRET_ACCESS_KEY}
  aws_profile: analytics
  role_arn: arn:aws:iam::123456789012:role/synq-athena
  external_id: ${ATHENA_EXTERNAL_ID}
  use_show_create_table: false
  use_iceberg_metrics_scan: false
```
Scope mapping for Athena: `database` = Glue catalog, `schema` = Glue database,
`table` = Glue table/view.
</details>

<details>
<summary><b>fabric</b> — Microsoft Fabric Warehouse / Lakehouse</summary>

```yaml
fabric:
  host: my-workspace.datawarehouse.fabric.microsoft.com   # ✱
  database: my_warehouse            # optional, defaults to master
  # authentication — service principal by default:
  client_id: 00000000-0000-0000-0000-000000000000
  client_secret: ${FABRIC_CLIENT_SECRET}
  tenant_id: 00000000-0000-0000-0000-000000000000
  # or a pre-acquired token:
  access_token: ${FABRIC_ACCESS_TOKEN}
```
</details>

<details>
<summary><b>duckdb</b> — DuckDB / MotherDuck <i>(CGO builds only)</i></summary>

```yaml
duckdb:
  database: ./local.duckdb          # file path, ':memory:', or MotherDuck db
  motherduck_account: my-org        # for MotherDuck cloud
  motherduck_token: ${MD_TOKEN}
```
</details>

---

## Scope filtering

Every command accepts a **ScopeFilter** that limits which databases, schemas,
and tables are touched. It can come from the config's `scope:` block, from
repeatable CLI flags, or both (they are **AND-ed** together).

**In the config:**

```yaml
snowflake:
  account: ab12345.eu-west-1
  # ...
scope:
  include:
    - database: ANALYTICS          # only the ANALYTICS database
  exclude:
    - schema: STAGING              # ...but never the STAGING schema
    - table: tmp_*                 # ...and never tmp_* tables anywhere
```

**On the command line** (`database[.schema[.table]]`, repeatable):

```bash
dwhctl tables --config conn.yaml \
  --include 'ANALYTICS.PUBLIC.*' \
  --exclude 'ANALYTICS.PUBLIC.tmp_*'
```

Semantics:

- Each rule level is a glob (`*` matches any run of characters); matching is
  case-insensitive.
- Omitted trailing levels match anything (`--include ANALYTICS` = all schemas &
  tables in `ANALYTICS`).
- If any **include** rules are present, an object must match at least one.
- **exclude** always wins over include.

---

## Output & automation

`dwhctl` is built to be scripted. A few rules make that reliable:

- **stdout = results, stderr = everything else.** Logs, warnings, and the
  `0 <things>` empty-state notices go to **stderr**; only the rendered result
  goes to **stdout**. So `dwhctl tables -c conn.yaml -o json > tables.json`
  yields clean JSON even with logging on.
- **`--output-file <path>` / `-O`** writes the result to a file directly (no
  shell redirect needed); logs still go to stderr.
- **Formats** via `-o`:

  | `-o` | Use |
  | --- | --- |
  | `table` | Human-readable columns (default at a terminal). |
  | `json` | Pretty JSON array/object. |
  | `yaml` | YAML. |
  | `toon` | [TOON](https://github.com/toon-format/toon-go) — compact, token-efficient; default when an AI-agent environment is detected. |
  | `tsv` | Tab-separated, one record per line (great for `cut`/`awk`). |
  | `wide` | Table with every column. |

- **Default format** is `table` for a human at a terminal, and `toon` when an
  agent environment is detected (`CLAUDECODE`, `CURSOR_AGENT`, …). An explicit
  `-o` always wins.
- **`--jq '<expr>'`** filters/transforms the result with a
  [jq](https://github.com/itchyny/gojq) expression (implies JSON input to jq).
- **`--columns a,b,c`** picks specific table/TSV columns; **`--wide`** shows all;
  **`--no-headers`** omits the header row.

```bash
# only tables with rows, as a flat list of names
dwhctl table-metrics -c conn.yaml -o json --jq '.[] | select(.row_count > 0) | .table'

# just three columns, no header, tab-separated
dwhctl columns -c conn.yaml -o tsv --no-headers --columns schema,table,column
```

---

## Commands

All commands accept the [global flags](#global-flags) below in addition to the
ones listed.

| Command | Description |
| --- | --- |
| `validate` | Connect and run the configuration precheck; prints warnings. Exits non-zero if invalid. |
| `catalog` | Full catalog in one object: `tables`, `columns`, `sql_definitions`, `constraints` (fetched concurrently). |
| `tables` | Tables and views with type, description, tags. |
| `columns` | Column-level catalog metadata (type, position, comments, tags). |
| `schemas` | Schemas visible to the connection. |
| `databases` | Databases visible to the connection. |
| `sql-definitions` | SQL of views / materialized views (`--wide` to include the SQL text). |
| `constraints` | Keys, indexes, partitioning, clustering. |
| `table-metrics` | Metadata metrics: row count, size in bytes, freshness. |
| `query` | Run an arbitrary `SELECT` and stream rows. |
| `shape` | Column shape (name/type/position) of a `SELECT` without running it. |
| `estimate` | Pre-execution scan estimate (bytes / rows) without running the query. |
| `dialects` | List supported warehouses and their config keys. |
| `completion` | Generate a shell completion script. |
| `version` | Print version / build info. |

**Command-specific flags:**

- `table-metrics --since <t>` — only tables updated at/after `<t>`, given as an
  RFC3339 timestamp (`2026-01-02T15:04:05Z`) or a duration (`24h`, `7d`).
- `query [SQL] --sql <SQL> --limit <n>` — SQL from a positional arg, `--sql`, or
  stdin (`-`); `--limit` caps rows (default 100, `0` = unlimited).
- `shape [SQL] --sql <SQL>` and `estimate [SQL] --sql <SQL>` — same SQL sources.

### Global flags

| Flag | Description |
| --- | --- |
| `-c, --config <path>` | Config file (`-` = stdin). |
| `--config-inline <str>` | Config as an inline YAML/JSON string. |
| `--no-expand-env` | Do not expand `${VAR}` in the config. |
| `--include <pat>` / `--exclude <pat>` | Scope patterns (repeatable). |
| `-o, --output <fmt>` | `table`, `json`, `yaml`, `toon`, `tsv`, `wide`. |
| `--jq <expr>` | Filter output with a jq expression. |
| `--columns <list>` / `--wide` / `--no-headers` | Shape table/TSV output. |
| `-O, --output-file <path>` | Write results to a file instead of stdout. |
| `-v, --verbose` | Info-level logs (to stderr). |
| `--log-level <lvl>` | `trace`, `debug`, `info`, `warn`, `error` (default `warn`). |

### Examples

```bash
# Validate, machine-readable
dwhctl validate --config conn.yaml -o json

# Full catalog to a file
dwhctl catalog -c conn.yaml -o json --output-file catalog.json

# Tables in one schema only, wide table
dwhctl tables -c conn.yaml --include 'ANALYTICS.PUBLIC' -o wide

# Table freshness in the last day
dwhctl table-metrics -c conn.yaml --since 24h -o json

# Preview 20 rows
dwhctl query 'SELECT * FROM analytics.public.orders' -c conn.yaml --limit 20

# What would this query scan?
dwhctl estimate 'SELECT * FROM analytics.public.orders' -c conn.yaml -o json
```

---

## Shell completion

```bash
# bash
source <(dwhctl completion bash)
# zsh
source <(dwhctl completion zsh)
# fish
dwhctl completion fish | source
```

Add the line to your shell rc file to make it permanent. `dwhctl completion
--help` lists all shells.

---

## Calling from other languages

`dwhctl` is meant to be shelled out to. Pass the config inline and read JSON off
stdout; read logs / errors off stderr; check the exit code.

**Python:**

```python
import json, subprocess

config = json.dumps({"postgres": {
    "host": "db.internal", "database": "analytics",
    "username": "ro", "password": os.environ["PGPASSWORD"],
}})

proc = subprocess.run(
    ["dwhctl", "tables", "--config-inline", config, "-o", "json"],
    capture_output=True, text=True,
)
if proc.returncode != 0:
    raise RuntimeError(proc.stderr)
tables = json.loads(proc.stdout)
```

**Node.js:**

```js
import { execFileSync } from "node:child_process";
const config = JSON.stringify({ postgres: { host: "db", database: "analytics", username: "ro", password: process.env.PGPASSWORD } });
const out = execFileSync("dwhctl", ["tables", "--config-inline", config, "-o", "json"]);
const tables = JSON.parse(out.toString());
```

---

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success (an empty result is still success — see the `0 <things>` notice on stderr). |
| non-zero | Any error — bad config, connection failure, query error, permission denied, unknown flag/command, … The message is printed to stderr, prefixed with `Error:`. |

---

## Environment variables

| Variable | Effect |
| --- | --- |
| `DWHCTL_CONFIG` | Path to the connection config when neither `--config` nor `--config-inline` is set. |
| `${...}` in config | Expanded from the environment unless `--no-expand-env` is passed. |
| `CLAUDECODE`, `CURSOR_AGENT`, … | When present, the default output format becomes `toon` instead of `table`. |

---

## Troubleshooting

- **`no connection config provided`** — pass `--config`, `--config-inline`, pipe
  it via `--config -`, or set `$DWHCTL_CONFIG`.
- **`no database type configured`** — the config must set exactly one top-level
  warehouse key (`postgres:`, `snowflake:`, …). Run `dwhctl dialects`.
- **A `${VAR}` came through literally** — the variable is unset; export it, use
  `${VAR:-default}`, or pass `--no-expand-env` if the literal is intended.
- **`duckdb support not available`** — the released binaries are pure-Go; build
  with `CGO_ENABLED=1` to include DuckDB.
- **Empty output but exit 0** — that's a genuine empty result; the `0 <things>`
  line on **stderr** confirms the command ran.
- **Want to see what it's doing** — add `-v` (or `--log-level debug`); logs go to
  stderr and never pollute the result on stdout.
