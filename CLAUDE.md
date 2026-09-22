# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`dwhsupport` is a Go library that provides standardized interfaces and models for interacting with different data warehouses. It abstracts the complexities of working with various data warehouse systems (Snowflake, BigQuery, Databricks, Postgres, Redshift, ClickHouse, DuckDB, MySQL, Trino) through a unified interface.

## Commands

### Testing
- `go test -v ./...` - Run all tests
- `go test -v ./path/to/package` - Run tests for specific package

### Formatting
- `golines -w -m 150 .` - Format all Go files with max line length of 150

### Building
- `go build ./...` - Build all packages

### Code Generation
- `go generate ./...` - Regenerate all generated code (mocks, etc.)
- `./mockgen.sh` - Regenerate mocks only (called by `go generate`)
- CI verifies generated code is up to date; run `go generate ./...` before pushing if interfaces change

## Architecture

### Core Abstraction Layers

The library is organized into three main layers:

1. **Executor Layer** (`exec/`)
   - Low-level database connection and query execution
   - Each warehouse type has its own executor (e.g., `exec/snowflake`, `exec/bigquery`)
   - Executors implement `StdSqlExecutor`: `GetDb()`, `QueryRows()`, `Select()`, `Exec()`, `Close()`
   - `QueryRows`, `Select`, `Exec` automatically apply `querycontext.AppendSQLComment` and warehouse-specific enrichment (Snowflake query tag, ClickHouse log_comment) — **scrappers must use these instead of `GetDb()` for queries**
   - `GetDb()` is only for passing to `stdsql.QueryMany`/`NewQuerier` helpers (which handle enrichment internally)
   - Uses `querier.Querier[T]` pattern for type-safe query execution
   - Generic executor functionality in `exec/generic.go` including `QueryMany[T]` for batch processing

2. **Scrapper Layer** (`scrapper/`)
   - High-level interface for warehouse metadata extraction
   - All scrappers implement the `Scrapper` interface from `scrapper/interface.go`
   - Methods return standardized models: `CatalogColumnRow`, `TableMetricsRow`, `SqlDefinitionRow`, `TableRow`, `DatabaseRow`
   - Each scrapper method typically lives in separate `query_*.go` files
   - SQL queries are often stored in separate `.sql` files and loaded at runtime
   - Methods can return `ErrUnsupported` if not implemented for a specific warehouse

3. **SQL Dialect Layer** (`sqldialect/`)
   - Handles SQL syntax differences between warehouses
   - Provides dialect-specific query building

### Key Interfaces

**Scrapper Interface** (`scrapper/interface.go`):
- `ValidateConfiguration(ctx)` - Validate connection config
- `QueryCatalog(ctx)` - Get column-level catalog information
- `QueryTableMetrics(ctx, lastFetchTime)` - Get table statistics (row counts, sizes, update times)
- `QuerySqlDefinitions(ctx)` - Get view/table SQL definitions
- `QueryTables(ctx)` - Get table metadata
- `QueryDatabases(ctx)` - Get database metadata
- `QuerySegments(ctx, sql, args)` - Query custom segments
- `QueryCustomMetrics(ctx, sql, args)` - Query custom metrics
- `QueryShape(ctx, sql)` - Get column schema for a SQL query
- `QueryTableConstraints(ctx)` - Get table constraints (indexes, keys)
- `Close()` - Close underlying executor

### Data Models

All data models are in `scrapper/models.go`:
- `TableMetricsRow` - Table statistics (row_count, size_bytes, updated_at)
- `CatalogColumnRow` - Column metadata with types, comments, tags, nested fields
- `TableRow` - Table metadata with types, descriptions, tags, annotations
- `SqlDefinitionRow` - SQL definitions for views/materialized views
- `DatabaseRow` - Database-level metadata
- `DwhFqn` - Fully qualified name (instance, database, schema, object)

### Adding New Data Warehouse Support

1. Create executor in `exec/yourwarehouse/`
   - Implement connection logic and `Executor` interface
   - Create `query.go` with `NewQuerier[T]` function

2. Create scrapper in `scrapper/yourwarehouse/`
   - Implement `Scrapper` interface
   - Create separate `query_*.go` files for each method
   - Store SQL queries in `.sql` files when appropriate
   - Return `ErrUnsupported` for unimplemented methods

3. Implement `SqlDialect` in `sqldialect/` if needed
   - All SQL functions that differ across databases (string length, substring, etc.) must go through the `Dialect` interface — never hardcode function names like `Fn("length", ...)` in `metrics/` code

4. Register the new dialect in `sqldialect/dialects.go` `DialectsToTest()` and regenerate snapshots:
   - `UPDATE_SNAPS=true go test ./sqldialect/... ./metrics/... -count=1`

### Adding a Method to the Scrapper Interface

A new `Scrapper` method must be added in all of these or the build breaks:
- `scrapper/interface.go` (the interface) + each warehouse's `query_*.go` (12 impls; `ErrUnsupported` is fine)
- Decorators: `scrapper/scope/scoped_scrapper.go`, `scrapper/reject/rejecting_scrapper.go`, `scrapper/sanitize/sanitizing_scrapper.go`, `pool/scrapper.go`
- `./mockgen.sh` to regenerate `scrapper/mocks.go`
- Test stubs that implement Scrapper: `scrapper/unwrap_test.go` and the mock scrappers in `scrapper/{reject,sanitize,scope}/*_test.go` and `pool/scrapper_test.go`
- For list methods: add a row model (e.g. `SchemaRow`) with `TableFqn`/`SetInstance`/`HasValidIdentity` (`identity.go`) + `Sanitize` (`sanitize.go`), and a scope post-filter helper in `scrapper/scope/filter.go`

## `cli/` — dwhctl command-line interface

`cli/` is a **separate Go module** (`github.com/getsynq/dwhsupport/cli`, binary
`dwhctl`) that exposes the Scrapper interface as a warehouse-agnostic CLI for
non-Go teams, shell pipelines, and AI agents. It is a nested module with its own
`go.mod` + `replace github.com/getsynq/dwhsupport => ../`, so its CLI-only deps
(cobra, gojq, tablewriter, TOON) are **NOT** pulled into the library module —
library consumers stay lean. It is not part of the root module's `go build ./...`;
CI builds/tests it as a separate `cli` job (`.github/workflows/go.yml`).

- Structure: `cli/main.go`, `cli/cmd/*` (one file per command group),
  `cli/internal/output/` (backported from `cloud/lib/cli/output` — table/json/
  yaml/toon/tsv/jq renderer; keep in sync there if fixing bugs).
- Config: reused `yamlconfig` (env `${VAR}` expansion + `*_file` resolution) →
  `ToProtoConnection` → `connect.Connect`. A single connection keyed by dialect,
  optionally with a top-level `scope:`; portable with a `synq-dwh` `connections:`
  entry. Sources: `--config-inline`, `--config` (`-`=stdin), `$DWHCTL_CONFIG`.
- Scope: config `scope:` AND CLI `--include`/`--exclude` (`db[.schema[.table]]`)
  → `scope.ScopeFilter` → `scope.NewScopedScrapper`. Every command accepts it.
- axi.md conventions: logs→stderr, results→stdout (or `--output-file`/`-O`);
  `-o` defaults to table for humans and TOON when an agent env is detected
  (`output.IsAgentContext`); empty results print `0 <things>` to stderr; errors
  exit non-zero.
- Adding a command: add a `cli/cmd/<name>.go` with a `*cobra.Command`, wire it in
  an `init()` via `rootCmd.AddCommand`, use `withScrapper` + `emitList`/`output.Print`.
  When adding a Scrapper interface method, no CLI change is required unless you
  want to surface it.

## Supported Warehouses

- BigQuery
- Snowflake
- Databricks
- Postgres
- Redshift
- ClickHouse
- DuckDB
- MySQL
- Trino
- Oracle
- MSSQL

## Testing

- Tests use `github.com/gkampitakis/go-snaps` for snapshot testing
- To create/update snapshots: `UPDATE_SNAPS=true go test ./path/to/package -count=1`
- Trino scrapper uses snapshot tests for SQL queries — changes to `scrapper/trino/*.sql` require snapshot updates
- Changes to `metrics/` or `sqldialect/` SQL generation require snapshot updates for both: `UPDATE_SNAPS=true go test ./sqldialect/... ./metrics/... -count=1`
- `CI=true` and `UPDATE_SNAPS=true` are mutually exclusive — don't use both
- Mock generation uses `go.uber.org/mock` via `go tool` — regenerate with `go generate ./...`
- Test files follow `*_test.go` naming convention
- Integration tests exist for most warehouse implementations

### Integration Test Suites

Five embeddable test suites in `scrapper/scrappertest/`:
- **ComplianceSuite** — validates all scrapper methods work or return ErrUnsupported
- **ScopeComplianceSuite** — validates scope filtering (include/exclude)
- **MonitorComplianceSuite** — tests QuerySegments/QueryCustomMetrics/QueryShape with dialect-specific SQL
- **MetricsExecutionSuite** — generates SQL via `metrics/querybuilder` and executes against real databases
- **SqlDialectExecutionSuite** — builds derived tables, window functions and qualified column refs with `sqldialect` and executes them, so a dialect that rejects what the builders emit fails here rather than in a customer's warehouse. Wired per warehouse in `scrapper/<wh>/sqldialect_execution_test.go`; the DuckDB copy is in-memory and runs in CI.

Integration tests connect to dwhtesting staging databases via Twingate (no port-forwarding needed). Each scrapper package has a `base_test.go` that loads `../../.env` via `godotenv`. Env var prefixes per database:
- `ORACLE_`, `MSSQL_`, `POSTGRES_`, `CLICKHOUSE_` — dwhtesting staging
- `MARIADB_` — MariaDB on dwhtesting staging
- `MYSQL_` — real MySQL on dwhtesting staging
- `STARBURST_` — Starburst Galaxy (HTTPS), `TRINO_` — self-hosted Trino (plaintext HTTP)
- `SNOWFLAKE_` — set `SNOWFLAKE_PRIVATE_KEY_FILE` for key-pair auth; a password is refused wherever the account enforces MFA

`godotenv.Load` does NOT overwrite an already-set variable, so prefixing `go test` with env vars points a suite at a different account or dataset for one run without touching `.env`. `SqlDialectExecutionSuite` needs a table that actually exists — `SNOWFLAKE_TEST_TABLE_NAME` / `_KEY_FIELD` / `_SEGMENT_FIELD` override its defaults, which name a fixture only one account has.

## Releases

- Tags with `-rcX` suffix (e.g. `v0.9.0-rc6`) are pre-releases — use `--prerelease` flag when creating with `gh release create`
- RC release changelogs must include all changes since the last **stable** release, not just since the previous RC
- Use `gh release create v0.X.0-rcN --prerelease --generate-notes --notes-start-tag <last-stable-tag>`
- Example: `v0.9.0-rc6` uses `--notes-start-tag v0.8.3` (last stable), not `v0.9.0-rc5`
- **Stable release**: `gh release create vX.Y.Z --generate-notes --latest` — a pushed git tag alone is NOT a release; always cut the GitHub release too. Then re-pin cloud consumers (`lib/dwh` + kernels using `dwhconnect`/`dwhaudit`) in their `go.mod`.
- **dwhctl binaries**: pushing a `vX.Y.Z` tag also triggers `.github/workflows/dwhctl-release-binaries.yaml`, which cross-compiles `dwhctl` (linux/darwin × amd64/arm64, `CGO_ENABLED=0`, version stamped via `-X .../cli/cmd.version`) and attaches the binaries to that release. The binary version tracks the library tag — no separate CLI tag.

## Special Patterns

- **Query Logs**: `querylogs/` defines `QueryLogsProvider` interface with `FetchQueryLogs` returning a `QueryLogIterator`. Implementations use `querylogs.NewSqlxRowsIterator[T]` with a warehouse-specific schema struct and converter function. See `scrapper/redshift/query_logs.go` for the canonical pattern.
- **Scrapper Configs**: All scrapper configs must be proper structs embedding their executor config (not type aliases). Each scrapper should have an `Executor()` accessor method. Example: `type MSSQLScrapperConf struct { dwhexecmssql.MSSQLConf }`.
- **Lazy Loading**: `lazy/lazy.go` provides lazy initialization pattern
- **SSH Tunneling**: `sshtunnel/ssh_tunnel.go` supports SSH tunnel connections
- **Query Building**: `querybuilder/` provides utilities for dynamic query construction
- **Blocklists**: `blocklist/` provides filtering for databases/schemas
- **Metrics Extraction**: `metrics/` contains logic for extracting and processing metrics from different warehouses
- **Row scanning**: never `rows.StructScan` — there are no remaining uses, and `grep StructScan` should stay empty. `rowscan.Scanner` plans the column-to-field map from the open result set, so the result set decides the shape: matching is case-insensitive, a result column no `db` tag claims is discarded and logged as drift, and a tag with no column keeps its zero value. Reach for `scrapper.ScanAll` when the loop just collects rows, and plan a `rowscan.New` scanner outside the loop when it accumulates per row. `exec/stdsql`'s `QueryMany`/`QueryAndProcessMany` go through it too, so every stdsql warehouse gets it for free. `StructScan` fails **every row** over one unmatched column, which is how a Snowflake account with `QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE` (it stores quoted identifiers upper-cased, and every alias we generate is quoted) loses its whole catalog and every monitor. The scanner lives in its own leaf package because `exec` must not import `scrapper`.
  - Two result columns with the same name are no longer last-wins: the first is scanned and the second logged as unknown. An alias collision in a query is a bug in the query — `duckdb/query_catalog.sql` had one.
- **QueryMaps lookups**: read a column with `exec.QueryMapResult.Get`, never by indexing the map. The keys are the driver's spelling, so on a folded account `AS "fail_0"` comes back keyed `FAIL_0`; indexing misses, callers read the miss as zero, and a SQL test reports green with no error raised anywhere. The keys stay verbatim on purpose — callers range over them to show a customer their own column names back.
- **Permission errors**: `exec/<dialect>.IsPermissionError(err)` is the single source of truth; scrappers delegate to it. Non-scrapper query paths (which hold an `exec` querier) reuse it too — don't re-match driver-specific errors elsewhere.
- **Scope Filtering**: `scrapper/scope/` provides include/exclude scope filtering. SQL files use `/* SYNQ_SCOPE_FILTER */` placeholder at the injection point; `AppendScopeConditions` replaces it with `AND <conditions>` or empty string. Never use heuristic WHERE-append. Scope is **context-driven** (`scope.WithScope`), never a method parameter. Use `AppendSchemaScopeConditions(ctx, sql, dbCol, schemaCol)` for schema-level listings (no table column). `ScopedScrapper` post-filters results via `FilterRows`/`FilterDatabaseRows`/`FilterSchemaRows`, so SQL push-down is an optimization, not the enforcement boundary.
- **Scope Compliance Testing**: `scrapper/scrappertest/ScopeComplianceSuite` is an embeddable test suite for validating scope filtering — embed alongside `ComplianceSuite` in warehouse integration tests
- **Identifiers**: `Ident` (`sqldialect/ident.go`) is the type to reach for when a name arrives from outside the statement being built — a config file, a catalog row, a `QueryShape` column. It always renders quoted, because every `…IfNeeded` helper here decides by character class and so cannot see that `order` or `group` is a reserved word. Pick the constructor by where the text came from: `CanonicalIdent` for a name the engine itself handed back (rendered as given), `WrittenIdent` for text a person typed (quotes the author wrote are honoured, an unquoted name is folded through `Dialect.FoldIdent` first so the quoted form addresses the object the unquoted one did). `QualifiedIdent` dot-joins parts, each quoted on its own — `"public.table"` is one object and `"public"."table"` is another, which is also why `SplitQualifiedIdent` splits on dots outside quotes rather than on every dot. `Dialect.QuoteIdent` / `FoldIdent` are the primitives underneath; call them directly only when you are not holding a name.
  - `Identifier` and `ResolveFieldRef` keep their "quote when the characters demand it" behaviour, and so does `TableFqn` / `ResolveFqn` — changing them would move the SQL every existing consumer emits. Convert a call site to `Ident` deliberately, statement at a time, under the all-or-nothing rule below.
  - **Per-dialect rules, each taken from the vendor's own lexical reference.** The escape is the one that bites: BigQuery is alone in backslashing its delimiter, because "Quoted identifiers have the same escape sequences as string literals".

    | Dialect | Delimiter | Escape inside | Unquoted reference folds to |
    |---|---|---|---|
    | Postgres, Trino, Athena | `"` | doubled | lower |
    | Redshift | `"` | doubled | lower — **and so does a quoted one**, unless the cluster sets `enable_case_sensitive_identifier`; quoting buys reserved words and punctuation here, not case |
    | Snowflake, Oracle | `"` | doubled | UPPER |
    | DuckDB | `"` | doubled | nothing — comparison ignores case either way |
    | BigQuery | `` ` `` | **`\``** | nothing — datasets and tables are case-sensitive, columns matched case-insensitively |
    | ClickHouse, MySQL, Databricks | `` ` `` | doubled | nothing |
    | MSSQL, Fabric | `[` `]` | `]]` | nothing — case is the collation's business |

    Folding is ASCII-only and applies only to a name the engine would have taken unquoted in the first place: `Created At` was never an unquoted reference, so it is quoted as written rather than upper-cased into a different object.
  - **The engines are asked directly, not just read about.** `scrappertest.SqlDialectExecutionSuite` carries three identifier checks that run against every warehouse (`TestSqlDialectExecution_IdentQuotingIsAccepted`, `…_WrittenIdentBehavesLikeTheUnquotedReference`, `…_QualifiedIdentNamesTheTable`). They use column aliases and reads, never DDL. Two things they taught that the docs do not say plainly: BigQuery parses `` `we\`ird` `` correctly and then rejects it with "Invalid field name", because what a column may be *named* is a separate rule from how an identifier is quoted; and Trino and Athena report an alias back under the case it was written in even though they resolve object names folded to lower — which is why the fold is probed by resolving a column two ways rather than by reading an alias back.
- **Aliases and qualified refs**: the rule is one spelling convention per statement, not one preferred builder. An alias and every reference to it — a `QualifiedCol`, an outer WHERE, a GROUP BY — must resolve through the same path, because `Identifier` quotes unconditionally on the dialects whose `Identifier` is a quoting function (Oracle, ClickHouse, BigQuery) and so pins the name to the case it was written in, while `ResolveFieldRef` folds. Mixing them puts `as "key_val"` next to a reference to `key_val`, which on Oracle names two different columns — the reference either fails or silently resolves to a same-named column of the inner table.
  - For new SQL, use `Alias` for the name and `QualifiedCol` for references to it. Both go through `ResolveFieldRef`, so they agree by construction; `TestSubqueryTableAliasMatchesQualifiedCol` pins that across every dialect.
  - When building on top of SQL that already declared its columns with `Identifier`, keep quoting references with `Identifier` too. `synq-recon`'s `CombineSideQueries` does this deliberately, because the side queries it reads from declare their columns that way.
  - So converting an existing statement is all-or-nothing. Moving one half of a statement onto `Alias`/`QualifiedCol` and leaving the other on `Identifier` produces exactly the failure above — if `pkg/segment` moves, check whether `checksum/builder.go` has to move with it.
  - What the warehouse then reports a column as is still its own choice, so reads stay case-insensitive either way (`exec.QueryMapResult.Get`).
- **Window functions**: `sqldialect.Over(fn).PartitionBy(...).OrderBy(...)` with `RowNumber()` / `Ntile(n)`. Frame clauses (ROWS/RANGE BETWEEN) are deliberately not modelled. Every dialect we support requires the `OVER` to carry an `ORDER BY` for `NTILE`.
- **Query Helpers**: `scrapper/stdsql/` helpers (`QueryShape`, `QueryCustomMetrics`) accept `RowQuerier` interface — pass the executor directly, not `GetDb()`. Use `stdsql.RawDB{DB: db}` wrapper only in tests with raw `*sqlx.DB`.

## Snowflake DDL Parsing

- Snowflake DDL parsing uses `go-sqllexer` with `sqllexer.DBMSSnowflake` — never use regex for SQL parsing
- `GET_DDL('SCHEMA', ...)` returns full DDL including `WITH TAG (...)` and `COMMENT` clauses
- When permissions are insufficient, Snowflake returns `UNKNOWN_TAG='#UNKNOWN_VALUE'` sentinels — filter these out
- Column-level `COMMENT` appears inside `()` of column defs; table-level `COMMENT` appears after — use parenthesis depth tracking to disambiguate
- Snowflake supports both `COMMENT='value'` and `COMMENT 'value'` syntax
- **`cluster by (...)` between the table name and the column list defeats the per-object DDL split** — `ParseCreateStatementsPerObject` drops those tables, so `QuerySqlDefinitions` returns them with an empty `Sql` even though `GET_DDL` did contain them. The same table without CLUSTER BY parses.
- **A SECURE VIEW has no definition for a non-owner role** — neither `information_schema.views.view_definition` nor `GET_DDL` returns it, so an empty `Sql` there is Snowflake behaviour, not a bug.

## Important Rules

- **Public repo**: Never include customer-specific data (table names, schemas, tag values) in test files — use generic placeholders like `MY_DB.MY_SCHEMA.MY_TABLE`
- **Errors: use `github.com/pkg/errors`, not stdlib `errors`/`fmt.Errorf`.** `errors.New` / `errors.Errorf` / `errors.Wrap` capture a stacktrace, which is what makes failures in the `sqldialect` AST (`ToSql` returning an error from deep in a nested expression) actually debuggable. Plain `fmt.Errorf` loses that.

## MySQL Gotchas

- **Post-processor pattern**: MySQL sets only `row.Database = e.conf.Host` (not `Instance`) in scrapper post-processors. `ResolveExternalDatabase` for MySQL uses `database` as the `HostId` and ignores `instance` entirely — consistent with BigQuery's pattern.
- **MariaDB vs MySQL detection**: `MySQLScrapper` detects MariaDB at construction via `SELECT VERSION()` (contains "mariadb"). Used for SQL branching — e.g., MySQL has `ENFORCED` column in `TABLE_CONSTRAINTS` for CHECK constraints, MariaDB does not.
- **MySQL FQN mapping**: MySQL `ResolveFqn` uses `datasetId.tableId`. When constructing `TableFqn` for MySQL, put database name in `datasetId` (second arg), not `projectId` (first arg): `TableFqn("", dbName, tableName)`.
- **MySQL dialect compatibility**: `DATE_ADD`/`DATE_SUB` (not `DATEADD`), `CAST AS DOUBLE` (not `FLOAT`), `NULL` for `MEDIAN` (no built-in aggregate). These work on both MySQL and MariaDB.

## Oracle & MSSQL Gotchas

- **Oracle rejects `AS` before a table alias**: `(select ...) AS t` fails with `ORA-03048: SQL reserved word 'AS' is not syntactically valid`. It is optional in the standard and accepted everywhere else, so it is a dialect flag — `Dialect.SupportsAsBeforeTableAlias()`, false only for Oracle.
- **Oracle requires an unquoted identifier to start with a letter**: `_` / `$` / `#` are legal inside one but not at the front, so a generated alias like `_recon_base` fails with `ORA-00911: _: invalid character`. `OracleQuoteIfNeeded` quotes those; every other dialect takes a leading underscore raw.
- **go-ora time.Time binding**: go-ora's `time.Time` bind parameters don't compare correctly with Oracle DATE columns. Use `TO_DATE(:1, 'YYYY-MM-DD HH24:MI:SS')` with `t.UTC().Format("2006-01-02 15:04:05")` string parameters instead.
- **MSSQL DB_NAME() consistency**: Always use `DB_NAME()` in SQL queries to populate the database field, never `conf.Database` — avoids casing mismatches between user config and SQL Server's canonical name.
- **sqldialect ResolveTime timezone**: Dialects that format time without timezone info (Oracle, MSSQL, ClickHouse) must call `.UTC()` before formatting to prevent wrong comparisons when Go runs in non-UTC timezone.
- **MSSQL Query Store testing**: Azure SQL Edge defaults to `QUERY_CAPTURE_MODE = AUTO` (skips infrequent queries). Tests need `QUERY_CAPTURE_MODE = ALL` and `EXEC sp_query_store_flush_db` before asserting.
- **MSSQL dialect compatibility**: Use `DATEADD+DATEDIFF` pattern for time truncation (not `DATETRUNC` — SQL Server 2022+ only, not in Azure SQL Edge). `MEDIAN` returns `NULL` (PERCENTILE_CONT is a window function, can't mix with aggregates).
- **MSSQL string length**: MSSQL uses `LEN()` not `LENGTH()`. Always use `dialect.StringLength()` — never hardcode `Fn("length", ...)` in metrics queries.
- **PostgreSQL MEDIAN**: Use `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY expr)` — no built-in `MEDIAN` function.

## Athena Gotchas

- **AWS credential chain is opt-in.** `exec/athena/AthenaConf.AllowDefaultChain` gates fall-through to env vars / EC2/EKS instance role / shared config. Set it `true` only in callers running inside the customer's own infrastructure (e.g. on-prem agent — `connect.Athena()` does this). Hosted/SaaS callers MUST leave it `false` — otherwise a customer config with empty auth fields would inherit the host process's AWS identity.
- **`SHOW CREATE` quoting is asymmetric** (and undocumented in AWS): `SHOW CREATE TABLE` accepts only backticks (ANSI double-quotes return "Queries of this type are not supported"); `SHOW CREATE VIEW` accepts only ANSI double-quotes (backticks return "backquoted identifiers are not supported"). Trailing `/* ... */` SQL comments are fine. The catalog prefix is rejected on both — connection is bound to a single Glue Data Catalog.
- **`SHOW CREATE TABLE` for Hive externals probes the `default` Glue DB** internally, regardless of which DB the table lives in. The IAM principal must grant `glue:GetDatabase` on `arn:aws:glue:<region>:<account>:database/default` or every external-table DDL fetch fails with `AccessDeniedException`. Iceberg / managed tables are unaffected.
- **Driver: `influxdata/athenadriver/v2`** (not the unmaintained `uber/athenadriver`) — uses `aws-sdk-go-v2`, matching the rest of our deps. Registers as `awsathena`.
- **Workgroup result location is mandatory.** `NewAthenaExecutor` calls `athena:GetWorkGroup` at startup and refuses if `ResultConfiguration.OutputLocation` is empty. This forces customers to set `EnforceWorkGroupConfiguration=true` so per-query overrides cannot escape the workgroup's data-scan cap.
- **Credentials are materialized once.** The driver doesn't accept a credentials provider, so AssumeRole / profile / chain paths get snapshot at executor construction. Long-lived processes using STS-backed creds must recreate the executor before token expiry (default 1h, max 12h for AssumeRole).
- **Glue-API methods must merge both scopes.** Methods that can't push scope into SQL (`QueryTableMetrics`, `QueryTableConstraints` — Glue API, not Athena SQL) must filter with `scope.Merge(e.conf.Scope, scope.GetScope(ctx))` via `e.effectiveScope(ctx)`. Using only `conf.Scope` ignores per-call `WithScope`. SQL-based methods get this for free via `AppendScopeConditions(ctx, ...)`.
- **`QueryDatabases` is schema-granular**: `DatabaseRow.Database` = Glue database, while `QuerySchemas` sets `Database`=catalog, `Schema`=Glue db. Cross-method db-subset checks must account for this mismatch.

## ClickHouse Gotchas

- **Exclude both `information_schema` AND `INFORMATION_SCHEMA`** (ClickHouse exposes both as aliases) in every catalog query (`query_tables`, `query_catalog`, `query_sql_definitions`, `query_table_metrics`, `query_table_constraints`). Excluding only the lowercase form leaks ~20 uppercase system views.
- **A ClickHouse database maps to our Schema** — there is no catalog level above it. `SchemaRow.Database`/`TableRow.Database` is the configured host/database label, not a real container.

## Databricks Gotchas

**Every Databricks REST client goes through `exec/databricks`** — `NewWorkspaceClient` for the SDK's workspace client, `NewApiClient` for endpoints the SDK does not model (Unity Catalog's table-lineage endpoint, called from the cloud repo). Never `databricks.NewWorkspaceClient` directly: a stock client retries a rate-limited response blindly for five minutes, ignores `Retry-After` and never reduces the rate it offers, so it absorbs the quota that the callers which do pace themselves have given up.

- **One `Throttle` per workspace host per process** (`exec/databricks/ratelimit.go`), keyed by host so any config spelling resolves to the same one. AIMD: unpaced until the first refusal, then each refusal doubles the spacing handed out to *every* in-flight request and holds them for the longer of `Retry-After` and that spacing, and runs of accepted requests halve it back to unpaced. Nothing is shared between processes and nothing needs to be — both producers observe the same 429 from the same workspace and back off multiplicatively, so they converge on a share of the real quota the way two TCP flows share a link.
- **The pacing lives in an `http.RoundTripper`** (`Config.HTTPTransport`), because that is the *only* hook the SDK gives a `WorkspaceClient` — `ClientConfig.ErrorRetriable` and `TransientErrors` can only be set on a raw `ApiClient`, which is why `NewApiClient` also turns those off explicitly. Sitting below the SDK's error mapping means a refusal has to be recognised in the response: `refusalOf` matches a 429, the `REQUEST_LIMIT_EXCEEDED` / `RESOURCE_EXHAUSTED` codes at any status, and the rate-limit message, because the control plane answers all three shapes.
- **`RateLimitedError.Error()` must not quote what the workspace answered.** The SDK re-sends any error whose *text* contains `REQUEST_LIMIT_EXCEEDED` — its workaround for SCIM answering 500 on a rate limit — and it matches that on the error the transport returns, so an error carrying the API's code or message goes straight back under the blind five-minute retry. The code and message stay on the struct (and in the transport's warning log) for callers to read. Same reason to keep `i/o timeout`, `connection refused`, `connection reset by peer`, `TLS handshake timeout` and `Unexpected error` out of it — `ratelimit_test.go` pins all of them.
- **`WithPacing` raises `HTTPTimeoutSeconds` to 120 when the caller left it unset.** The SDK's per-attempt timeout is an inactivity deadline that starts *before* the request is sent, so it has to cover the time a request spends held back; `Pacing.MaxPause` and `WaitBudget` are what keep the waiting inside it.
- **A rate-limited per-table read fails a metrics run** (`query_table_metrics.go`). The 429s from that per-table `Tables.Get` used to be collected into a `[]string` nothing ever read, so a throttled run reported the tables it had managed to fetch as the workspace's metrics — downstream that is indistinguishable from tables that stopped growing. A read that fails for any other reason (dropped between the listing and the read is the common one) still degrades to the numbers the listing carried.
- **Detection is `IsRateLimitError` / `RateLimitRetryAfter`**, which handle both the SDK's `*apierr.APIError` and the pacing's own error. The ping each constructor comes up with checks it, so a workspace over its quota is not reported as one whose credentials are wrong.
- **Tests**: `UsePacing(workspaceUrl, pacing)` installs millisecond-scale pacing before a client is built — the first caller to ask for a workspace's throttle decides its pacing, so it cannot be swapped under a request in flight. `meteredQuota` (in `exec/databricks/client_test.go` and `scrapper/databricks/rate_limit_test.go`) is a limiter that counts the requests it *refuses* as well as those it admits, the way a real one does: that is what separates a client which paces itself from one which retries harder, and a fake that meters only admitted requests lets an unpaced client pass. `TestAnUnpacedClientStarvesOnTheSameQuota` keeps that fake honest by failing with a stock SDK client. `fakeWorkspace` (`scrapper/databricks/list_test.go`) builds its scrapper through `NewDatabricksScrapper`, so what those tests drive is the client the product builds, and its `refuse` hook sits in front of every endpoint.

## Two flavors of the same proto can't link in one binary

The same `synq/common/v1/scope.proto` (and any other `proto_public/` proto) is generated under two different Go packages downstream consumers may use:

- `buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/common/v1` — this repo's path
- `github.com/getsynq/api/common/v1` — a downstream's workspace-resolved path

Any single binary that links BOTH panics on init — protobuf's global registry rejects duplicate registrations of the same `.proto` file path. So **keep `scrapper/scope` (and any package that exposes runtime types like `*ScopeFilter`) free of proto-generated imports**. Put per-caller proto-conversion next to the call site instead — see `connect/connect.go`'s `athenaScopeFromProto`. The previously-tempting `scope.FromProto` helper was deleted for this reason.
