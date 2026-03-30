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

Follow the rules in `RULE_FOR_NEW_EXECUTER_AND_SCRAPPER.md`:

1. Create executor in `exec/yourwarehouse/`
   - Implement connection logic and `Executor` interface
   - Create `query.go` with `NewQuerier[T]` function

2. Create scrapper in `scrapper/yourwarehouse/`
   - Implement `Scrapper` interface
   - Create separate `query_*.go` files for each method
   - Store SQL queries in `.sql` files when appropriate
   - Return `ErrUnsupported` for unimplemented methods

3. Implement `SqlDialect` in `sqldialect/` if needed

4. Register the new dialect in `sqldialect/dialects.go` `DialectsToTest()` and regenerate snapshots:
   - `UPDATE_SNAPS=true go test ./sqldialect/... ./metrics/... -count=1`

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

Four embeddable test suites in `scrapper/scrappertest/`:
- **ComplianceSuite** — validates all scrapper methods work or return ErrUnsupported
- **ScopeComplianceSuite** — validates scope filtering (include/exclude)
- **MonitorComplianceSuite** — tests QuerySegments/QueryCustomMetrics/QueryShape with dialect-specific SQL
- **MetricsExecutionSuite** — generates SQL via `metrics/querybuilder` and executes against real databases

Integration tests connect to dwhtesting staging databases via Twingate (no port-forwarding needed). Each scrapper package has a `base_test.go` that loads `../../.env` via `godotenv`. Env var prefixes per database:
- `ORACLE_`, `MSSQL_`, `POSTGRES_`, `CLICKHOUSE_` — dwhtesting staging
- `MARIADB_` — MariaDB on dwhtesting staging
- `MYSQL_` — real MySQL on dwhtesting staging
- `STARBURST_` — Starburst Galaxy (HTTPS), `TRINO_` — self-hosted Trino (plaintext HTTP)

## Releases

- Tags with `-rcX` suffix (e.g. `v0.9.0-rc6`) are pre-releases — use `--prerelease` flag when creating with `gh release create`
- RC release changelogs must include all changes since the last **stable** release, not just since the previous RC
- Use `gh release create v0.X.0-rcN --prerelease --generate-notes --notes-start-tag <last-stable-tag>`
- Example: `v0.9.0-rc6` uses `--notes-start-tag v0.8.3` (last stable), not `v0.9.0-rc5`

## Special Patterns

- **Query Logs**: `querylogs/` defines `QueryLogsProvider` interface with `FetchQueryLogs` returning a `QueryLogIterator`. Implementations use `querylogs.NewSqlxRowsIterator[T]` with a warehouse-specific schema struct and converter function. See `scrapper/redshift/query_logs.go` for the canonical pattern.
- **Scrapper Configs**: All scrapper configs must be proper structs embedding their executor config (not type aliases). Each scrapper should have an `Executor()` accessor method. Example: `type MSSQLScrapperConf struct { dwhexecmssql.MSSQLConf }`.
- **Lazy Loading**: `lazy/lazy.go` provides lazy initialization pattern
- **SSH Tunneling**: `sshtunnel/ssh_tunnel.go` supports SSH tunnel connections
- **Query Building**: `querybuilder/` provides utilities for dynamic query construction
- **Blocklists**: `blocklist/` provides filtering for databases/schemas
- **Metrics Extraction**: `metrics/` contains logic for extracting and processing metrics from different warehouses
- **Scope Filtering**: `scrapper/scope/` provides include/exclude scope filtering. SQL files use `/* SYNQ_SCOPE_FILTER */` placeholder at the injection point; `AppendScopeConditions` replaces it with `AND <conditions>` or empty string. Never use heuristic WHERE-append.
- **Scope Compliance Testing**: `scrapper/scrappertest/ScopeComplianceSuite` is an embeddable test suite for validating scope filtering — embed alongside `ComplianceSuite` in warehouse integration tests
- **Query Helpers**: `scrapper/stdsql/` helpers (`QueryShape`, `QueryCustomMetrics`) accept `RowQuerier` interface — pass the executor directly, not `GetDb()`. Use `stdsql.RawDB{DB: db}` wrapper only in tests with raw `*sqlx.DB`.

## Snowflake DDL Parsing

- Snowflake DDL parsing uses `go-sqllexer` with `sqllexer.DBMSSnowflake` — never use regex for SQL parsing
- `GET_DDL('SCHEMA', ...)` returns full DDL including `WITH TAG (...)` and `COMMENT` clauses
- When permissions are insufficient, Snowflake returns `UNKNOWN_TAG='#UNKNOWN_VALUE'` sentinels — filter these out
- Column-level `COMMENT` appears inside `()` of column defs; table-level `COMMENT` appears after — use parenthesis depth tracking to disambiguate
- Snowflake supports both `COMMENT='value'` and `COMMENT 'value'` syntax

## Important Rules

- **Public repo**: Never include customer-specific data (table names, schemas, tag values) in test files — use generic placeholders like `MY_DB.MY_SCHEMA.MY_TABLE`

## MySQL Gotchas

- **Post-processor pattern**: MySQL sets only `row.Database = e.conf.Host` (not `Instance`) in scrapper post-processors. `ResolveExternalDatabase` for MySQL uses `database` as the `HostId` and ignores `instance` entirely — consistent with BigQuery's pattern.
- **MariaDB vs MySQL detection**: `MySQLScrapper` detects MariaDB at construction via `SELECT VERSION()` (contains "mariadb"). Used for SQL branching — e.g., MySQL has `ENFORCED` column in `TABLE_CONSTRAINTS` for CHECK constraints, MariaDB does not.
- **MySQL FQN mapping**: MySQL `ResolveFqn` uses `datasetId.tableId`. When constructing `TableFqn` for MySQL, put database name in `datasetId` (second arg), not `projectId` (first arg): `TableFqn("", dbName, tableName)`.
- **MySQL dialect compatibility**: `DATE_ADD`/`DATE_SUB` (not `DATEADD`), `CAST AS DOUBLE` (not `FLOAT`), `NULL` for `MEDIAN` (no built-in aggregate). These work on both MySQL and MariaDB.

## Oracle & MSSQL Gotchas

- **go-ora time.Time binding**: go-ora's `time.Time` bind parameters don't compare correctly with Oracle DATE columns. Use `TO_DATE(:1, 'YYYY-MM-DD HH24:MI:SS')` with `t.UTC().Format("2006-01-02 15:04:05")` string parameters instead.
- **MSSQL DB_NAME() consistency**: Always use `DB_NAME()` in SQL queries to populate the database field, never `conf.Database` — avoids casing mismatches between user config and SQL Server's canonical name.
- **sqldialect ResolveTime timezone**: Dialects that format time without timezone info (Oracle, MSSQL, ClickHouse) must call `.UTC()` before formatting to prevent wrong comparisons when Go runs in non-UTC timezone.
- **MSSQL Query Store testing**: Azure SQL Edge defaults to `QUERY_CAPTURE_MODE = AUTO` (skips infrequent queries). Tests need `QUERY_CAPTURE_MODE = ALL` and `EXEC sp_query_store_flush_db` before asserting.
- **MSSQL dialect compatibility**: Use `DATEADD+DATEDIFF` pattern for time truncation (not `DATETRUNC` — SQL Server 2022+ only, not in Azure SQL Edge). `MEDIAN` returns `NULL` (PERCENTILE_CONT is a window function, can't mix with aggregates).
- **PostgreSQL MEDIAN**: Use `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY expr)` — no built-in `MEDIAN` function.
