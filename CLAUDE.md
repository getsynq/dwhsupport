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

## Architecture

### Core Abstraction Layers

The library is organized into three main layers:

1. **Executor Layer** (`exec/`)
   - Low-level database connection and query execution
   - Each warehouse type has its own executor (e.g., `exec/snowflake`, `exec/bigquery`)
   - Executors implement connection logic, `GetDb()`, `QueryRows()`, `Exec()`, `Close()`
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

## Testing

- Tests use `github.com/gkampitakis/go-snaps` for snapshot testing
- Mock generation uses `go.uber.org/mock`
- Test files follow `*_test.go` naming convention
- Integration tests exist for most warehouse implementations

## Special Patterns

- **Lazy Loading**: `lazy/lazy.go` provides lazy initialization pattern
- **SSH Tunneling**: `sshtunnel/ssh_tunnel.go` supports SSH tunnel connections
- **Query Building**: `querybuilder/` provides utilities for dynamic query construction
- **Blocklists**: `blocklist/` provides filtering for databases/schemas
- **Metrics Extraction**: `metrics/` contains logic for extracting and processing metrics from different warehouses
