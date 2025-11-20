package querylogs

import (
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
)

// ObfuscationMode represents the level of SQL obfuscation applied to query logs.
// This is critical for on-premise deployments where customers want to prevent
// sensitive data in SQL queries from being sent to SYNQ backend.
type ObfuscationMode int

const (
	// ObfuscationNone indicates no obfuscation was applied
	ObfuscationNone ObfuscationMode = 0

	// ObfuscationRedactLiterals indicates string and numeric literals were replaced
	// with placeholders while preserving query structure for SQL parsing
	ObfuscationRedactLiterals ObfuscationMode = 1

	// ObfuscationRemoveQuery indicates the entire SQL query was removed (future feature)
	ObfuscationRemoveQuery ObfuscationMode = 2
)

// QueryLog represents a standardized query log entry from any data warehouse platform.
// It provides a common structure while preserving platform-specific details in Metadata.
type QueryLog struct {
	// CreatedAt is the timestamp when the query was executed
	CreatedAt time.Time

	// QueryID is the native query identifier or computed hash
	QueryID string

	// SQL is the query text (may be obfuscated based on SqlObfuscationMode)
	SQL string

	// SqlHash is a hash of the original SQL for deduplication and caching
	SqlHash string

	// DwhContext contains structured context information (database, schema, warehouse, user, role, etc.)
	DwhContext *DwhContext

	// QueryType is the platform-specific query type (e.g., "CREATE_TABLE_AS_SELECT", "SELECT", "INSERT")
	QueryType string

	// Status represents the query execution status ("SUCCESS", "FAILED", "CANCELED", etc.)
	Status string

	// Metadata contains platform-specific fields that don't fit into the standard structure
	Metadata map[string]interface{}

	// SqlObfuscationMode indicates how the SQL was obfuscated (if at all)
	SqlObfuscationMode ObfuscationMode

	// HasCompleteNativeLineage indicates that NativeLineage contains complete lineage information
	// and SQL parsing can be skipped entirely. When true, the backend should trust the native
	// lineage and avoid expensive SQL parsing.
	HasCompleteNativeLineage bool

	// IsTruncated indicates whether the SQL text was truncated by the data warehouse.
	// When true, the SQL field contains incomplete query text and should not be parsed.
	// This is common in systems that limit query text length in their system tables.
	IsTruncated bool

	// NativeLineage contains table lineage information when provided natively by the platform
	// (e.g., BigQuery, ClickHouse). For other platforms, this will be nil and SQL parsing is required.
	NativeLineage *NativeLineage
}

// DwhContext represents the execution context of a query with optional fields
// that may or may not be available depending on the data warehouse platform.
//
// These mappings MUST match exactly what's in scrapper/*/query_tables.go for consistency.
//
// Platform-specific mappings:
//   - Snowflake: Instance=account, Database=database_name, Schema=schema_name
//   - Databricks: Instance=workspace_url, Database=catalog_name, Schema=schema_name
//   - BigQuery: Instance="", Database=project_id, Schema=dataset_id
//   - Redshift: Instance=host, Database=database_name, Schema=schema_name
//   - Postgres: Instance=host, Database=database_name, Schema=schema_name
//   - Trino: Instance=host, Database=catalog, Schema=schema
//   - MySQL: Instance="", Database=host, Schema=schema_name
//   - ClickHouse: Instance="", Database=hostname (or configured database alias), Schema=database_name
//   - DuckDB: Instance=motherduck_account, Database="", Schema=schema_name
type DwhContext struct {
	// Instance is the unique identifier for the data warehouse instance.
	// This is typically the hostname, account ID, or workspace URL that uniquely
	// identifies the data warehouse deployment.
	// - Set by: Snowflake (account), Databricks (workspace_url), Redshift/Postgres/Trino (host), DuckDB (motherduck_account)
	// - Empty for: BigQuery, ClickHouse, MySQL
	Instance string

	// Database name (may be empty for platforms without database level)
	// - Snowflake: database_name
	// - Databricks: catalog_name
	// - BigQuery: project_id
	// - Redshift/Postgres: database_name
	// - Trino: catalog
	// - MySQL: host (used as instance identifier)
	// - ClickHouse: hostname or configured database alias from config (used as instance identifier, NOT the schema-level database)
	// - DuckDB: "" (empty)
	Database string

	// Schema name (may be empty for platforms without schema concept)
	// - Snowflake/Databricks/Redshift/Postgres/MySQL: schema_name
	// - BigQuery: dataset_id
	// - Trino: schema
	// - ClickHouse: database_name (2-level hierarchy)
	// - DuckDB: schema_name
	Schema string

	// Warehouse identifier (available in Snowflake, Databricks)
	Warehouse string

	// User who executed the query
	User string

	// Role used to execute the query (available in Snowflake, Postgres, etc.)
	Role string

	// Cluster identifier or hostname (for platforms that use cluster concept)
	// Redshift: cluster hostname
	// ClickHouse: cluster name (if applicable)
	Cluster string
}

// NativeLineage represents table lineage information provided natively by the platform.
// This is available for platforms like BigQuery and ClickHouse that expose lineage metadata.
// Uses scrapper.DwhFqn for table references (InstanceName can be left empty when not applicable).
type NativeLineage struct {
	// InputTables are the tables read by the query
	InputTables []scrapper.DwhFqn

	// OutputTables are the tables written by the query
	OutputTables []scrapper.DwhFqn
}
