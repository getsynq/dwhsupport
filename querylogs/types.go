package querylogs

import (
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
)

// QueryLog represents a standardized query log entry from any data warehouse platform.
// It provides a common structure while preserving platform-specific details in Metadata.
type QueryLog struct {
	// CreatedAt is the timestamp when the query was executed
	CreatedAt time.Time

	// QueryID is the native query identifier or computed hash
	QueryID string

	// SQL is the query text
	SQL string

	// DwhContext contains structured context information (database, schema, warehouse, user, role, etc.)
	DwhContext *DwhContext

	// QueryType is the platform-specific query type (e.g., "CREATE_TABLE_AS_SELECT", "SELECT", "INSERT")
	QueryType string

	// Status represents the query execution status ("SUCCESS", "FAILED", "CANCELED", etc.)
	Status string

	// Metadata contains platform-specific fields that don't fit into the standard structure
	Metadata map[string]interface{}

	// NativeLineage contains table lineage information when provided natively by the platform
	// (e.g., BigQuery, ClickHouse). For other platforms, this will be nil and SQL parsing is required.
	NativeLineage *NativeLineage
}

// DwhContext represents the execution context of a query with optional fields
// that may or may not be available depending on the data warehouse platform.
type DwhContext struct {
	// Database name (called "project" in BigQuery, "database_name" in Snowflake)
	Database string

	// Schema name (called "dataset" in BigQuery, "schema_name" in Snowflake)
	Schema string

	// Warehouse identifier (available in Snowflake, Databricks)
	Warehouse string

	// User who executed the query
	User string

	// Role used for query execution (primarily Snowflake)
	Role string

	// Cluster identifier (for platforms that use cluster concept)
	Cluster string

	// Catalog name (for platforms supporting catalog concept like Databricks)
	Catalog string
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
