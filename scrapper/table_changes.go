package scrapper

import (
	"context"
	"time"
)

// TableChangeEvent represents a single modification event on a table.
type TableChangeEvent struct {
	// When the change was committed. Use for time-travel AS OF.
	Timestamp time.Time

	// Database-specific version identifier.
	// Databricks: Delta version number. BigQuery: job_id. Snowflake: empty (hourly bucket).
	Version string

	// Type of operation: "INSERT", "UPDATE", "DELETE", "MERGE", "COPY", "TRUNCATE", "OTHER"
	Operation string

	// Row counts (nil if not available)
	RowsInserted *int64
	RowsUpdated  *int64
	RowsDeleted  *int64
}

// TableChangeHistoryProvider returns recent modification events for a table,
// ordered by timestamp descending (most recent first).
//
// Enables constructing time-travel queries:
//
//	AT(TIMESTAMP => event.Timestamp - 1s)   -- Snowflake
//	FOR SYSTEM_TIME AS OF event.Timestamp    -- BigQuery
//	TIMESTAMP AS OF event.Timestamp          -- Databricks
type TableChangeHistoryProvider interface {
	FetchTableChangeHistory(
		ctx context.Context,
		fqn DwhFqn,
		from, to time.Time,
		limit int,
	) ([]*TableChangeEvent, error)
}
