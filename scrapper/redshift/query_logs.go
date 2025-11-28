package redshift

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
)

//go:embed query_logs.sql
var queryLogsSql string

type RedshiftQueryLogSchema struct {
	UserId                *int64     `db:"user_id"`
	QueryId               int64      `db:"query_id"`
	QueryLabel            *string    `db:"query_label"`
	TransactionId         *int64     `db:"transaction_id"`
	SessionId             *int64     `db:"session_id"`
	DatabaseName          *string    `db:"database_name"`
	QueryType             *string    `db:"query_type"`
	Status                *string    `db:"status"`
	ResultCacheHit        *bool      `db:"result_cache_hit"`
	StartTime             *time.Time `db:"start_time"`
	EndTime               *time.Time `db:"end_time"`
	ElapsedTime           *int64     `db:"elapsed_time"`
	QueueTime             *int64     `db:"queue_time"`
	ExecutionTime         *int64     `db:"execution_time"`
	ErrorMessage          *string    `db:"error_message"`
	ReturnedRows          *int64     `db:"returned_rows"`
	ReturnedBytes         *int64     `db:"returned_bytes"`
	QueryText             *string    `db:"query_text"`
	RedshiftVersion       *string    `db:"redshift_version"`
	UsageLimit            *string    `db:"usage_limit"`
	ComputeType           *string    `db:"compute_type"`
	CompileTime           *int64     `db:"compile_time"`
	PlanningTime          *int64     `db:"planning_time"`
	LockWaitTime          *int64     `db:"lock_wait_time"`
	ServiceClassId        *int64     `db:"service_class_id"`
	ServiceClassName      *string    `db:"service_class_name"`
	QueryPriority         *string    `db:"query_priority"`
	ShortQueryAccelerated *string    `db:"short_query_accelerated"`
	GenericQueryHash      *string    `db:"generic_query_hash"`
	UserQueryHash         *string    `db:"user_query_hash"`
}

func (s *RedshiftScrapper) FetchQueryLogs(
	ctx context.Context,
	from, to time.Time,
	obfuscator querylogs.QueryObfuscator,
) (querylogs.QueryLogIterator, error) {
	// Validate obfuscator is provided
	if obfuscator == nil {
		return nil, fmt.Errorf("obfuscator is required")
	}

	// Use native QueryRows - returns sqlx.Rows iterator
	rows, err := s.Executor().QueryRows(ctx, queryLogsSql, from, to)
	if err != nil {
		return nil, err
	}

	host := s.conf.Host
	database := s.conf.Database
	return querylogs.NewSqlxRowsIterator[RedshiftQueryLogSchema](
		rows,
		obfuscator,
		s.DialectType(),
		func(row *RedshiftQueryLogSchema, obfuscator querylogs.QueryObfuscator, sqlDialect string) (*querylogs.QueryLog, error) {
			return convertRedshiftRowToQueryLog(row, obfuscator, sqlDialect, host, database)
		},
	), nil
}

// trimStringPtr trims whitespace from a string pointer and returns it
// Returns nil if input is nil
func trimStringPtr(s *string) *string {
	if s == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*s)
	return &trimmed
}

func convertRedshiftRowToQueryLog(
	row *RedshiftQueryLogSchema,
	obfuscator querylogs.QueryObfuscator,
	sqlDialect string,
	host string,
	configDatabase string,
) (*querylogs.QueryLog, error) {
	// Determine status - Redshift provides it directly
	status := "UNKNOWN"
	if row.Status != nil {
		status = strings.ToUpper(strings.TrimSpace(*row.Status))
	}

	// Timing information - use end_time as CreatedAt (when query finished/logged)
	var createdAt time.Time
	if row.EndTime != nil {
		createdAt = *row.EndTime
	} else if row.StartTime != nil {
		createdAt = *row.StartTime // Fallback to start if end not available
	} else {
		createdAt = time.Now() // Defensive fallback
	}

	// Build metadata with all Redshift-specific fields
	// Include ALL available fields, even those mapped to higher-level QueryLog fields
	metadata := make(map[string]any)

	// Fields also mapped to higher-level QueryLog fields
	metadata["query_id"] = row.QueryId
	if trimmed := trimStringPtr(row.DatabaseName); trimmed != nil {
		metadata["database_name"] = *trimmed
	}
	if trimmed := trimStringPtr(row.QueryType); trimmed != nil {
		metadata["query_type"] = *trimmed
	}
	if trimmed := trimStringPtr(row.Status); trimmed != nil {
		metadata["status"] = *trimmed
	}
	if row.StartTime != nil {
		metadata["start_time"] = *row.StartTime
	}
	if row.EndTime != nil {
		metadata["end_time"] = *row.EndTime
	}

	// Redshift-specific fields
	if row.UserId != nil {
		metadata["user_id"] = *row.UserId
	}
	if trimmed := trimStringPtr(row.QueryLabel); trimmed != nil {
		metadata["query_label"] = *trimmed
	}
	if row.TransactionId != nil {
		metadata["transaction_id"] = *row.TransactionId
	}
	if row.SessionId != nil {
		metadata["session_id"] = *row.SessionId
	}
	if row.ResultCacheHit != nil {
		metadata["result_cache_hit"] = *row.ResultCacheHit
	}
	if row.ElapsedTime != nil {
		metadata["elapsed_time"] = *row.ElapsedTime
	}
	if row.QueueTime != nil {
		metadata["queue_time"] = *row.QueueTime
	}
	if row.ExecutionTime != nil {
		metadata["execution_time"] = *row.ExecutionTime
	}
	if trimmed := trimStringPtr(row.ErrorMessage); trimmed != nil {
		metadata["error_message"] = *trimmed
	}
	if row.ReturnedRows != nil {
		metadata["returned_rows"] = *row.ReturnedRows
	}
	if row.ReturnedBytes != nil {
		metadata["returned_bytes"] = *row.ReturnedBytes
	}
	if trimmed := trimStringPtr(row.RedshiftVersion); trimmed != nil {
		metadata["redshift_version"] = *trimmed
	}
	if trimmed := trimStringPtr(row.UsageLimit); trimmed != nil {
		metadata["usage_limit"] = *trimmed
	}
	if trimmed := trimStringPtr(row.ComputeType); trimmed != nil {
		metadata["compute_type"] = *trimmed
	}
	if row.CompileTime != nil {
		metadata["compile_time"] = *row.CompileTime
	}
	if row.PlanningTime != nil {
		metadata["planning_time"] = *row.PlanningTime
	}
	if row.LockWaitTime != nil {
		metadata["lock_wait_time"] = *row.LockWaitTime
	}
	if row.ServiceClassId != nil {
		metadata["service_class_id"] = *row.ServiceClassId
	}
	if trimmed := trimStringPtr(row.ServiceClassName); trimmed != nil {
		metadata["service_class_name"] = *trimmed
	}
	if trimmed := trimStringPtr(row.QueryPriority); trimmed != nil {
		metadata["query_priority"] = *trimmed
	}
	if trimmed := trimStringPtr(row.ShortQueryAccelerated); trimmed != nil {
		metadata["short_query_accelerated"] = *trimmed
	}
	if trimmed := trimStringPtr(row.GenericQueryHash); trimmed != nil {
		metadata["generic_query_hash"] = *trimmed
	}
	if trimmed := trimStringPtr(row.UserQueryHash); trimmed != nil {
		metadata["user_query_hash"] = *trimmed
	}

	// Get query text, sanitize and apply obfuscation
	queryText := ""
	if row.QueryText != nil {
		queryText = strings.TrimSpace(strings.ToValidUTF8(*row.QueryText, ""))
		queryText = obfuscator.Obfuscate(queryText)
	}

	// Get query type - trim whitespace
	queryType := ""
	if trimmed := trimStringPtr(row.QueryType); trimmed != nil {
		queryType = *trimmed
	}

	// Build DwhContext
	dwhContext := &querylogs.DwhContext{
		Instance: host,
	}
	if trimmed := trimStringPtr(row.DatabaseName); trimmed != nil && *trimmed != "" {
		dwhContext.Database = *trimmed
	} else {
		// Fall back to config database if not in row data
		dwhContext.Database = configDatabase
	}
	// Redshift doesn't provide user in SYS_QUERY_HISTORY, but we have user_id
	// We'll leave User empty for now

	// Use query_id as QueryID - trim whitespace from generic hash
	queryID := fmt.Sprintf("%d", row.QueryId)
	if trimmed := trimStringPtr(row.GenericQueryHash); trimmed != nil && *trimmed != "" {
		queryID = *trimmed // Prefer generic hash if available
	}

	return &querylogs.QueryLog{
		CreatedAt:                createdAt,
		StartedAt:                row.StartTime, // When query execution started
		FinishedAt:               row.EndTime,   // When query execution finished
		QueryID:                  queryID,
		SQL:                      queryText,
		NormalizedQueryHash:      nil, // Redshift doesn't provide normalized query hash
		SqlDialect:               sqlDialect,
		DwhContext:               dwhContext,
		QueryType:                queryType,
		Status:                   status,
		Metadata:                 querylogs.SanitizeMetadata(metadata),
		SqlObfuscationMode:       obfuscator.Mode(),
		HasCompleteNativeLineage: false, // Redshift doesn't provide lineage in SYS_QUERY_HISTORY
		NativeLineage:            nil,
	}, nil
}
