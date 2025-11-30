package redshift

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"google.golang.org/protobuf/types/known/structpb"
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
	// nil values are filtered out by NewMetadataStruct
	metadata := map[string]*structpb.Value{
		// Fields also mapped to higher-level QueryLog fields
		"query_id":      querylogs.IntValue(row.QueryId),
		"database_name": querylogs.TrimmedStringPtrValue(row.DatabaseName),
		"query_type":    querylogs.TrimmedStringPtrValue(row.QueryType),
		"status":        querylogs.TrimmedStringPtrValue(row.Status),
		"start_time":    querylogs.TimePtrValue(row.StartTime),
		"end_time":      querylogs.TimePtrValue(row.EndTime),

		// Redshift-specific fields
		"user_id":                 querylogs.IntPtrValue(row.UserId),
		"query_label":             querylogs.TrimmedStringPtrValue(row.QueryLabel),
		"transaction_id":          querylogs.IntPtrValue(row.TransactionId),
		"session_id":              querylogs.IntPtrValue(row.SessionId),
		"result_cache_hit":        querylogs.BoolPtrValue(row.ResultCacheHit),
		"elapsed_time":            querylogs.IntPtrValue(row.ElapsedTime),
		"queue_time":              querylogs.IntPtrValue(row.QueueTime),
		"execution_time":          querylogs.IntPtrValue(row.ExecutionTime),
		"error_message":           querylogs.TrimmedStringPtrValue(row.ErrorMessage),
		"returned_rows":           querylogs.IntPtrValue(row.ReturnedRows),
		"returned_bytes":          querylogs.IntPtrValue(row.ReturnedBytes),
		"redshift_version":        querylogs.TrimmedStringPtrValue(row.RedshiftVersion),
		"usage_limit":             querylogs.TrimmedStringPtrValue(row.UsageLimit),
		"compute_type":            querylogs.TrimmedStringPtrValue(row.ComputeType),
		"compile_time":            querylogs.IntPtrValue(row.CompileTime),
		"planning_time":           querylogs.IntPtrValue(row.PlanningTime),
		"lock_wait_time":          querylogs.IntPtrValue(row.LockWaitTime),
		"service_class_id":        querylogs.IntPtrValue(row.ServiceClassId),
		"service_class_name":      querylogs.TrimmedStringPtrValue(row.ServiceClassName),
		"query_priority":          querylogs.TrimmedStringPtrValue(row.QueryPriority),
		"short_query_accelerated": querylogs.TrimmedStringPtrValue(row.ShortQueryAccelerated),
		"generic_query_hash":      querylogs.TrimmedStringPtrValue(row.GenericQueryHash),
		"user_query_hash":         querylogs.TrimmedStringPtrValue(row.UserQueryHash),
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
		Metadata:                 querylogs.NewMetadataStruct(metadata),
		SqlObfuscationMode:       obfuscator.Mode(),
		HasCompleteNativeLineage: false, // Redshift doesn't provide lineage in SYS_QUERY_HISTORY
		NativeLineage:            nil,
	}, nil
}
