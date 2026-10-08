package bigquery

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/getsynq/dwhsupport/scrapper"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/structpb"
)

type BqQueryTable struct {
	ProjectId bigquery.NullString `bigquery:"project_id"`
	DatasetId bigquery.NullString `bigquery:"dataset_id"`
	TableId   bigquery.NullString `bigquery:"table_id"`
}

type BigQueryQueryLogSchema struct {
	CreationTime        time.Time           `bigquery:"creation_time"`
	ProjectId           bigquery.NullString `bigquery:"project_id"`
	ProjectNumber       bigquery.NullInt64  `bigquery:"project_number"`
	UserEmail           bigquery.NullString `bigquery:"user_email"`
	JobId               bigquery.NullString `bigquery:"job_id"`
	JobType             bigquery.NullString `bigquery:"job_type"`
	StatementType       bigquery.NullString `bigquery:"statement_type"`
	Priority            bigquery.NullString `bigquery:"priority"`
	StartTime           time.Time           `bigquery:"start_time"`
	EndTime             time.Time           `bigquery:"end_time"`
	Query               bigquery.NullString `bigquery:"query"`
	State               bigquery.NullString `bigquery:"state"`
	ReservationId       bigquery.NullString `bigquery:"reservation_id"`
	TotalBytesProcessed bigquery.NullInt64  `bigquery:"total_bytes_processed"`
	TotalSlotMs         bigquery.NullInt64  `bigquery:"total_slot_ms"`
	ErrorResult         *struct {
		Reason  bigquery.NullString `bigquery:"reason"`
		Message bigquery.NullString `bigquery:"message"`
	} `bigquery:"error_result"`
	CacheHit         bigquery.NullBool `bigquery:"cache_hit"`
	DestinationTable *BqQueryTable     `bigquery:"destination_table"`
	ReferencedTables []*BqQueryTable   `bigquery:"referenced_tables"`
	Labels           []*struct {
		Key   bigquery.NullString `bigquery:"key"`
		Value bigquery.NullString `bigquery:"value"`
	} `bigquery:"labels"`
	JobStages []*struct {
		Name           bigquery.NullString `bigquery:"name"`
		RecordsRead    bigquery.NullInt64  `bigquery:"records_read"`
		RecordsWritten bigquery.NullInt64  `bigquery:"records_written"`
		Status         bigquery.NullString `bigquery:"status"`
	} `bigquery:"job_stages"`
	TotalBytesBilled bigquery.NullInt64  `bigquery:"total_bytes_billed"`
	TransactionId    bigquery.NullString `bigquery:"transaction_id"`
	ParentJobId      bigquery.NullString `bigquery:"parent_job_id"`
	TransferredBytes bigquery.NullInt64  `bigquery:"transferred_bytes"`
	// QueryHashes is read from query_info (see queryLogColumnExpressions).
	QueryHashes *struct {
		NormalizedLiterals bigquery.NullString `bigquery:"normalized_literals"`
	} `bigquery:"query_hashes"`
	// SessionId is read from session_info (see queryLogColumnExpressions).
	SessionId bigquery.NullString `bigquery:"session_id"`
}

// queryLogColumnExpressions selects the columns of BigQueryQueryLogSchema that are not top-level
// columns of INFORMATION_SCHEMA.JOBS.
var queryLogColumnExpressions = map[string]string{
	"query_hashes": "query_info.query_hashes AS query_hashes",
	"session_id":   "session_info.session_id AS session_id",
}

type bigqueryQueryLogIterator struct {
	iter       *bigquery.RowIterator
	obfuscator querylogs.QueryObfuscator
	sqlDialect string
	closed     bool
}

func (s *BigQueryScrapper) FetchQueryLogs(
	ctx context.Context,
	from, to time.Time,
	obfuscator querylogs.QueryObfuscator,
) (querylogs.QueryLogIterator, error) {
	// Validate obfuscator is provided
	if obfuscator == nil {
		return nil, fmt.Errorf("obfuscator is required")
	}
	sqlQuery, err := s.buildQueryLogsSql(from, to)
	if err != nil {
		return nil, err
	}

	// Use native queryRows - returns bigquery.RowIterator
	iter, err := s.queryRows(ctx, sqlQuery)
	if err != nil {
		return nil, err
	}

	return &bigqueryQueryLogIterator{
		iter:       iter,
		obfuscator: obfuscator,
		sqlDialect: s.DialectType(),
	}, nil
}

func (it *bigqueryQueryLogIterator) Next(ctx context.Context) (*querylogs.QueryLog, error) {
	if it.closed {
		return nil, io.EOF
	}

	// Check context cancellation
	select {
	case <-ctx.Done():
		it.Close()
		return nil, ctx.Err()
	default:
	}

	// Use native iterator.Next()
	var row BigQueryQueryLogSchema
	err := it.iter.Next(&row)
	if err == iterator.Done {
		// Normal completion - auto-close
		it.Close()
		return nil, io.EOF
	}
	if err != nil {
		// Don't auto-close on error - caller might want to inspect
		return nil, err
	}

	// Convert to QueryLog
	log, err := convertBigQueryRowToQueryLog(&row, it.obfuscator, it.sqlDialect)
	if err != nil {
		// Defensive: conversion error
		return nil, err
	}

	return log, nil
}

func (it *bigqueryQueryLogIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	// BigQuery RowIterator doesn't have an explicit Close method
	return nil
}

// getDBFields extracts bigquery tag values from struct fields
func getBigQueryFields(v interface{}) []string {
	var fields []string
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("bigquery")
		if tag != "" && tag != "-" {
			fields = append(fields, tag)
		}
	}

	return fields
}

func (s *BigQueryScrapper) buildQueryLogsSql(from, to time.Time) (string, error) {
	schemaColumns := getBigQueryFields(&BigQueryQueryLogSchema{})
	for i, column := range schemaColumns {
		if expression, ok := queryLogColumnExpressions[column]; ok {
			schemaColumns[i] = expression
		}
	}

	wheres := []string{
		"state = 'DONE'",
		"(job_type = 'LOAD' OR job_type = 'QUERY')",
		fmt.Sprintf("creation_time between '%s' and '%s'", from.Format("2006-01-02 15:04:05"), to.Format("2006-01-02 15:04:05")),
	}

	tableName := fmt.Sprintf("`%s`.`region-%s`.INFORMATION_SCHEMA.JOBS", s.conf.ProjectId, s.conf.Region)

	// Build SQL manually
	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		strings.Join(schemaColumns, ", "),
		tableName,
		strings.Join(wheres, " AND "))

	return sql, nil
}

func convertBigQueryRowToQueryLog(row *BigQueryQueryLogSchema, obfuscator querylogs.QueryObfuscator, sqlDialect string) (*querylogs.QueryLog, error) {
	// Determine status
	status := "SUCCESS"
	if row.ErrorResult != nil {
		status = "FAILED"
	}

	// Build native lineage from referenced_tables and destination_table
	var nativeLineage *querylogs.NativeLineage
	var inputTables []scrapper.DwhFqn
	var outputTables []scrapper.DwhFqn

	// Convert referenced tables (input tables)
	for _, t := range row.ReferencedTables {
		if t != nil && t.ProjectId.Valid && t.DatasetId.Valid && t.TableId.Valid {
			inputTables = append(inputTables, scrapper.DwhFqn{
				DatabaseName: t.ProjectId.StringVal, // BigQuery: project is database
				SchemaName:   t.DatasetId.StringVal, // BigQuery: dataset is schema
				ObjectName:   t.TableId.StringVal,
			})
		}
	}

	// Convert destination table (output table)
	if row.DestinationTable != nil && row.DestinationTable.ProjectId.Valid && row.DestinationTable.DatasetId.Valid &&
		row.DestinationTable.TableId.Valid {
		outputTables = append(outputTables, scrapper.DwhFqn{
			DatabaseName: row.DestinationTable.ProjectId.StringVal,
			SchemaName:   row.DestinationTable.DatasetId.StringVal,
			ObjectName:   row.DestinationTable.TableId.StringVal,
		})
	}

	// Only create lineage if we have input or output tables
	if len(inputTables) > 0 || len(outputTables) > 0 {
		nativeLineage = &querylogs.NativeLineage{
			InputTables:  inputTables,
			OutputTables: outputTables,
		}
	}

	// Build metadata with all BigQuery-specific fields
	// Include ALL available fields, even those mapped to higher-level QueryLog fields
	metadata := map[string]*structpb.Value{
		// Fields also mapped to higher-level QueryLog fields
		"job_id":         querylogs.StringValue(row.JobId.StringVal),
		"user_email":     querylogs.StringValue(row.UserEmail.StringVal),
		"project_id":     querylogs.StringValue(row.ProjectId.StringVal),
		"creation_time":  querylogs.TimeValue(row.CreationTime), // BigQuery job creation time (when queued)
		"start_time":     querylogs.TimeValue(row.StartTime),
		"end_time":       querylogs.TimeValue(row.EndTime),
		"statement_type": querylogs.StringValue(row.StatementType.StringVal),

		// BigQuery-specific fields
		"state":                 querylogs.StringValue(row.State.StringVal), // Raw BigQuery state (e.g., "DONE")
		"project_number":        querylogs.IntValue(row.ProjectNumber.Int64),
		"job_type":              querylogs.StringValue(row.JobType.StringVal),
		"priority":              querylogs.StringValue(row.Priority.StringVal),
		"reservation_id":        nullStringValue(row.ReservationId),
		"total_bytes_processed": nullInt64Value(row.TotalBytesProcessed),
		"total_slot_ms":         nullInt64Value(row.TotalSlotMs),
		"cache_hit":             querylogs.BoolValue(row.CacheHit.Bool),
		"total_bytes_billed":    nullInt64Value(row.TotalBytesBilled),
		"transaction_id":        querylogs.StringValue(row.TransactionId.StringVal),
		"parent_job_id":         querylogs.StringValue(row.ParentJobId.StringVal),
		"session_id":            querylogs.StringValue(row.SessionId.StringVal),
		"transferred_bytes":     nullInt64Value(row.TransferredBytes),
	}

	// Add error details if present
	if row.ErrorResult != nil {
		metadata["error_reason"] = querylogs.StringValue(row.ErrorResult.Reason.StringVal)
		metadata["error_message"] = querylogs.StringValue(row.ErrorResult.Message.StringVal)
	}

	// Add labels
	if len(row.Labels) > 0 {
		labels := make(map[string]*structpb.Value)
		for _, l := range row.Labels {
			if l != nil && l.Key.Valid && l.Value.Valid {
				labels[l.Key.StringVal] = querylogs.StringValue(l.Value.StringVal)
			}
		}
		if len(labels) > 0 {
			metadata["labels"] = querylogs.StructValue(labels)
		}
	}

	// Add job stages statistics
	if len(row.JobStages) > 0 {
		// Get last job stage if it's an output stage
		lastJob := row.JobStages[len(row.JobStages)-1]
		if lastJob != nil && strings.Contains(lastJob.Name.StringVal, "Output") {
			if lastJob.RecordsRead.Valid {
				metadata["records_read"] = querylogs.IntValue(lastJob.RecordsRead.Int64)
			}
			if lastJob.RecordsWritten.Valid {
				metadata["records_written"] = querylogs.IntValue(lastJob.RecordsWritten.Int64)
			}
		}
	}

	// Get query text, sanitize and apply obfuscation
	queryText := ""
	if row.Query.Valid {
		queryText = strings.TrimSpace(strings.ToValidUTF8(row.Query.StringVal, ""))
		queryText = obfuscator.Obfuscate(queryText)
	}

	// Build DwhContext
	dwhContext := &querylogs.DwhContext{}
	if row.UserEmail.Valid {
		dwhContext.User = row.UserEmail.StringVal
	}
	if row.ProjectId.Valid {
		dwhContext.Database = row.ProjectId.StringVal // BigQuery: project is database
	}
	// BigQuery doesn't have a default schema/dataset context in INFORMATION_SCHEMA.JOBS

	// normalized_literals ignores comments, parameter values and literals. BigQuery sets it for query
	// jobs only, so a load job has none.
	var normalizedQueryHash *string
	if row.QueryHashes != nil && row.QueryHashes.NormalizedLiterals.Valid && row.QueryHashes.NormalizedLiterals.StringVal != "" {
		hash := row.QueryHashes.NormalizedLiterals.StringVal
		normalizedQueryHash = &hash
	}

	// A job run in a session belongs to that session. A statement of a multi-statement query runs as a
	// child job of the script, so the script job groups its statements. A job in neither has none.
	var sessionID *string
	switch {
	case row.SessionId.Valid && row.SessionId.StringVal != "":
		sessionID = &row.SessionId.StringVal
	case row.ParentJobId.Valid && row.ParentJobId.StringVal != "":
		sessionID = &row.ParentJobId.StringVal
	}

	// Timing information
	startedAt := row.StartTime
	finishedAt := row.EndTime

	return &querylogs.QueryLog{
		CreatedAt:                row.EndTime, // Use EndTime as CreatedAt (when query finished/logged)
		StartedAt:                &startedAt,  // When query execution started
		FinishedAt:               &finishedAt, // When query execution finished
		QueryID:                  row.JobId.StringVal,
		SQL:                      queryText,
		NormalizedQueryHash:      normalizedQueryHash,
		SessionID:                sessionID,
		SqlDialect:               sqlDialect,
		DwhContext:               dwhContext,
		QueryType:                row.StatementType.StringVal,
		Status:                   status,
		Metadata:                 querylogs.NewMetadataStruct(metadata),
		SqlObfuscationMode:       obfuscator.Mode(),
		HasCompleteNativeLineage: nativeLineage != nil && len(nativeLineage.OutputTables) > 0, // BigQuery provides complete lineage
		NativeLineage:            nativeLineage,
	}, nil
}

// nullInt64Value is a figure the job reported, zero included, and nil for a NULL: a job that reported
// nothing must not read as one that cost nothing.
func nullInt64Value(v bigquery.NullInt64) *structpb.Value {
	if !v.Valid {
		return nil
	}
	return querylogs.IntValue(v.Int64)
}

// nullStringValue is nil for a NULL or an empty string.
func nullStringValue(v bigquery.NullString) *structpb.Value {
	if !v.Valid || v.StringVal == "" {
		return nil
	}
	return querylogs.StringValue(v.StringVal)
}
