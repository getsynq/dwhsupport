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
		"reservation_id":        querylogs.StringValue(row.ReservationId.StringVal),
		"total_bytes_processed": querylogs.IntValue(row.TotalBytesProcessed.Int64),
		"total_slot_ms":         querylogs.IntValue(row.TotalSlotMs.Int64),
		"cache_hit":             querylogs.BoolValue(row.CacheHit.Bool),
		"total_bytes_billed":    querylogs.IntValue(row.TotalBytesBilled.Int64),
		"transaction_id":        querylogs.StringValue(row.TransactionId.StringVal),
		"parent_job_id":         querylogs.StringValue(row.ParentJobId.StringVal),
		"transferred_bytes":     querylogs.IntValue(row.TransferredBytes.Int64),
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

	// Timing information
	startedAt := row.StartTime
	finishedAt := row.EndTime

	return &querylogs.QueryLog{
		CreatedAt:                row.EndTime, // Use EndTime as CreatedAt (when query finished/logged)
		StartedAt:                &startedAt,  // When query execution started
		FinishedAt:               &finishedAt, // When query execution finished
		QueryID:                  row.JobId.StringVal,
		SQL:                      queryText,
		NormalizedQueryHash:      nil, // BigQuery doesn't provide normalized query hash
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
