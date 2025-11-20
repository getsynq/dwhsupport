package bigquery

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

func TestConvertBigQueryRowToQueryLog(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	obfuscatorRedact, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationRedactLiterals)
	require.NoError(t, err)

	startTime := time.Date(2025, 11, 1, 10, 30, 0, 0, time.UTC)
	endTime := time.Date(2025, 11, 1, 10, 35, 0, 0, time.UTC)

	tests := []struct {
		name          string
		row           *BigQueryQueryLogSchema
		obfuscator    querylogs.QueryObfuscator
		expectedSQL   string
		expectedMode  querylogs.ObfuscationMode
		expectedError bool
	}{
		{
			name: "successful_query_with_lineage",
			row: &BigQueryQueryLogSchema{
				CreationTime:        startTime,
				ProjectId:           bigquery.NullString{StringVal: "my-project", Valid: true},
				UserEmail:           bigquery.NullString{StringVal: "user@example.com", Valid: true},
				JobId:               bigquery.NullString{StringVal: "job-123", Valid: true},
				JobType:             bigquery.NullString{StringVal: "QUERY", Valid: true},
				StatementType:       bigquery.NullString{StringVal: "SELECT", Valid: true},
				Priority:            bigquery.NullString{StringVal: "INTERACTIVE", Valid: true},
				StartTime:           startTime,
				EndTime:             endTime,
				Query:               bigquery.NullString{StringVal: "SELECT * FROM users WHERE id = 123", Valid: true},
				State:               bigquery.NullString{StringVal: "DONE", Valid: true},
				TotalBytesProcessed: bigquery.NullInt64{Int64: 1024000, Valid: true},
				TotalSlotMs:         bigquery.NullInt64{Int64: 5000, Valid: true},
				CacheHit:            bigquery.NullBool{Bool: false, Valid: true},
				ReferencedTables: []*BqQueryTable{
					{
						ProjectId: bigquery.NullString{StringVal: "my-project", Valid: true},
						DatasetId: bigquery.NullString{StringVal: "analytics", Valid: true},
						TableId:   bigquery.NullString{StringVal: "users", Valid: true},
					},
				},
				DestinationTable: &BqQueryTable{
					ProjectId: bigquery.NullString{StringVal: "my-project", Valid: true},
					DatasetId: bigquery.NullString{StringVal: "analytics", Valid: true},
					TableId:   bigquery.NullString{StringVal: "results", Valid: true},
				},
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT * FROM users WHERE id = 123",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "successful_query_with_obfuscation",
			row: &BigQueryQueryLogSchema{
				CreationTime:        startTime,
				ProjectId:           bigquery.NullString{StringVal: "my-project", Valid: true},
				UserEmail:           bigquery.NullString{StringVal: "user@example.com", Valid: true},
				JobId:               bigquery.NullString{StringVal: "job-456", Valid: true},
				JobType:             bigquery.NullString{StringVal: "QUERY", Valid: true},
				StatementType:       bigquery.NullString{StringVal: "INSERT", Valid: true},
				StartTime:           startTime,
				EndTime:             endTime,
				Query:               bigquery.NullString{StringVal: "INSERT INTO logs VALUES ('error', 123, 'message')", Valid: true},
				State:               bigquery.NullString{StringVal: "DONE", Valid: true},
				TotalBytesProcessed: bigquery.NullInt64{Int64: 2048000, Valid: true},
			},
			obfuscator:   obfuscatorRedact,
			expectedSQL:  "INSERT INTO logs VALUES (?, ?, ?)",
			expectedMode: querylogs.ObfuscationRedactLiterals,
		},
		{
			name: "failed_query_with_error",
			row: &BigQueryQueryLogSchema{
				CreationTime:  startTime,
				ProjectId:     bigquery.NullString{StringVal: "my-project", Valid: true},
				UserEmail:     bigquery.NullString{StringVal: "user@example.com", Valid: true},
				JobId:         bigquery.NullString{StringVal: "job-789", Valid: true},
				JobType:       bigquery.NullString{StringVal: "QUERY", Valid: true},
				StatementType: bigquery.NullString{StringVal: "SELECT", Valid: true},
				StartTime:     startTime,
				EndTime:       endTime,
				Query:         bigquery.NullString{StringVal: "SELECT * FROM nonexistent_table", Valid: true},
				State:         bigquery.NullString{StringVal: "DONE", Valid: true},
				ErrorResult: &struct {
					Reason  bigquery.NullString `bigquery:"reason"`
					Message bigquery.NullString `bigquery:"message"`
				}{
					Reason:  bigquery.NullString{StringVal: "notFound", Valid: true},
					Message: bigquery.NullString{StringVal: "Table not found", Valid: true},
				},
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT * FROM nonexistent_table",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "query_without_text",
			row: &BigQueryQueryLogSchema{
				CreationTime:  startTime,
				ProjectId:     bigquery.NullString{StringVal: "my-project", Valid: true},
				UserEmail:     bigquery.NullString{StringVal: "user@example.com", Valid: true},
				JobId:         bigquery.NullString{StringVal: "job-999", Valid: true},
				JobType:       bigquery.NullString{StringVal: "LOAD", Valid: true},
				StatementType: bigquery.NullString{StringVal: "LOAD", Valid: true},
				StartTime:     startTime,
				EndTime:       endTime,
				Query:         bigquery.NullString{Valid: false}, // No query text for LOAD jobs
				State:         bigquery.NullString{StringVal: "DONE", Valid: true},
			},
			obfuscator:   obfuscator,
			expectedSQL:  "",
			expectedMode: querylogs.ObfuscationNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log, err := convertBigQueryRowToQueryLog(tt.row, tt.obfuscator, "bigquery")

			if tt.expectedError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, log)

			// Verify basic fields
			require.Equal(t, tt.row.StartTime, log.CreatedAt)
			require.Equal(t, tt.row.JobId.StringVal, log.QueryID)
			require.Equal(t, tt.expectedSQL, log.SQL)
			require.Equal(t, "bigquery", log.SqlDialect)
			require.Equal(t, tt.expectedMode, log.SqlObfuscationMode)
			require.Equal(t, tt.row.StatementType.StringVal, log.QueryType)

			// Verify status
			if tt.row.ErrorResult != nil {
				require.Equal(t, "FAILED", log.Status)
			} else {
				require.Equal(t, "SUCCESS", log.Status)
			}

			// Verify DwhContext
			require.NotNil(t, log.DwhContext)
			if tt.row.UserEmail.Valid {
				require.Equal(t, tt.row.UserEmail.StringVal, log.DwhContext.User)
			}
			if tt.row.ProjectId.Valid {
				require.Equal(t, tt.row.ProjectId.StringVal, log.DwhContext.Database)
			}

			// Verify lineage when present
			if tt.row.ReferencedTables != nil && len(tt.row.ReferencedTables) > 0 {
				require.NotNil(t, log.NativeLineage)
				require.NotEmpty(t, log.NativeLineage.InputTables)
			}
			if tt.row.DestinationTable != nil && tt.row.DestinationTable.ProjectId.Valid {
				require.NotNil(t, log.NativeLineage)
				require.NotEmpty(t, log.NativeLineage.OutputTables)
				require.True(t, log.HasCompleteNativeLineage)
			}

			// Verify metadata contains expected fields
			require.NotNil(t, log.Metadata)
			require.Contains(t, log.Metadata, "job_type")
			require.Contains(t, log.Metadata, "statement_type")
			require.Contains(t, log.Metadata, "total_bytes_processed")

			// Verify error metadata when present
			if tt.row.ErrorResult != nil {
				require.Contains(t, log.Metadata, "error_reason")
				require.Contains(t, log.Metadata, "error_message")
			}
		})
	}
}
