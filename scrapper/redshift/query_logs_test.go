package redshift

import (
	"strings"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

func TestConvertRedshiftRowToQueryLog(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	obfuscatorRedact, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationRedactLiterals)
	require.NoError(t, err)

	startTime := time.Date(2025, 11, 1, 10, 30, 0, 0, time.UTC)
	endTime := time.Date(2025, 11, 1, 10, 35, 0, 0, time.UTC)

	userId := int64(100)
	queryId := int64(12345)
	transactionId := int64(67890)
	sessionId := int64(111)
	elapsedTime := int64(5000)
	executionTime := int64(4800)
	queueTime := int64(200)
	returnedRows := int64(100)
	returnedBytes := int64(10000)
	compileTime := int64(50)
	planningTime := int64(100)
	lockWaitTime := int64(0)
	serviceClassId := int64(6)
	resultCacheHit := false

	tests := []struct {
		name          string
		row           *RedshiftQueryLogSchema
		obfuscator    querylogs.QueryObfuscator
		expectedSQL   string
		expectedMode  querylogs.ObfuscationMode
		expectedError bool
	}{
		{
			name: "successful_query",
			row: &RedshiftQueryLogSchema{
				UserId:           &userId,
				QueryId:          queryId,
				DatabaseName:     strPtr("analytics"),
				QueryType:        strPtr("SELECT"),
				Status:           strPtr("success"),
				ResultCacheHit:   &resultCacheHit,
				StartTime:        &startTime,
				EndTime:          &endTime,
				ElapsedTime:      &elapsedTime,
				QueueTime:        &queueTime,
				ExecutionTime:    &executionTime,
				QueryText:        strPtr("SELECT * FROM users WHERE age > 25"),
				ReturnedRows:     &returnedRows,
				ReturnedBytes:    &returnedBytes,
				RedshiftVersion:  strPtr("1.0.50000"),
				ComputeType:      strPtr("standard"),
				CompileTime:      &compileTime,
				PlanningTime:     &planningTime,
				LockWaitTime:     &lockWaitTime,
				ServiceClassId:   &serviceClassId,
				ServiceClassName: strPtr("Default queue"),
				QueryPriority:    strPtr("NORMAL"),
				GenericQueryHash: strPtr("hash-123"),
				UserQueryHash:    strPtr("user-hash-456"),
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT * FROM users WHERE age > 25",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "successful_query_with_obfuscation",
			row: &RedshiftQueryLogSchema{
				UserId:           &userId,
				QueryId:          queryId,
				DatabaseName:     strPtr("logs"),
				QueryType:        strPtr("INSERT"),
				Status:           strPtr("success"),
				StartTime:        &startTime,
				EndTime:          &endTime,
				ElapsedTime:      &elapsedTime,
				ExecutionTime:    &executionTime,
				QueryText:        strPtr("INSERT INTO events VALUES ('error', 123, 'critical')"),
				GenericQueryHash: strPtr("hash-789"),
			},
			obfuscator:   obfuscatorRedact,
			expectedSQL:  "INSERT INTO events VALUES (?, ?, ?)",
			expectedMode: querylogs.ObfuscationRedactLiterals,
		},
		{
			name: "failed_query_with_error",
			row: &RedshiftQueryLogSchema{
				UserId:       &userId,
				QueryId:      queryId,
				DatabaseName: strPtr("analytics"),
				QueryType:    strPtr("SELECT"),
				Status:       strPtr("failed"),
				StartTime:    &startTime,
				EndTime:      &endTime,
				ElapsedTime:  int64Ptr(100),
				QueryText:    strPtr("SELECT * FROM nonexistent_table"),
				ErrorMessage: strPtr("Table 'nonexistent_table' not found"),
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT * FROM nonexistent_table",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "query_with_cache_hit",
			row: &RedshiftQueryLogSchema{
				UserId:         &userId,
				QueryId:        queryId,
				DatabaseName:   strPtr("analytics"),
				QueryType:      strPtr("SELECT"),
				Status:         strPtr("success"),
				ResultCacheHit: boolPtr(true),
				StartTime:      &startTime,
				EndTime:        &endTime,
				ElapsedTime:    int64Ptr(10),
				ExecutionTime:  int64Ptr(5),
				QueryText:      strPtr("SELECT COUNT(*) FROM users"),
				ReturnedRows:   int64Ptr(1),
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT COUNT(*) FROM users",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "query_with_short_query_acceleration",
			row: &RedshiftQueryLogSchema{
				UserId:                &userId,
				QueryId:               queryId,
				DatabaseName:          strPtr("analytics"),
				QueryType:             strPtr("SELECT"),
				Status:                strPtr("success"),
				StartTime:             &startTime,
				EndTime:               &endTime,
				ElapsedTime:           int64Ptr(50),
				ExecutionTime:         int64Ptr(45),
				QueryText:             strPtr("SELECT version()"),
				ShortQueryAccelerated: boolPtr(true),
				ReturnedRows:          int64Ptr(1),
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT version()",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "query_with_transaction_and_session",
			row: &RedshiftQueryLogSchema{
				UserId:        &userId,
				QueryId:       queryId,
				TransactionId: &transactionId,
				SessionId:     &sessionId,
				DatabaseName:  strPtr("analytics"),
				QueryType:     strPtr("UPDATE"),
				Status:        strPtr("success"),
				StartTime:     &startTime,
				EndTime:       &endTime,
				ElapsedTime:   &elapsedTime,
				ExecutionTime: &executionTime,
				QueryText:     strPtr("UPDATE users SET status = 'active' WHERE id = 100"),
			},
			obfuscator:   obfuscator,
			expectedSQL:  "UPDATE users SET status = 'active' WHERE id = 100",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "query_without_start_time",
			row: &RedshiftQueryLogSchema{
				UserId:        &userId,
				QueryId:       queryId,
				DatabaseName:  strPtr("analytics"),
				QueryType:     strPtr("SELECT"),
				Status:        strPtr("success"),
				StartTime:     nil, // No start time
				EndTime:       &endTime,
				ElapsedTime:   int64Ptr(100),
				ExecutionTime: int64Ptr(95),
				QueryText:     strPtr("SELECT * FROM table1"),
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT * FROM table1",
			expectedMode: querylogs.ObfuscationNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log, err := convertRedshiftRowToQueryLog(tt.row, tt.obfuscator, "redshift")

			if tt.expectedError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, log)

			// Verify basic fields - CreatedAt should use EndTime (when query finished)
			// Fallback order: EndTime → StartTime
			if tt.row.EndTime != nil {
				require.Equal(t, *tt.row.EndTime, log.CreatedAt)
			} else if tt.row.StartTime != nil {
				require.Equal(t, *tt.row.StartTime, log.CreatedAt)
			}

			// Verify timing fields
			require.Equal(t, tt.row.StartTime, log.StartedAt)
			require.Equal(t, tt.row.EndTime, log.FinishedAt)

			// QueryID should be generic hash if available, otherwise string of query_id
			if tt.row.GenericQueryHash != nil && *tt.row.GenericQueryHash != "" {
				require.Equal(t, *tt.row.GenericQueryHash, log.QueryID)
			} else {
				require.Contains(t, log.QueryID, "12345")
			}

			require.Equal(t, tt.expectedSQL, log.SQL)
			require.Equal(t, "redshift", log.SqlDialect)
			require.Equal(t, tt.expectedMode, log.SqlObfuscationMode)

			if tt.row.QueryType != nil {
				require.Equal(t, *tt.row.QueryType, log.QueryType)
			}

			// Verify status (converted to uppercase)
			if tt.row.Status != nil {
				require.Equal(t, strings.ToUpper(*tt.row.Status), log.Status)
			}

			// Verify DwhContext
			require.NotNil(t, log.DwhContext)
			if tt.row.DatabaseName != nil {
				require.Equal(t, *tt.row.DatabaseName, log.DwhContext.Database)
			}

			// Redshift doesn't provide native lineage
			require.False(t, log.HasCompleteNativeLineage)
			require.Nil(t, log.NativeLineage)

			// Verify metadata contains expected fields
			require.NotNil(t, log.Metadata)
			if tt.row.UserId != nil {
				require.Contains(t, log.Metadata, "user_id")
			}
			if tt.row.ElapsedTime != nil {
				require.Contains(t, log.Metadata, "elapsed_time")
			}
			if tt.row.ExecutionTime != nil {
				require.Contains(t, log.Metadata, "execution_time")
			}
			if tt.row.QueueTime != nil {
				require.Contains(t, log.Metadata, "queue_time")
			}
			if tt.row.ResultCacheHit != nil {
				require.Contains(t, log.Metadata, "result_cache_hit")
			}
			if tt.row.ShortQueryAccelerated != nil {
				require.Contains(t, log.Metadata, "short_query_accelerated")
			}
			if tt.row.ErrorMessage != nil {
				require.Contains(t, log.Metadata, "error_message")
				require.Equal(t, *tt.row.ErrorMessage, log.Metadata["error_message"])
			}
			if tt.row.GenericQueryHash != nil {
				require.Contains(t, log.Metadata, "generic_query_hash")
			}
			if tt.row.TransactionId != nil {
				require.Contains(t, log.Metadata, "transaction_id")
			}
			if tt.row.SessionId != nil {
				require.Contains(t, log.Metadata, "session_id")
			}
		})
	}
}

// Helper functions for pointer creation
func strPtr(s string) *string {
	return &s
}

func int64Ptr(i int64) *int64 {
	return &i
}

func boolPtr(b bool) *bool {
	return &b
}
