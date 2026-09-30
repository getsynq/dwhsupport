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
				ShortQueryAccelerated: strPtr("t"),
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
		{
			name: "query_with_trailing_whitespace_in_char_fields",
			row: &RedshiftQueryLogSchema{
				UserId:                &userId,
				QueryId:               queryId,
				DatabaseName:          strPtr("analytics                "), // CHAR field with padding
				QueryType:             strPtr("COPY      "),                // CHAR field with padding
				Status:                strPtr("SUCCESS   "),                // CHAR field with padding
				StartTime:             &startTime,
				EndTime:               &endTime,
				ElapsedTime:           &elapsedTime,
				ExecutionTime:         &executionTime,
				QueryText:             strPtr("COPY table FROM 's3://bucket/data'"),
				RedshiftVersion:       strPtr("1.0.162991                      "),         // CHAR field with padding
				ComputeType:           strPtr("primary          "),                        // CHAR field with padding
				ServiceClassName:      strPtr("priority_0                              "), // CHAR field with padding
				QueryPriority:         strPtr("Highest             "),                     // CHAR field with padding
				ShortQueryAccelerated: strPtr("false     "),                               // CHAR field with padding
				GenericQueryHash:      strPtr("8t9wfhBxtpU=                            "), // CHAR field with padding
				UserQueryHash:         strPtr("8t9wfhBxtpU=                            "), // CHAR field with padding
				QueryLabel:            strPtr("default         "),                         // CHAR field with padding
				ErrorMessage:          strPtr(""),                                         // Empty error message
			},
			obfuscator:   obfuscator,
			expectedSQL:  "COPY table FROM 's3://bucket/data'",
			expectedMode: querylogs.ObfuscationNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log, err := convertRedshiftRowToQueryLog(tt.row, nil, tt.obfuscator, "redshift", "test-host.redshift.amazonaws.com", "test_database")

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

			// QueryID is the run, the generic hash (trimmed) is the normalized hash
			require.Equal(t, "12345", log.QueryID)
			if tt.row.GenericQueryHash != nil && strings.TrimSpace(*tt.row.GenericQueryHash) != "" {
				require.Equal(t, strings.TrimSpace(*tt.row.GenericQueryHash), *log.NormalizedQueryHash)
			} else {
				require.Nil(t, log.NormalizedQueryHash)
			}

			require.Equal(t, tt.expectedSQL, log.SQL)
			require.Equal(t, "redshift", log.SqlDialect)
			require.Equal(t, tt.expectedMode, log.SqlObfuscationMode)

			if tt.row.QueryType != nil {
				require.Equal(t, strings.TrimSpace(*tt.row.QueryType), log.QueryType)
			}

			// Verify status (converted to uppercase and trimmed)
			if tt.row.Status != nil {
				require.Equal(t, strings.ToUpper(strings.TrimSpace(*tt.row.Status)), log.Status)
			}

			// Verify DwhContext
			require.NotNil(t, log.DwhContext)
			if tt.row.DatabaseName != nil {
				require.Equal(t, strings.TrimSpace(*tt.row.DatabaseName), log.DwhContext.Database)
			}

			// Redshift doesn't provide native lineage
			require.False(t, log.HasCompleteNativeLineage)
			require.Nil(t, log.NativeLineage)

			// Verify metadata contains expected fields
			require.NotNil(t, log.Metadata)
			fields := log.Metadata.GetFields()
			if tt.row.UserId != nil {
				require.Contains(t, fields, "user_id")
			}
			if tt.row.ElapsedTime != nil {
				require.Contains(t, fields, "elapsed_time")
			}
			if tt.row.ExecutionTime != nil {
				require.Contains(t, fields, "execution_time")
			}
			if tt.row.QueueTime != nil {
				require.Contains(t, fields, "queue_time")
			}
			if tt.row.ResultCacheHit != nil {
				require.Contains(t, fields, "result_cache_hit")
			}
			if tt.row.ShortQueryAccelerated != nil {
				require.Contains(t, fields, "short_query_accelerated")
			}
			// Error message is only included if non-empty (empty strings are filtered out by TrimmedStringPtrValue)
			if tt.row.ErrorMessage != nil && strings.TrimSpace(*tt.row.ErrorMessage) != "" {
				require.Contains(t, fields, "error_message")
				require.Equal(t, strings.TrimSpace(*tt.row.ErrorMessage), fields["error_message"].GetStringValue())
			}
			if tt.row.GenericQueryHash != nil {
				require.Contains(t, fields, "generic_query_hash")
			}
			if tt.row.TransactionId != nil {
				require.Contains(t, fields, "transaction_id")
			}
			if tt.row.SessionId != nil {
				require.Contains(t, fields, "session_id")
			}

			// Additional assertions for whitespace trimming test
			if tt.name == "query_with_trailing_whitespace_in_char_fields" {
				// Verify that metadata string fields are trimmed
				require.Equal(t, "analytics", fields["database_name"].GetStringValue())
				require.Equal(t, "COPY", fields["query_type"].GetStringValue())
				require.Equal(t, "SUCCESS", fields["status"].GetStringValue())
				require.Equal(t, "1.0.162991", fields["redshift_version"].GetStringValue())
				require.Equal(t, "primary", fields["compute_type"].GetStringValue())
				require.Equal(t, "priority_0", fields["service_class_name"].GetStringValue())
				require.Equal(t, "Highest", fields["query_priority"].GetStringValue())
				require.Equal(t, "false", fields["short_query_accelerated"].GetStringValue())
				require.Equal(t, "8t9wfhBxtpU=", fields["generic_query_hash"].GetStringValue())
				require.Equal(t, "8t9wfhBxtpU=", fields["user_query_hash"].GetStringValue())
				require.Equal(t, "default", fields["query_label"].GetStringValue())

				// Verify the normalized hash is trimmed
				require.Equal(t, "8t9wfhBxtpU=", *log.NormalizedQueryHash)

				// Verify QueryType is trimmed
				require.Equal(t, "COPY", log.QueryType)

				// Verify Status is uppercase and trimmed
				require.Equal(t, "SUCCESS", log.Status)

				// Verify DwhContext.Database is trimmed
				require.Equal(t, "analytics", log.DwhContext.Database)
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

// SYS_QUERY_HISTORY stores query_text C-escaped: a line break is the two characters `\n`, a carriage
// return `\r`, and a backslash is doubled. A `--` comment then swallows the rest of the statement.
func TestConvertRedshiftRowUnescapesQueryText(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	endTime := time.Date(2025, 11, 1, 10, 35, 0, 0, time.UTC)
	row := &RedshiftQueryLogSchema{
		QueryId:   42,
		EndTime:   &endTime,
		QueryText: strPtr(`select 1 as a, -- comment\n  'back\\slash' as b,\r\n  'lit\\nchars' as c`),
	}

	log, err := convertRedshiftRowToQueryLog(row, nil, obfuscator, "redshift", "host", "db")
	require.NoError(t, err)
	require.Equal(t, "select 1 as a, -- comment\n  'back\\slash' as b,\r\n  'lit\\nchars' as c", log.SQL)
}

// query_text holds the start of a statement, cut short of 4000 bytes. A statement cut there is not
// SQL, and the processor only skips it when IsTruncated says so.
func TestConvertRedshiftRowFlagsTextCutAtTheCap(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	endTime := time.Date(2025, 11, 1, 10, 35, 0, 0, time.UTC)
	text := "select '" + strings.Repeat("x", 4000-len("select '"))
	row := &RedshiftQueryLogSchema{QueryId: 42, EndTime: &endTime, QueryText: &text}

	log, err := convertRedshiftRowToQueryLog(row, nil, obfuscator, "redshift", "host", "db")
	require.NoError(t, err)
	require.True(t, log.IsTruncated)
}

// generic_query_hash is the same for every run of a statement shape; the run is query_id.
func TestConvertRedshiftRowIdentifiesTheRun(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	endTime := time.Date(2025, 11, 1, 10, 35, 0, 0, time.UTC)
	row := &RedshiftQueryLogSchema{
		QueryId:          1750077804,
		EndTime:          &endTime,
		QueryText:        strPtr("select 1 where 5 = 5"),
		GenericQueryHash: strPtr("v2DdY0D8q/k=                            "),
	}

	log, err := convertRedshiftRowToQueryLog(row, nil, obfuscator, "redshift", "host", "db")
	require.NoError(t, err)
	require.Equal(t, "1750077804", log.QueryID)
	require.NotNil(t, log.NormalizedQueryHash)
	require.Equal(t, "v2DdY0D8q/k=", *log.NormalizedQueryHash)
}
