package databricks

import (
	"strings"
	"testing"
	"time"

	servicesql "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

func TestConvertDatabricksQueryInfoToQueryLog(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	obfuscatorRedact, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationRedactLiterals)
	require.NoError(t, err)

	startTime := time.Date(2025, 11, 1, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name          string
		queryInfo     *servicesql.QueryInfo
		obfuscator    querylogs.QueryObfuscator
		expectedSQL   string
		expectedMode  querylogs.ObfuscationMode
		expectNil     bool // For queries that should be skipped
		expectedError bool
	}{
		{
			name: "successful_select_query",
			queryInfo: &servicesql.QueryInfo{
				QueryId:            "query-123",
				QueryText:          "SELECT * FROM users WHERE age > 25",
				QueryStartTimeMs:   startTime.UnixMilli(),
				QueryEndTimeMs:     startTime.Add(5 * time.Second).UnixMilli(),
				Status:             servicesql.QueryStatusFinished,
				StatementType:      servicesql.QueryStatementTypeSelect,
				UserName:           "admin@example.com",
				ExecutedAsUserId:   123,
				ExecutedAsUserName: "Admin User",
				EndpointId:         "endpoint-1",
				WarehouseId:        "warehouse-1",
				Duration:           5000,
				ExecutionEndTimeMs: startTime.Add(5 * time.Second).UnixMilli(),
				RowsProduced:       100,
				IsFinal:            true,
				PlansState:         servicesql.PlansStateExists,
				Metrics: &servicesql.QueryMetrics{
					ExecutionTimeMs: 4500,
					ReadBytes:       1024000,
					RowsReadCount:   1000,
				},
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT * FROM users WHERE age > 25",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "successful_query_with_obfuscation",
			queryInfo: &servicesql.QueryInfo{
				QueryId:          "query-456",
				QueryText:        "INSERT INTO logs VALUES ('error', 123, 'critical')",
				QueryStartTimeMs: startTime.UnixMilli(),
				QueryEndTimeMs:   startTime.Add(2 * time.Second).UnixMilli(),
				Status:           servicesql.QueryStatusFinished,
				StatementType:    servicesql.QueryStatementTypeOther,
				UserName:         "service@example.com",
				Duration:         2000,
				IsFinal:          true,
				PlansState:       servicesql.PlansStateExists,
			},
			obfuscator:   obfuscatorRedact,
			expectedSQL:  "INSERT INTO logs VALUES (?, ?, ?)",
			expectedMode: querylogs.ObfuscationRedactLiterals,
		},
		{
			name: "failed_query",
			queryInfo: &servicesql.QueryInfo{
				QueryId:          "query-789",
				QueryText:        "SELECT * FROM nonexistent_table",
				QueryStartTimeMs: startTime.UnixMilli(),
				QueryEndTimeMs:   startTime.Add(1 * time.Second).UnixMilli(),
				Status:           servicesql.QueryStatusFailed,
				StatementType:    servicesql.QueryStatementTypeSelect,
				UserName:         "admin@example.com",
				ErrorMessage:     "Table 'nonexistent_table' not found",
				Duration:         1000,
				IsFinal:          true,
				PlansState:       servicesql.PlansStateExists,
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT * FROM nonexistent_table",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "canceled_query",
			queryInfo: &servicesql.QueryInfo{
				QueryId:          "query-999",
				QueryText:        "SELECT * FROM large_table",
				QueryStartTimeMs: startTime.UnixMilli(),
				QueryEndTimeMs:   startTime.Add(10 * time.Second).UnixMilli(),
				Status:           servicesql.QueryStatusCanceled,
				StatementType:    servicesql.QueryStatementTypeSelect,
				UserName:         "analyst@example.com",
				Duration:         10000,
				IsFinal:          true,
				PlansState:       servicesql.PlansStateExists,
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT * FROM large_table",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "show_statement_should_be_skipped",
			queryInfo: &servicesql.QueryInfo{
				QueryId:          "query-show",
				QueryText:        "SHOW TABLES",
				QueryStartTimeMs: startTime.UnixMilli(),
				QueryEndTimeMs:   startTime.Add(1 * time.Second).UnixMilli(),
				Status:           servicesql.QueryStatusFinished,
				StatementType:    servicesql.QueryStatementTypeShow,
				UserName:         "admin@example.com",
				IsFinal:          true,
			},
			obfuscator: obfuscator,
			expectNil:  true, // Should return nil (skip this query)
		},
		{
			name: "use_statement_should_be_skipped",
			queryInfo: &servicesql.QueryInfo{
				QueryId:          "query-use",
				QueryText:        "USE DATABASE analytics",
				QueryStartTimeMs: startTime.UnixMilli(),
				QueryEndTimeMs:   startTime.Add(1 * time.Second).UnixMilli(),
				Status:           servicesql.QueryStatusFinished,
				StatementType:    servicesql.QueryStatementTypeUse,
				UserName:         "admin@example.com",
				IsFinal:          true,
			},
			obfuscator: obfuscator,
			expectNil:  true, // Should return nil (skip this query)
		},
		{
			name: "insert_statement_sql_should_be_empty",
			queryInfo: &servicesql.QueryInfo{
				QueryId:          "query-insert",
				QueryText:        strings.Repeat("INSERT INTO table VALUES ", 50000), // Large INSERT
				QueryStartTimeMs: startTime.UnixMilli(),
				QueryEndTimeMs:   startTime.Add(5 * time.Second).UnixMilli(),
				Status:           servicesql.QueryStatusFinished,
				StatementType:    servicesql.QueryStatementTypeInsert,
				UserName:         "etl@example.com",
				Duration:         5000,
				IsFinal:          true,
			},
			obfuscator:   obfuscator,
			expectedSQL:  "", // INSERT queries are not included (can be huge)
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "query_with_metrics",
			queryInfo: &servicesql.QueryInfo{
				QueryId:            "query-metrics",
				QueryText:          "SELECT COUNT(*) FROM orders",
				QueryStartTimeMs:   startTime.UnixMilli(),
				QueryEndTimeMs:     startTime.Add(3 * time.Second).UnixMilli(),
				Status:             servicesql.QueryStatusFinished,
				StatementType:      servicesql.QueryStatementTypeSelect,
				UserName:           "analyst@example.com",
				Duration:           3000,
				ExecutionEndTimeMs: startTime.Add(3 * time.Second).UnixMilli(),
				RowsProduced:       1,
				IsFinal:            true,
				PlansState:         servicesql.PlansStateExists,
				Metrics: &servicesql.QueryMetrics{
					CompilationTimeMs:   100,
					ExecutionTimeMs:     2800,
					NetworkSentBytes:    1024,
					PhotonTotalTimeMs:   2500,
					ReadBytes:           5000000,
					ReadCacheBytes:      1000000,
					ReadFilesCount:      10,
					ReadPartitionsCount: 5,
					ReadRemoteBytes:     4000000,
					ResultFetchTimeMs:   100,
					ResultFromCache:     false,
					RowsProducedCount:   1,
					RowsReadCount:       10000,
					SpillToDiskBytes:    0,
					TaskTotalTimeMs:     2800,
					TotalTimeMs:         3000,
					WriteRemoteBytes:    0,
					PrunedBytes:         1000000,
					PrunedFilesCount:    2,
				},
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT COUNT(*) FROM orders",
			expectedMode: querylogs.ObfuscationNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log, err := convertDatabricksQueryInfoToQueryLog(tt.queryInfo, tt.obfuscator, "databricks", "https://test.cloud.databricks.com")

			if tt.expectedError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.expectNil {
				require.Nil(t, log, "Expected nil for skipped query")
				return
			}

			require.NotNil(t, log)

			// Verify basic fields - CreatedAt should use QueryEndTimeMs (when query finished)
			require.Equal(t, time.UnixMilli(int64(tt.queryInfo.QueryEndTimeMs)), log.CreatedAt)

			// Verify timing fields
			expectedStartedAt := time.UnixMilli(int64(tt.queryInfo.QueryStartTimeMs))
			expectedFinishedAt := time.UnixMilli(int64(tt.queryInfo.QueryEndTimeMs))
			require.NotNil(t, log.StartedAt)
			require.Equal(t, expectedStartedAt, *log.StartedAt)
			require.NotNil(t, log.FinishedAt)
			require.Equal(t, expectedFinishedAt, *log.FinishedAt)

			require.Equal(t, tt.queryInfo.QueryId, log.QueryID)
			require.Equal(t, tt.expectedSQL, log.SQL)
			require.Equal(t, "databricks", log.SqlDialect)
			require.Equal(t, tt.expectedMode, log.SqlObfuscationMode)
			require.Equal(t, tt.queryInfo.StatementType.String(), log.QueryType)

			// Verify status
			switch tt.queryInfo.Status {
			case servicesql.QueryStatusFinished:
				require.Equal(t, "SUCCESS", log.Status)
			case servicesql.QueryStatusCanceled:
				require.Equal(t, "CANCELED", log.Status)
			case servicesql.QueryStatusFailed:
				require.Equal(t, "FAILED", log.Status)
			}

			// Verify DwhContext
			require.NotNil(t, log.DwhContext)
			require.Equal(t, tt.queryInfo.UserName, log.DwhContext.User)

			// Databricks doesn't provide complete native lineage
			require.False(t, log.HasCompleteNativeLineage)
			require.Nil(t, log.NativeLineage)

			// Verify metadata contains expected fields
			require.NotNil(t, log.Metadata)
			require.Contains(t, log.Metadata, "endpoint_id")
			require.Contains(t, log.Metadata, "warehouse_id")
			require.Contains(t, log.Metadata, "executed_as_user_name")
			require.Contains(t, log.Metadata, "statement_type")
			require.Contains(t, log.Metadata, "is_final")

			// Verify error message when present
			if tt.queryInfo.ErrorMessage != "" {
				require.Contains(t, log.Metadata, "error_message")
				require.Equal(t, tt.queryInfo.ErrorMessage, log.Metadata["error_message"])
			}

			// Verify metrics when present
			if tt.queryInfo.Metrics != nil {
				require.Contains(t, log.Metadata, "metrics")
				metrics := log.Metadata["metrics"].(map[string]any)
				if tt.queryInfo.Metrics.ExecutionTimeMs != 0 {
					require.Contains(t, metrics, "execution_time_ms")
				}
				if tt.queryInfo.Metrics.ReadBytes != 0 {
					require.Contains(t, metrics, "read_bytes")
				}
			}

			// Verify duration when present
			if tt.queryInfo.Duration != 0 {
				require.Contains(t, log.Metadata, "duration_ms")
			}
		})
	}
}
