package trino

import (
	"database/sql"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
	"github.com/trinodb/trino-go-client/trino"
)

// Helper to create trino.NullSliceString from strings
func nullSliceString(vals ...string) trino.NullSliceString {
	slice := make([]sql.NullString, len(vals))
	for i, v := range vals {
		slice[i] = sql.NullString{String: v, Valid: true}
	}
	return trino.NullSliceString{SliceString: slice}
}

func TestConvertTrinoRowToQueryLog(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	obfuscatorRedact, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationRedactLiterals)
	require.NoError(t, err)

	createdTime := time.Date(2025, 11, 1, 10, 30, 0, 0, time.UTC)
	startedTime := time.Date(2025, 11, 1, 10, 30, 1, 0, time.UTC)
	endTime := time.Date(2025, 11, 1, 10, 35, 0, 0, time.UTC)
	heartbeatTime := time.Date(2025, 11, 1, 10, 34, 0, 0, time.UTC)

	tests := []struct {
		name          string
		row           *TrinoQueryLogSchema
		obfuscator    querylogs.QueryObfuscator
		expectedSQL   string
		expectedMode  querylogs.ObfuscationMode
		expectedError bool
	}{
		{
			name: "successful_finished_query",
			row: &TrinoQueryLogSchema{
				QueryId:         "20251101_103000_12345_query",
				State:           strPtr("FINISHED"),
				User:            strPtr("admin"),
				Source:          strPtr("trino-cli"),
				Query:           strPtr("SELECT * FROM users WHERE age > 25"),
				ResourceGroupId: nullSliceString("global"),
				QueuedTimeMs:    int64Ptr(100),
				AnalysisTimeMs:  int64Ptr(50),
				PlanningTimeMs:  int64Ptr(200),
				Created:         &createdTime,
				Started:         &startedTime,
				LastHeartbeat:   &heartbeatTime,
				End:             &endTime,
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT * FROM users WHERE age > 25",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "successful_query_with_obfuscation",
			row: &TrinoQueryLogSchema{
				QueryId:        "20251101_103100_12346_query",
				State:          strPtr("FINISHED"),
				User:           strPtr("service"),
				Source:         strPtr("jdbc"),
				Query:          strPtr("INSERT INTO events VALUES ('error', 123, 'critical')"),
				QueuedTimeMs:   int64Ptr(50),
				AnalysisTimeMs: int64Ptr(30),
				PlanningTimeMs: int64Ptr(100),
				Created:        &createdTime,
				Started:        &startedTime,
				End:            &endTime,
			},
			obfuscator:   obfuscatorRedact,
			expectedSQL:  "INSERT INTO events VALUES (?, ?, ?)",
			expectedMode: querylogs.ObfuscationRedactLiterals,
		},
		{
			name: "failed_query_with_error",
			row: &TrinoQueryLogSchema{
				QueryId:        "20251101_103200_12347_query",
				State:          strPtr("FAILED"),
				User:           strPtr("analyst"),
				Source:         strPtr("trino-cli"),
				Query:          strPtr("SELECT * FROM nonexistent_table"),
				QueuedTimeMs:   int64Ptr(10),
				AnalysisTimeMs: int64Ptr(5),
				Created:        &createdTime,
				Started:        &startedTime,
				End:            &endTime,
				ErrorType:      strPtr("USER_ERROR"),
				ErrorCode:      strPtr("SYNTAX_ERROR"),
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT * FROM nonexistent_table",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "running_query",
			row: &TrinoQueryLogSchema{
				QueryId:         "20251101_103300_12348_query",
				State:           strPtr("RUNNING"),
				User:            strPtr("admin"),
				Source:          strPtr("trino-cli"),
				Query:           strPtr("SELECT COUNT(*) FROM large_table"),
				ResourceGroupId: nullSliceString("global"),
				QueuedTimeMs:    int64Ptr(200),
				AnalysisTimeMs:  int64Ptr(100),
				PlanningTimeMs:  int64Ptr(300),
				Created:         &createdTime,
				Started:         &startedTime,
				LastHeartbeat:   &heartbeatTime,
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT COUNT(*) FROM large_table",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "queued_query",
			row: &TrinoQueryLogSchema{
				QueryId:         "20251101_103400_12349_query",
				State:           strPtr("QUEUED"),
				User:            strPtr("user1"),
				Source:          strPtr("jdbc"),
				Query:           strPtr("SELECT * FROM orders"),
				ResourceGroupId: nullSliceString("limited"),
				QueuedTimeMs:    int64Ptr(5000),
				Created:         &createdTime,
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT * FROM orders",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "query_with_error_but_finished_state",
			row: &TrinoQueryLogSchema{
				QueryId:        "20251101_103500_12350_query",
				State:          strPtr("FINISHED"),
				User:           strPtr("admin"),
				Source:         strPtr("trino-cli"),
				Query:          strPtr("SELECT * FROM table1"),
				QueuedTimeMs:   int64Ptr(50),
				AnalysisTimeMs: int64Ptr(30),
				Created:        &createdTime,
				Started:        &startedTime,
				End:            &endTime,
				ErrorType:      strPtr("INTERNAL_ERROR"), // Has error, should override state
				ErrorCode:      strPtr("GENERIC_INTERNAL_ERROR"),
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT * FROM table1",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "query_without_created_time",
			row: &TrinoQueryLogSchema{
				QueryId:        "20251101_103600_12351_query",
				State:          strPtr("FINISHED"),
				User:           strPtr("admin"),
				Query:          strPtr("SELECT version()"),
				Created:        nil, // No created time
				Started:        &startedTime,
				End:            &endTime,
				QueuedTimeMs:   int64Ptr(0),
				AnalysisTimeMs: int64Ptr(10),
				PlanningTimeMs: int64Ptr(20),
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT version()",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "query_with_planning_state",
			row: &TrinoQueryLogSchema{
				QueryId:        "20251101_103700_12352_query",
				State:          strPtr("PLANNING"),
				User:           strPtr("user2"),
				Source:         strPtr("trino-cli"),
				Query:          strPtr("SELECT * FROM complex_join"),
				QueuedTimeMs:   int64Ptr(100),
				AnalysisTimeMs: int64Ptr(200),
				PlanningTimeMs: int64Ptr(1000),
				Created:        &createdTime,
				Started:        &startedTime,
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT * FROM complex_join",
			expectedMode: querylogs.ObfuscationNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log, err := convertTrinoRowToQueryLog(tt.row, tt.obfuscator, "trino", "test-trino-host.example.com")

			if tt.expectedError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, log)

			// Verify basic fields - CreatedAt should use End time (when query finished)
			// Fallback order: End → Started → Created
			if tt.row.End != nil {
				require.Equal(t, *tt.row.End, log.CreatedAt)
			} else if tt.row.Started != nil {
				require.Equal(t, *tt.row.Started, log.CreatedAt)
			} else if tt.row.Created != nil {
				require.Equal(t, *tt.row.Created, log.CreatedAt)
			}

			// Verify timing fields
			require.Equal(t, tt.row.Started, log.StartedAt)
			require.Equal(t, tt.row.End, log.FinishedAt)

			require.Equal(t, tt.row.QueryId, log.QueryID)
			require.Equal(t, tt.expectedSQL, log.SQL)
			require.Equal(t, "trino", log.SqlDialect)
			require.Equal(t, tt.expectedMode, log.SqlObfuscationMode)
			require.Empty(t, log.QueryType) // Trino doesn't provide query type

			// Verify status
			if tt.row.State != nil {
				state := *tt.row.State
				// If there's an error, status should be FAILED
				if tt.row.ErrorType != nil || tt.row.ErrorCode != nil {
					require.Equal(t, "FAILED", log.Status)
				} else {
					switch state {
					case "FINISHED":
						require.Equal(t, "SUCCESS", log.Status)
					case "FAILED":
						require.Equal(t, "FAILED", log.Status)
					case "RUNNING", "QUEUED", "PLANNING", "STARTING":
						require.Equal(t, "RUNNING", log.Status)
					}
				}
			}

			// Verify DwhContext
			require.NotNil(t, log.DwhContext)
			if tt.row.User != nil {
				require.Equal(t, *tt.row.User, log.DwhContext.User)
			}

			// Trino doesn't provide native lineage
			require.False(t, log.HasCompleteNativeLineage)
			require.Nil(t, log.NativeLineage)

			// Verify metadata contains expected fields
			require.NotNil(t, log.Metadata)
			if tt.row.State != nil {
				require.Contains(t, log.Metadata, "state")
			}
			if tt.row.Source != nil {
				require.Contains(t, log.Metadata, "source")
			}
			if len(tt.row.ResourceGroupId.SliceString) > 0 {
				require.Contains(t, log.Metadata, "resource_group_id")
			}
			if tt.row.QueuedTimeMs != nil {
				require.Contains(t, log.Metadata, "queued_time_ms")
			}
			if tt.row.AnalysisTimeMs != nil {
				require.Contains(t, log.Metadata, "analysis_time_ms")
			}
			if tt.row.PlanningTimeMs != nil {
				require.Contains(t, log.Metadata, "planning_time_ms")
			}
			if tt.row.ErrorType != nil {
				require.Contains(t, log.Metadata, "error_type")
				require.Equal(t, *tt.row.ErrorType, log.Metadata["error_type"])
			}
			if tt.row.ErrorCode != nil {
				require.Contains(t, log.Metadata, "error_code")
				require.Equal(t, *tt.row.ErrorCode, log.Metadata["error_code"])
			}
			if tt.row.Started != nil {
				require.Contains(t, log.Metadata, "started")
			}
			if tt.row.LastHeartbeat != nil {
				require.Contains(t, log.Metadata, "last_heartbeat")
			}
			if tt.row.End != nil {
				require.Contains(t, log.Metadata, "end")
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
