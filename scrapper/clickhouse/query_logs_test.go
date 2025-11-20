package clickhouse

import (
	"net"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

func TestConvertClickhouseRowToQueryLog(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	obfuscatorRedact, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationRedactLiterals)
	require.NoError(t, err)

	startTime := time.Date(2025, 11, 1, 10, 30, 0, 0, time.UTC)
	ipAddr := net.ParseIP("192.168.1.100")

	tests := []struct {
		name          string
		row           *ClickhouseQueryLogSchema
		obfuscator    querylogs.QueryObfuscator
		expectedSQL   string
		expectedMode  querylogs.ObfuscationMode
		expectedError bool
	}{
		{
			name: "successful_query_with_lineage",
			row: &ClickhouseQueryLogSchema{
				QueryType:                  "QueryFinish",
				EventTimeMicroseconds:      startTime,
				QueryStartTimeMicroseconds: startTime,
				ReadRows:                   1000,
				ReadBytes:                  50000,
				WrittenRows:                500,
				WrittenBytes:               25000,
				ResultRows:                 100,
				ResultBytes:                5000,
				MemoryUsage:                1024000,
				CurrentDatabase:            "analytics",
				Query:                      "SELECT * FROM users WHERE age > 18",
				QueryKind:                  "Select",
				Databases:                  []string{"analytics"},
				Tables:                     []string{"analytics.users"},
				Columns:                    []string{"age", "name"},
				Projections:                []string{},
				Views:                      []string{},
				ExceptionCode:              0,
				Exception:                  "",
				StackTrace:                 "",
				InitialUser:                "admin",
				InitialQueryId:             "query-123",
				InitialAddress:             &ipAddr,
				InitialPort:                9000,
				InitialQueryStartTimeMicroseconds: startTime,
				OsUser:                            "ubuntu",
				ClientHostname:                    "client-01",
				ClientName:                        "clickhouse-client",
				ClientRevision:                    54449,
				ClientRevisionMajor:               23,
				ClientRevisionMinor:               8,
				ClientRevisionPatch:               1,
				DistributedDepth:                  0,
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT * FROM users WHERE age > 18",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "successful_query_with_obfuscation",
			row: &ClickhouseQueryLogSchema{
				QueryType:                  "QueryFinish",
				EventTimeMicroseconds:      startTime,
				QueryStartTimeMicroseconds: startTime,
				ReadRows:                   0,
				ReadBytes:                  0,
				WrittenRows:                100,
				WrittenBytes:               10000,
				ResultRows:                 0,
				ResultBytes:                0,
				MemoryUsage:                512000,
				CurrentDatabase:            "logs",
				Query:                      "INSERT INTO events VALUES ('error', 123, 'critical')",
				QueryKind:                  "Insert",
				Databases:                  []string{"logs"},
				Tables:                     []string{"logs.events"},
				Columns:                    []string{"level", "code", "message"},
				InitialUser:                "service",
				InitialQueryId:             "query-456",
				InitialQueryStartTimeMicroseconds: startTime,
				ClientName:                        "python-client",
			},
			obfuscator:   obfuscatorRedact,
			expectedSQL:  "INSERT INTO events VALUES (?, ?, ?)",
			expectedMode: querylogs.ObfuscationRedactLiterals,
		},
		{
			name: "failed_query_with_exception",
			row: &ClickhouseQueryLogSchema{
				QueryType:                  "ExceptionWhileProcessing",
				EventTimeMicroseconds:      startTime,
				QueryStartTimeMicroseconds: startTime,
				ReadRows:                   0,
				ReadBytes:                  0,
				CurrentDatabase:            "analytics",
				Query:                      "SELECT * FROM nonexistent_table",
				QueryKind:                  "Select",
				Databases:                  []string{"analytics"},
				Tables:                     []string{"analytics.nonexistent_table"},
				ExceptionCode:              60,
				Exception:                  "Table analytics.nonexistent_table doesn't exist",
				StackTrace:                 "Stack trace...",
				InitialUser:                "admin",
				InitialQueryId:             "query-789",
				InitialQueryStartTimeMicroseconds: startTime,
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT * FROM nonexistent_table",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "query_with_temporary_table_filtered",
			row: &ClickhouseQueryLogSchema{
				QueryType:                  "QueryFinish",
				EventTimeMicroseconds:      startTime,
				QueryStartTimeMicroseconds: startTime,
				ReadRows:                   100,
				ReadBytes:                  10000,
				CurrentDatabase:            "default",
				Query:                      "SELECT * FROM temp_table",
				QueryKind:                  "Select",
				Databases:                  []string{"default"},
				Tables: []string{
					"default.users",
					"default.`.inner_id.e16b8c51-6afc-4d0f-877c-76e4f75b39d9", // Should be filtered
				},
				InitialUser:                       "admin",
				InitialQueryId:                    "query-999",
				InitialQueryStartTimeMicroseconds: startTime,
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT * FROM temp_table",
			expectedMode: querylogs.ObfuscationNone,
		},
		{
			name: "query_with_multiple_tables",
			row: &ClickhouseQueryLogSchema{
				QueryType:                  "QueryFinish",
				EventTimeMicroseconds:      startTime,
				QueryStartTimeMicroseconds: startTime,
				ReadRows:                   5000,
				ReadBytes:                  250000,
				CurrentDatabase:            "analytics",
				Query:                      "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
				QueryKind:                  "Select",
				Databases:                  []string{"analytics"},
				Tables: []string{
					"analytics.users",
					"analytics.orders",
				},
				Columns:                           []string{"name", "total", "id", "user_id"},
				InitialUser:                       "analyst",
				InitialQueryId:                    "query-111",
				InitialQueryStartTimeMicroseconds: startTime,
			},
			obfuscator:   obfuscator,
			expectedSQL:  "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
			expectedMode: querylogs.ObfuscationNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log, err := convertClickhouseRowToQueryLog(tt.row, tt.obfuscator, "clickhouse")

			if tt.expectedError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, log)

			// Verify basic fields
			require.Equal(t, tt.row.QueryStartTimeMicroseconds, log.CreatedAt)
			require.Equal(t, tt.row.InitialQueryId, log.QueryID)
			require.Equal(t, tt.expectedSQL, log.SQL)
			require.Equal(t, "clickhouse", log.SqlDialect)
			require.Equal(t, tt.expectedMode, log.SqlObfuscationMode)
			require.Equal(t, tt.row.QueryKind, log.QueryType)

			// Verify status
			if tt.row.Exception != "" {
				require.Equal(t, "FAILED", log.Status)
			} else {
				require.Equal(t, "SUCCESS", log.Status)
			}

			// Verify DwhContext
			require.NotNil(t, log.DwhContext)
			require.Equal(t, tt.row.CurrentDatabase, log.DwhContext.Schema)
			require.Equal(t, tt.row.InitialUser, log.DwhContext.User)
			require.Empty(t, log.DwhContext.Database) // ClickHouse doesn't have database concept

			// Verify lineage when tables present (excluding temporary tables)
			hasNonTempTables := false
			for _, table := range tt.row.Tables {
				if !containsString(table, "`.inner_id") {
					hasNonTempTables = true
					break
				}
			}
			if hasNonTempTables {
				require.NotNil(t, log.NativeLineage)
				require.NotEmpty(t, log.NativeLineage.InputTables)
				// Verify temp tables are filtered out
				for _, fqn := range log.NativeLineage.InputTables {
					require.NotContains(t, fqn.ObjectName, "`.inner_id")
				}
			}

			// ClickHouse only provides input tables, not complete lineage
			require.False(t, log.HasCompleteNativeLineage)

			// Verify metadata contains expected fields
			require.NotNil(t, log.Metadata)
			require.Contains(t, log.Metadata, "query_type")
			require.Contains(t, log.Metadata, "read_rows")
			require.Contains(t, log.Metadata, "read_bytes")
			require.Contains(t, log.Metadata, "memory_usage")

			// Verify exception metadata when present
			if tt.row.Exception != "" {
				require.Contains(t, log.Metadata, "exception_code")
				require.Contains(t, log.Metadata, "stack_trace")
			}

			// Verify IP address when present
			if tt.row.InitialAddress != nil {
				require.Contains(t, log.Metadata, "initial_address")
				require.Contains(t, log.Metadata, "initial_port")
			}
		})
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && s[:len(substr)] == substr || len(s) > len(substr) && containsString(s[1:], substr)
}
