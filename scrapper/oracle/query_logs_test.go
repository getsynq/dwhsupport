package oracle

import (
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

// TestConvertOracleRowNamesTheParsingUser converts a V$SQL row (anonymised): the
// user is the one who parsed the cursor, never the parsing schema, which a
// session can switch to another schema with ALTER SESSION SET CURRENT_SCHEMA.
func TestConvertOracleRowNamesTheParsingUser(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	lastActive := time.Date(2026, 10, 5, 7, 48, 27, 0, time.UTC)
	cases := []struct {
		name   string
		user   *string
		schema *string
		want   querylogs.DwhContext
	}{
		{
			name:   "user parses in its own schema",
			user:   ptr("REPORTING"),
			schema: ptr("REPORTING"),
			want:   querylogs.DwhContext{Instance: "db.example.internal", Database: "ORCLPDB1", Schema: "REPORTING", User: "REPORTING"},
		},
		{
			name:   "user switched to another schema",
			user:   ptr("ETL_LOADER"),
			schema: ptr("SALES"),
			want:   querylogs.DwhContext{Instance: "db.example.internal", Database: "ORCLPDB1", Schema: "SALES", User: "ETL_LOADER"},
		},
		{
			name:   "parsing user since dropped",
			user:   nil,
			schema: ptr("SALES"),
			want:   querylogs.DwhContext{Instance: "db.example.internal", Database: "ORCLPDB1", Schema: "SALES"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := &OracleQueryLogSchema{
				SqlId:             "f8tpvxx4yha7y",
				SqlFulltext:       ptr("SELECT COUNT(*) FROM sales.orders WHERE status = 'OPEN'"),
				ParsingSchemaName: tc.schema,
				ParsingUserName:   tc.user,
				LastActiveTime:    &lastActive,
				Executions:        ptr(int64(11)),
				ElapsedTime:       ptr(int64(1308471)),
				CpuTime:           ptr(int64(1296169)),
				DiskReads:         ptr(int64(1044)),
				BufferGets:        ptr(int64(180652)),
				RowsProcessed:     ptr(int64(176)),
				CommandType:       ptr(int64(3)),
				Module:            ptr("app"),
				OptimizerCost:     ptr(int64(416)),
				Fetches:           ptr(int64(11)),
				Sorts:             ptr(int64(11)),
			}
			log, err := convertOracleRowToQueryLog(row, obfuscator, "oracle", "db.example.internal", "ORCLPDB1")
			require.NoError(t, err)
			require.Equal(t, &tc.want, log.DwhContext)
			require.Equal(t, "SELECT", log.QueryType)
		})
	}
}

func ptr[T any](v T) *T { return &v }
