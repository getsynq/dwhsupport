package bigquery

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/require"
)

func TestConvertBigQueryRowToQueryLog_Destination(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	at := time.Date(2026, 10, 9, 10, 0, 0, 0, time.UTC)
	table := func(dataset, table string) *BqQueryTable {
		return &BqQueryTable{
			ProjectId: bigquery.NullString{StringVal: "my-project", Valid: true},
			DatasetId: bigquery.NullString{StringVal: dataset, Valid: true},
			TableId:   bigquery.NullString{StringVal: table, Valid: true},
		}
	}
	users := table("analytics", "users")

	tests := []struct {
		name          string
		statementType string
		destination   *BqQueryTable
		// The output tables expected, nil for none.
		outputs []scrapper.DwhFqn
	}{
		{
			// A job that names no destination puts its result in an anonymous table, which it
			// reports as its destination. Nothing ever writes a table there.
			name:          "anonymous_result_table",
			statementType: "SELECT",
			destination:   table("_0123456789abcdef0123456789abcdef01234567", "anon0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0"),
		},
		{
			name:          "anonymous_result_table_with_dashed_id",
			statementType: "SELECT",
			destination:   table("_fedcba9876543210fedcba9876543210fedcba98", "anon12345678_9abc_4def_8123_456789abcdef"),
		},
		{
			// A SELECT given a destination table, as a scheduled query is, writes it.
			name:          "select_into_configured_destination",
			statementType: "SELECT",
			destination:   table("reporting", "daily_users"),
			outputs:       []scrapper.DwhFqn{{DatabaseName: "my-project", SchemaName: "reporting", ObjectName: "daily_users"}},
		},
		{
			// A customer's own dataset may start with an underscore too.
			name:          "customer_dataset_with_leading_underscore",
			statementType: "CREATE_VIEW",
			destination:   table("_staging", "events"),
			outputs:       []scrapper.DwhFqn{{DatabaseName: "my-project", SchemaName: "_staging", ObjectName: "events"}},
		},
		{
			// A script's temporary table is a real table the script reads again.
			name:          "script_temporary_table",
			statementType: "CREATE_TABLE_AS_SELECT",
			destination:   table("_script00112233445566778899aabbccddeeff00112233", "batch"),
			outputs: []scrapper.DwhFqn{
				{DatabaseName: "my-project", SchemaName: "_script00112233445566778899aabbccddeeff00112233", ObjectName: "batch"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log, err := convertBigQueryRowToQueryLog(&BigQueryQueryLogSchema{
				CreationTime:     at,
				ProjectId:        bigquery.NullString{StringVal: "my-project", Valid: true},
				JobId:            bigquery.NullString{StringVal: "job-1", Valid: true},
				JobType:          bigquery.NullString{StringVal: "QUERY", Valid: true},
				StatementType:    bigquery.NullString{StringVal: tt.statementType, Valid: true},
				StartTime:        at,
				EndTime:          at.Add(time.Second),
				Query:            bigquery.NullString{StringVal: "SELECT * FROM analytics.users", Valid: true},
				State:            bigquery.NullString{StringVal: "DONE", Valid: true},
				ReferencedTables: []*BqQueryTable{users},
				DestinationTable: tt.destination,
			}, obfuscator, "bigquery")
			require.NoError(t, err)
			require.NotNil(t, log.NativeLineage)
			require.Equal(t, []scrapper.DwhFqn{{DatabaseName: "my-project", SchemaName: "analytics", ObjectName: "users"}}, log.NativeLineage.InputTables)
			require.Equal(t, tt.outputs, log.NativeLineage.OutputTables)
			require.Equal(t, len(tt.outputs) > 0, log.HasCompleteNativeLineage)
		})
	}
}
