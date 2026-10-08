package bigquery

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

// INFORMATION_SCHEMA.JOBS leaves total_slot_ms, total_bytes_processed and reservation_id NULL on
// a job that reported none. NULL is not zero: a cached query reports a zero, a job that never
// reported a figure has none, and a reader adding the figures up must be able to tell them apart.
func TestConvertBigQueryRowLeavesNullFiguresOut(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	start := time.Date(2026, 10, 8, 14, 58, 2, 197_000_000, time.UTC)
	row := &BigQueryQueryLogSchema{
		CreationTime:  start,
		StartTime:     start,
		EndTime:       start.Add(690 * time.Millisecond),
		ProjectId:     bigquery.NullString{StringVal: "my-project", Valid: true},
		JobId:         bigquery.NullString{StringVal: "job-1", Valid: true},
		JobType:       bigquery.NullString{StringVal: "QUERY", Valid: true},
		StatementType: bigquery.NullString{StringVal: "SELECT", Valid: true},
		State:         bigquery.NullString{StringVal: "DONE", Valid: true},
	}

	log, err := convertBigQueryRowToQueryLog(row, obfuscator, "bigquery")
	require.NoError(t, err)
	fields := log.Metadata.GetFields()
	require.NotContains(t, fields, "total_slot_ms")
	require.NotContains(t, fields, "total_bytes_processed")
	require.NotContains(t, fields, "total_bytes_billed")
	require.NotContains(t, fields, "reservation_id")

	row.TotalSlotMs = bigquery.NullInt64{Int64: 0, Valid: true}
	row.TotalBytesProcessed = bigquery.NullInt64{Int64: 0, Valid: true}
	log, err = convertBigQueryRowToQueryLog(row, obfuscator, "bigquery")
	require.NoError(t, err)
	fields = log.Metadata.GetFields()
	require.Contains(t, fields, "total_slot_ms", "a zero the job reported is a value")
	require.Contains(t, fields, "total_bytes_processed", "a zero the job reported is a value")
}
