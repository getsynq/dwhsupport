package bigquery

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

func TestBigQueryQueryLogWeight(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)
	ptr := func(v int64) *int64 { return &v }

	start := time.Date(2026, 10, 8, 14, 58, 2, 197_000_000, time.UTC)
	row := &BigQueryQueryLogSchema{
		CreationTime:        start,
		StartTime:           start,
		EndTime:             start.Add(690 * time.Millisecond),
		ProjectId:           bigquery.NullString{StringVal: "my-project", Valid: true},
		JobId:               bigquery.NullString{StringVal: "job-1", Valid: true},
		JobType:             bigquery.NullString{StringVal: "QUERY", Valid: true},
		StatementType:       bigquery.NullString{StringVal: "SELECT", Valid: true},
		State:               bigquery.NullString{StringVal: "DONE", Valid: true},
		ReservationId:       bigquery.NullString{StringVal: "admin-project:EU.default", Valid: true},
		TotalSlotMs:         bigquery.NullInt64{Int64: 688, Valid: true},
		TotalBytesProcessed: bigquery.NullInt64{Int64: 109_557, Valid: true},
	}
	log, err := convertBigQueryRowToQueryLog(row, obfuscator, "bigquery")
	require.NoError(t, err)
	require.Equal(t, querylogs.QueryWeight{
		ExecutionMs: ptr(690), ComputeMs: ptr(688), BytesScanned: ptr(109_557), ComputeId: "admin-project:EU.default",
	}, log.Weight())

	row.ReservationId = bigquery.NullString{}
	row.TotalSlotMs = bigquery.NullInt64{}
	row.TotalBytesProcessed = bigquery.NullInt64{}
	log, err = convertBigQueryRowToQueryLog(row, obfuscator, "bigquery")
	require.NoError(t, err)
	require.Equal(t, querylogs.QueryWeight{ExecutionMs: ptr(690), ComputeId: "my-project"}, log.Weight(),
		"on demand, the project is the compute, and figures the job did not report stay empty")
}
