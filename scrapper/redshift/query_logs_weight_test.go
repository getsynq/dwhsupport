package redshift

import (
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

// SYS_QUERY_HISTORY reports its times in microseconds. elapsed_time adds the queue, planning and
// compilation to execution_time, so only execution_time is time the query held compute.
func TestRedshiftQueryLogWeight(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)
	start := time.Date(2026, 10, 8, 15, 40, 46, 570_895_000, time.UTC)
	end := start.Add(818_637 * time.Microsecond)

	row := &RedshiftQueryLogSchema{
		QueryId:       1001,
		Status:        strPtr("success"),
		QueryType:     strPtr("SELECT"),
		StartTime:     &start,
		EndTime:       &end,
		ElapsedTime:   int64Ptr(818_637),
		QueueTime:     int64Ptr(0),
		ExecutionTime: int64Ptr(106_226),
		QueryText:     strPtr("select 1"),
	}
	log, err := convertRedshiftRowToQueryLog(row, nil, obfuscator, "redshift", "host", "db")
	require.NoError(t, err)
	require.Equal(t, querylogs.QueryWeight{ExecutionMs: int64Ptr(106)}, log.Weight())

	row.ExecutionTime = int64Ptr(0)
	log, err = convertRedshiftRowToQueryLog(row, nil, obfuscator, "redshift", "host", "db")
	require.NoError(t, err)
	require.Equal(t, querylogs.QueryWeight{ExecutionMs: int64Ptr(0)}, log.Weight(), "the leader node answered alone")
}
