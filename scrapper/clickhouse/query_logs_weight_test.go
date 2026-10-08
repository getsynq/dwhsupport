package clickhouse

import (
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

func TestClickhouseQueryLogWeight(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)
	start := time.Date(2026, 10, 8, 15, 0, 0, 0, time.UTC)

	row := &ClickhouseQueryLogSchema{
		QueryType:                  "QueryFinish",
		EventTimeMicroseconds:      start.Add(23 * time.Millisecond),
		QueryStartTimeMicroseconds: start,
		QueryDurationMs:            23,
		ReadRows:                   1200,
		ReadBytes:                  97_981,
		MemoryUsage:                4_000_000,
		Query:                      "SELECT count() FROM events",
		QueryKind:                  "Select",
	}
	log, err := convertClickhouseRowToQueryLog(row, obfuscator, "clickhouse", testQueryLogHost, testQueryLogName)
	require.NoError(t, err)
	duration, read := int64(23), int64(97_981)
	require.Equal(t, querylogs.QueryWeight{ExecutionMs: &duration, BytesScanned: &read}, log.Weight())
}
