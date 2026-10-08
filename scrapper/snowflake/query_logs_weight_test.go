package snowflake

import (
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

// A QUERY_HISTORY row as the fetch reads it, decoded into the weight a consumer reads.
func TestSnowflakeQueryLogWeight(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)
	ptr := func(v int64) *int64 { return &v }
	str := func(v string) *string { return &v }
	end := time.Date(2026, 10, 8, 15, 4, 19, 88_000_000, time.UTC)

	t.Run("a sized warehouse with its load", func(t *testing.T) {
		row := &SnowflakeQueryLogSchema{
			QueryID: "q1", ExecutionStatus: "SUCCESS", EndTime: end, StartTime: end.Add(-2868 * time.Millisecond),
			ExecutionTime: 1567, WarehouseID: ptr(1234), WarehouseSize: str("Small"), WarehouseType: str("STANDARD"),
			QueryLoadPercent: ptr(80), BytesScanned: 52_428_800, CreditsUsedCloudServices: 0.000391,
		}
		log, err := convertSnowflakeRowToQueryLog(row, obfuscator, "snowflake", "account")
		require.NoError(t, err)
		require.Equal(t, querylogs.QueryWeight{
			ExecutionMs: ptr(1567), ComputeMs: ptr(2507), BytesScanned: ptr(52_428_800), ComputeId: "1234",
		}, log.Weight())
	})

	t.Run("no warehouse", func(t *testing.T) {
		row := &SnowflakeQueryLogSchema{QueryID: "q2", ExecutionStatus: "SUCCESS", EndTime: end, ExecutionTime: 2}
		log, err := convertSnowflakeRowToQueryLog(row, obfuscator, "snowflake", "account")
		require.NoError(t, err)
		require.Equal(t, querylogs.QueryWeight{ExecutionMs: ptr(2), BytesScanned: ptr(0)}, log.Weight())
	})
}
