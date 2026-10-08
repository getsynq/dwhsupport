package databricks

import (
	"testing"
	"time"

	servicesql "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

func TestDatabricksQueryLogWeight(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)
	ptr := func(v int64) *int64 { return &v }

	start := time.Date(2026, 10, 8, 15, 0, 0, 0, time.UTC)
	queryInfo := &servicesql.QueryInfo{
		QueryId:          "query-1",
		QueryText:        "CREATE TABLE t AS SELECT * FROM s",
		QueryStartTimeMs: start.UnixMilli(),
		QueryEndTimeMs:   start.Add(5396 * time.Millisecond).UnixMilli(),
		Status:           servicesql.QueryStatusFinished,
		StatementType:    servicesql.QueryStatementTypeCreate,
		UserName:         "analyst@example.com",
		WarehouseId:      "abc123",
		EndpointId:       "abc123",
		Duration:         5396,
		Metrics: &servicesql.QueryMetrics{
			ExecutionTimeMs: 4820, TaskTotalTimeMs: 15_873, ReadBytes: 1_048_576, TotalTimeMs: 5396,
		},
	}
	log, err := convertDatabricksQueryInfoToQueryLog(queryInfo, obfuscator, "databricks", "https://example.cloud.databricks.com", "")
	require.NoError(t, err)
	require.Equal(t, querylogs.QueryWeight{
		ExecutionMs: ptr(4820), ComputeMs: ptr(15_873), BytesScanned: ptr(1_048_576), ComputeId: "abc123",
	}, log.Weight())
}
