package databricks

import (
	"testing"
	"time"

	servicesql "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

// A query served from the result cache reads no bytes and spends no task time. Those zeros are what
// the query cost, so they belong in the metrics as much as any other figure.
func TestConvertDatabricksQueryInfoKeepsZeroMetrics(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	start := time.Date(2026, 10, 8, 15, 0, 0, 0, time.UTC)
	queryInfo := &servicesql.QueryInfo{
		QueryId:          "query-cached",
		QueryText:        "SELECT 1",
		QueryStartTimeMs: start.UnixMilli(),
		QueryEndTimeMs:   start.Add(40 * time.Millisecond).UnixMilli(),
		Status:           servicesql.QueryStatusFinished,
		StatementType:    servicesql.QueryStatementTypeSelect,
		UserName:         "analyst@example.com",
		WarehouseId:      "warehouse-1",
		Duration:         40,
		Metrics:          &servicesql.QueryMetrics{ResultFromCache: true, TotalTimeMs: 40},
	}

	log, err := convertDatabricksQueryInfoToQueryLog(queryInfo, obfuscator, "databricks", "https://example.cloud.databricks.com", "")
	require.NoError(t, err)
	metrics := log.Metadata.GetFields()["metrics"].GetStructValue().GetFields()
	for _, key := range []string{"execution_time_ms", "task_total_time_ms", "read_bytes", "read_remote_bytes", "rows_read_count"} {
		require.Contains(t, metrics, key)
		require.Zero(t, metrics[key].GetNumberValue(), key)
	}
	require.Equal(t, float64(40), metrics["total_time_ms"].GetNumberValue())
}
