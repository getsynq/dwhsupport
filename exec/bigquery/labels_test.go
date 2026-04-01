package bigquery

import (
	"testing"

	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/stretchr/testify/assert"
)

func TestQueryContextToBigQueryLabels(t *testing.T) {
	assert.Nil(t, queryContextToBigQueryLabels(nil))
	assert.Nil(t, queryContextToBigQueryLabels(querycontext.QueryContext{}))

	qc := querycontext.QueryContext{
		"source":     "synq",
		"MonitorID":  "abc-123",
		"123numeric": "val",
	}
	labels := queryContextToBigQueryLabels(qc)
	assert.Equal(t, "synq", labels["source"])
	assert.Equal(t, "abc-123", labels["monitorid"])
	assert.Equal(t, "val", labels["l_123numeric"]) // prefixed because starts with digit
}

func TestQueryContextToBigQueryLabels_ValueSanitization(t *testing.T) {
	qc := querycontext.QueryContext{
		"source":  "SYNQ",
		"monitor": "My Monitor / Test",
		"empty":   "",
	}
	labels := queryContextToBigQueryLabels(qc)
	assert.Equal(t, "synq", labels["source"])
	assert.Equal(t, "my_monitor_test", labels["monitor"]) // uppercase, spaces, slash sanitized; consecutive _ collapsed
	assert.Equal(t, "", labels["empty"])

	// Real-world failing value with colons
	qc2 := querycontext.QueryContext{"scope": "monitor::bq-mz-data-prod-datawarehouse::bronze_funnel_exports::"}
	labels2 := queryContextToBigQueryLabels(qc2)
	assert.Equal(t, "monitor_bq-mz-data-prod-datawarehouse_bronze_funnel_exports_", labels2["scope"])
}

func TestQueryContextToBigQueryLabels_Truncation(t *testing.T) {
	longKey := "k" + string(make([]byte, 100))
	longVal := string(make([]byte, 100))
	qc := querycontext.QueryContext{longKey: longVal}
	labels := queryContextToBigQueryLabels(qc)
	for k, v := range labels {
		assert.LessOrEqual(t, len(k), 63)
		assert.LessOrEqual(t, len(v), 63)
	}
}
