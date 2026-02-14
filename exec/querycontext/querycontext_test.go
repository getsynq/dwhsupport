package querycontext

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithQueryContext_RoundTrip(t *testing.T) {
	ctx := context.Background()
	assert.Nil(t, GetQueryContext(ctx))

	qc := QueryContext{"source": "synq", "monitor": "foo"}
	ctx = WithQueryContext(ctx, qc)

	got := GetQueryContext(ctx)
	require.NotNil(t, got)
	assert.Equal(t, "synq", got["source"])
	assert.Equal(t, "foo", got["monitor"])
}

func TestFormatAsJSON(t *testing.T) {
	assert.Equal(t, "", QueryContext(nil).FormatAsJSON())
	assert.Equal(t, "", QueryContext{}.FormatAsJSON())

	qc := QueryContext{"source": "synq"}
	assert.Equal(t, `{"source":"synq"}`, qc.FormatAsJSON())
}

func TestFormatAsSQLComment(t *testing.T) {
	assert.Equal(t, "", QueryContext(nil).FormatAsSQLComment())
	assert.Equal(t, "", QueryContext{}.FormatAsSQLComment())

	qc := QueryContext{"source": "synq"}
	assert.Equal(t, ` /* {"source":"synq"} */`, qc.FormatAsSQLComment())
}

func TestAppendSQLComment(t *testing.T) {
	ctx := context.Background()
	sql := "SELECT 1"

	// No query context — unchanged
	assert.Equal(t, "SELECT 1", AppendSQLComment(ctx, sql))

	// With query context — appended
	ctx = WithQueryContext(ctx, QueryContext{"source": "synq"})
	result := AppendSQLComment(ctx, sql)
	assert.Equal(t, `SELECT 1 /* {"source":"synq"} */`, result)
}

func TestAppendSQLComment_StripsTrailingSemicolon(t *testing.T) {
	ctx := WithQueryContext(context.Background(), QueryContext{"source": "synq"})

	result := AppendSQLComment(ctx, "SELECT 1;")
	assert.Equal(t, `SELECT 1 /* {"source":"synq"} */`, result)

	result = AppendSQLComment(ctx, "SELECT 1 ;  \n")
	assert.Equal(t, `SELECT 1 /* {"source":"synq"} */`, result)
}

func TestAsBigQueryLabels(t *testing.T) {
	assert.Nil(t, QueryContext(nil).AsBigQueryLabels())
	assert.Nil(t, QueryContext{}.AsBigQueryLabels())

	qc := QueryContext{
		"source":     "synq",
		"MonitorID":  "abc-123",
		"123numeric": "val",
	}
	labels := qc.AsBigQueryLabels()
	assert.Equal(t, "synq", labels["source"])
	assert.Equal(t, "abc-123", labels["monitorid"])
	assert.Equal(t, "val", labels["l_123numeric"]) // prefixed because starts with digit
}

func TestAsBigQueryLabels_Truncation(t *testing.T) {
	longKey := "k" + string(make([]byte, 100))
	longVal := string(make([]byte, 100))
	qc := QueryContext{longKey: longVal}
	labels := qc.AsBigQueryLabels()
	for k, v := range labels {
		assert.LessOrEqual(t, len(k), 63)
		assert.LessOrEqual(t, len(v), 63)
	}
}
