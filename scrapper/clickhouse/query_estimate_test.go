package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseClickhouseEstimate(t *testing.T) {
	columns := []string{"database", "table", "parts", "rows", "marks"}

	t.Run("sums rows across parts", func(t *testing.T) {
		// ClickHouse surfaces UInt64 columns as uint64 via the native driver.
		rows := [][]any{
			{"db", "t1", uint64(3), uint64(1000), uint64(4)},
			{"db", "t2", uint64(1), uint64(250), uint64(1)},
		}
		est, err := parseClickhouseEstimate(columns, rows)
		require.NoError(t, err)
		require.NotNil(t, est.Rows)
		assert.EqualValues(t, 1250, *est.Rows)
		assert.Nil(t, est.BytesScanned)
		assert.False(t, est.Exact)
	})

	t.Run("handles text-encoded numbers", func(t *testing.T) {
		rows := [][]any{{"db", "t1", "2", []byte("500"), "3"}}
		est, err := parseClickhouseEstimate(columns, rows)
		require.NoError(t, err)
		require.NotNil(t, est.Rows)
		assert.EqualValues(t, 500, *est.Rows)
	})

	t.Run("empty estimate for constant select", func(t *testing.T) {
		est, err := parseClickhouseEstimate(columns, nil)
		require.NoError(t, err)
		require.NotNil(t, est.Rows)
		assert.EqualValues(t, 0, *est.Rows)
	})

	t.Run("errors when rows column absent", func(t *testing.T) {
		_, err := parseClickhouseEstimate([]string{"database", "table"}, [][]any{{"db", "t"}})
		assert.Error(t, err)
	})
}
