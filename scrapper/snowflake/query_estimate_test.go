package snowflake

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSnowflakeEstimate(t *testing.T) {
	t.Run("reads GlobalStats.bytesAssigned", func(t *testing.T) {
		// Representative `EXPLAIN USING JSON SELECT ...` output.
		jsonText := `{
		  "GlobalStats": {
		    "partitionsTotal": 10,
		    "partitionsAssigned": 4,
		    "bytesAssigned": 8388608
		  },
		  "Operations": [[{"id":0,"operation":"TableScan"}]]
		}`
		est, err := parseSnowflakeEstimate(jsonText)
		require.NoError(t, err)
		require.NotNil(t, est.BytesScanned)
		assert.EqualValues(t, 8388608, *est.BytesScanned)
		assert.Nil(t, est.Rows)
		assert.False(t, est.Exact)
	})

	t.Run("zero bytes for constant select", func(t *testing.T) {
		jsonText := `{"GlobalStats":{"partitionsTotal":0,"partitionsAssigned":0,"bytesAssigned":0}}`
		est, err := parseSnowflakeEstimate(jsonText)
		require.NoError(t, err)
		require.NotNil(t, est.BytesScanned)
		assert.EqualValues(t, 0, *est.BytesScanned)
	})

	t.Run("errors on malformed json", func(t *testing.T) {
		_, err := parseSnowflakeEstimate("<xml/>")
		assert.Error(t, err)
	})
}
