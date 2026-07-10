package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePostgresEstimate(t *testing.T) {
	t.Run("single scan node", func(t *testing.T) {
		// Representative `EXPLAIN (FORMAT JSON) SELECT ...` output.
		jsonText := `[
		  {
		    "Plan": {
		      "Node Type": "Seq Scan",
		      "Relation Name": "orders",
		      "Plan Rows": 12345,
		      "Plan Width": 64
		    }
		  }
		]`
		est, err := parsePostgresEstimate(jsonText)
		require.NoError(t, err)
		require.NotNil(t, est.Rows)
		assert.EqualValues(t, 12345, *est.Rows)
		assert.Nil(t, est.BytesScanned)
		assert.False(t, est.Exact)
	})

	t.Run("aggregate uses deepest scan estimate, not top output", func(t *testing.T) {
		// SELECT count(*) FROM big_table: top Aggregate emits 1 row while the
		// underlying Seq Scan reads 1,000,000 — the scan figure is what matters.
		jsonText := `[{"Plan":{"Node Type":"Aggregate","Plan Rows":1,"Plan Width":8,"Plans":[{"Node Type":"Seq Scan","Plan Rows":1000000,"Plan Width":4}]}}]`
		est, err := parsePostgresEstimate(jsonText)
		require.NoError(t, err)
		require.NotNil(t, est.Rows)
		assert.EqualValues(t, 1000000, *est.Rows)
	})

	t.Run("errors on malformed json", func(t *testing.T) {
		_, err := parsePostgresEstimate("not json")
		assert.Error(t, err)
	})

	t.Run("errors on empty array", func(t *testing.T) {
		_, err := parsePostgresEstimate("[]")
		assert.Error(t, err)
	})
}
