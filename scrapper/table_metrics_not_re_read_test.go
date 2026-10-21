package scrapper

import (
	"encoding/json"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestMarkNotReReadListsOnlyWhatTheCheapReadLeftUnset(t *testing.T) {
	for name, tc := range map[string]struct {
		row  TableMetricsRow
		want []TableMetric
	}{
		"nothing read":      {TableMetricsRow{}, []TableMetric{TableMetricRowCount, TableMetricSizeBytes}},
		"row count read":    {TableMetricsRow{RowCount: lo.ToPtr(int64(3))}, []TableMetric{TableMetricSizeBytes}},
		"size read":         {TableMetricsRow{SizeBytes: lo.ToPtr(int64(30))}, []TableMetric{TableMetricRowCount}},
		"both read":         {TableMetricsRow{RowCount: lo.ToPtr(int64(3)), SizeBytes: lo.ToPtr(int64(30))}, nil},
		"a zero is a value": {TableMetricsRow{RowCount: lo.ToPtr(int64(0)), SizeBytes: lo.ToPtr(int64(0))}, nil},
		"marked twice":      {TableMetricsRow{NotReRead: []TableMetric{TableMetricRowCount, TableMetricRowCount}}, []TableMetric{TableMetricRowCount, TableMetricSizeBytes}},
	} {
		t.Run(name, func(t *testing.T) {
			row := tc.row
			row.MarkNotReRead()
			require.Equal(t, tc.want, row.NotReRead)
		})
	}
}

func TestReuseLastReading(t *testing.T) {
	t.Run("fills what was not re-read", func(t *testing.T) {
		row := TableMetricsRow{}
		row.MarkNotReRead()
		last := int64(10)
		row.ReuseLastReading(&last, lo.ToPtr(int64(1000)))
		require.Equal(t, int64(10), *row.RowCount)
		require.Equal(t, int64(1000), *row.SizeBytes)
		require.Empty(t, row.NotReRead)

		last = 11
		require.Equal(t, int64(10), *row.RowCount, "the row holds its own copy")
	})

	t.Run("never replaces what this read reported", func(t *testing.T) {
		row := TableMetricsRow{RowCount: lo.ToPtr(int64(5))}
		row.MarkNotReRead()
		row.ReuseLastReading(lo.ToPtr(int64(10)), lo.ToPtr(int64(1000)))
		require.Equal(t, int64(5), *row.RowCount)
		require.Equal(t, int64(1000), *row.SizeBytes)
	})

	t.Run("a metric listed but set is current", func(t *testing.T) {
		row := TableMetricsRow{RowCount: lo.ToPtr(int64(5)), NotReRead: []TableMetric{TableMetricRowCount}}
		row.ReuseLastReading(lo.ToPtr(int64(10)), nil)
		require.Equal(t, int64(5), *row.RowCount)
		require.Empty(t, row.NotReRead)
	})

	t.Run("what the last reading lacks stays listed", func(t *testing.T) {
		row := TableMetricsRow{}
		row.MarkNotReRead()
		row.ReuseLastReading(nil, lo.ToPtr(int64(1000)))
		require.Nil(t, row.RowCount)
		require.Equal(t, []TableMetric{TableMetricRowCount}, row.NotReRead)
	})

	t.Run("an unmarked row is left as it is", func(t *testing.T) {
		row := TableMetricsRow{}
		row.ReuseLastReading(lo.ToPtr(int64(10)), lo.ToPtr(int64(1000)))
		require.Nil(t, row.RowCount, "unset and not listed: the warehouse did not report it")
		require.Nil(t, row.SizeBytes)
	})

	t.Run("a metric this version does not know stays listed", func(t *testing.T) {
		row := TableMetricsRow{NotReRead: []TableMetric{"bytes_scanned"}}
		row.ReuseLastReading(lo.ToPtr(int64(10)), nil)
		require.Nil(t, row.RowCount)
		require.Equal(t, []TableMetric{"bytes_scanned"}, row.NotReRead)
	})
}

// The marker survives a JSON round trip, a row without one encodes as it did before, and a row
// written before the marker existed decodes as marking nothing.
func TestNotReReadJson(t *testing.T) {
	row := TableMetricsRow{Table: "orders"}
	row.MarkNotReRead()
	encoded, err := json.Marshal(row)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"not_re_read":["row_count","size_bytes"]`)

	var decoded TableMetricsRow
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Equal(t, row.NotReRead, decoded.NotReRead)

	plain, err := json.Marshal(TableMetricsRow{Table: "orders"})
	require.NoError(t, err)
	require.NotContains(t, string(plain), "not_re_read")

	var old TableMetricsRow
	require.NoError(t, json.Unmarshal([]byte(`{"table":"orders","row_count":null}`), &old))
	require.Empty(t, old.NotReRead)
}
