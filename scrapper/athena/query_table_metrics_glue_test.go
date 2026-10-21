package athena

import (
	"context"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/stretchr/testify/require"
)

func metricsByTable(rows []*scrapper.TableMetricsRow) map[string]*scrapper.TableMetricsRow {
	out := map[string]*scrapper.TableMetricsRow{}
	for _, r := range rows {
		out[r.Schema+"."+r.Table] = r
	}
	return out
}

// A table that has not changed since the last fetch still reports its
// metrics. The caller passes the last successful start of a job that covers
// the whole workspace, so a filter on it meant a new integration never got a
// first reading and a table that does not change never reported again.
func TestAthenaQueryTableMetrics_ReportsTablesUnchangedSinceLastFetch(t *testing.T) {
	lastFetch := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	before := lastFetch.Add(-48 * time.Hour)
	after := lastFetch.Add(time.Hour)

	f := &fakeGlue{databases: map[string][]fakeGlueTable{
		"sales": {
			{Name: "unchanged", UpdateTime: epoch(before), Parameters: map[string]string{"numRows": "10", "totalSize": "1000"}},
			{Name: "changed", UpdateTime: epoch(after), Parameters: map[string]string{"numRows": "20"}},
			{Name: "at_last_fetch", UpdateTime: epoch(lastFetch), Parameters: map[string]string{"numRows": "30"}},
		},
	}}
	sc := newFakeGlueScrapper(t, f, nil)

	rows, err := sc.QueryTableMetrics(context.Background(), lastFetch)
	require.NoError(t, err)

	byTable := metricsByTable(rows)
	require.Len(t, byTable, 3)

	unchanged := byTable["sales.unchanged"]
	require.NotNil(t, unchanged)
	require.Equal(t, int64(10), *unchanged.RowCount)
	require.Equal(t, int64(1000), *unchanged.SizeBytes)
	require.Equal(t, before, *unchanged.UpdatedAt)
	require.Equal(t, "123456789012.eu-west-1", unchanged.Instance)
	require.Equal(t, "AwsDataCatalog", unchanged.Database)

	require.Equal(t, int64(20), *byTable["sales.changed"].RowCount)
	require.Equal(t, int64(30), *byTable["sales.at_last_fetch"].RowCount)
}

// The same tables come back whatever time the caller passes.
func TestAthenaQueryTableMetrics_IgnoresLastFetchTime(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	f := &fakeGlue{databases: map[string][]fakeGlueTable{
		"sales": {
			{Name: "orders", UpdateTime: epoch(now.Add(-time.Hour)), Parameters: map[string]string{"numRows": "5"}},
		},
	}}
	sc := newFakeGlueScrapper(t, f, nil)

	for _, lastFetch := range []time.Time{{}, now.Add(-24 * time.Hour), now, now.Add(24 * time.Hour)} {
		rows, err := sc.QueryTableMetrics(context.Background(), lastFetch)
		require.NoError(t, err)
		require.Len(t, rows, 1, "lastFetchTime %s", lastFetch)
		require.Equal(t, int64(5), *rows[0].RowCount)
	}
}

func TestAthenaQueryTableMetrics_GlueTables(t *testing.T) {
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	f := &fakeGlue{databases: map[string][]fakeGlueTable{
		"sales": {
			{Name: "orders", UpdateTime: epoch(created.Add(time.Hour)), Parameters: map[string]string{"numRows": "7"}},
			{Name: "orders_view", TableType: "VIRTUAL_VIEW", UpdateTime: epoch(created)},
			{Name: "only_created", CreateTime: epoch(created)},
			{Name: "ddl_time_only", Parameters: map[string]string{"transient_lastDdlTime": "1767323045"}},
			{Name: "no_stats_at_all"},
			{Name: "bad_stats", UpdateTime: epoch(created), Parameters: map[string]string{"numRows": "-1", "totalSize": "x"}},
		},
		"excluded": {
			{Name: "secret", UpdateTime: epoch(created), Parameters: map[string]string{"numRows": "1"}},
		},
	}}

	t.Run("views and tables without any metric are left out", func(t *testing.T) {
		sc := newFakeGlueScrapper(t, f, nil)
		rows, err := sc.QueryTableMetrics(context.Background(), time.Time{})
		require.NoError(t, err)
		byTable := metricsByTable(rows)

		require.NotContains(t, byTable, "sales.orders_view")
		require.NotContains(t, byTable, "sales.no_stats_at_all")

		require.Equal(t, int64(7), *byTable["sales.orders"].RowCount)
		require.Nil(t, byTable["sales.orders"].SizeBytes)

		require.Equal(t, created, *byTable["sales.only_created"].UpdatedAt)
		require.Nil(t, byTable["sales.only_created"].RowCount)

		require.Equal(t, time.Unix(1767323045, 0).UTC(), *byTable["sales.ddl_time_only"].UpdatedAt)

		require.Nil(t, byTable["sales.bad_stats"].RowCount, "a negative numRows is not a count")
		require.Nil(t, byTable["sales.bad_stats"].SizeBytes)
		require.Contains(t, byTable, "excluded.secret")
	})

	t.Run("scope still selects the tables", func(t *testing.T) {
		sc := newFakeGlueScrapper(t, f, nil)
		f.calls = nil
		ctx := scope.WithScope(context.Background(), &scope.ScopeFilter{Include: []scope.ScopeRule{{Schema: "sales"}}})
		rows, err := sc.QueryTableMetrics(ctx, time.Time{})
		require.NoError(t, err)
		byTable := metricsByTable(rows)
		require.NotContains(t, byTable, "excluded.secret")
		require.Contains(t, byTable, "sales.orders")
		require.NotContains(t, f.calls, "GetTables excluded")
	})
}

func TestAthenaQueryTableMetrics_EmptyCatalog(t *testing.T) {
	sc := newFakeGlueScrapper(t, &fakeGlue{databases: map[string][]fakeGlueTable{"empty": nil}}, nil)
	rows, err := sc.QueryTableMetrics(context.Background(), time.Now())
	require.NoError(t, err)
	require.Empty(t, rows)
}
