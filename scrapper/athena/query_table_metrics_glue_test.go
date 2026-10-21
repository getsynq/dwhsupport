package athena

import (
	"context"
	"database/sql/driver"
	"maps"
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

	f := &fakeGlueEndpoint{databases: map[string][]fakeGlueTable{
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
	f := &fakeGlueEndpoint{databases: map[string][]fakeGlueTable{
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
	f := &fakeGlueEndpoint{databases: map[string][]fakeGlueTable{
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
	sc := newFakeGlueScrapper(t, &fakeGlueEndpoint{databases: map[string][]fakeGlueTable{"empty": nil}}, nil)
	rows, err := sc.QueryTableMetrics(context.Background(), time.Now())
	require.NoError(t, err)
	require.Empty(t, rows)
}

// The Iceberg scan is one Athena query per table, so it runs only for a table
// whose Glue UpdateTime moved since the last fetch, which every Iceberg commit
// does. An unchanged Iceberg table is still returned, with what Glue has.
func TestAthenaQueryTableMetrics_IcebergScanOnlyForChangedTables(t *testing.T) {
	lastFetch := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	before := lastFetch.Add(-48 * time.Hour)
	after := lastFetch.Add(time.Hour)
	committed := after.Add(-time.Minute)
	iceberg := func(params map[string]string) map[string]string {
		out := map[string]string{"table_type": "ICEBERG"}
		maps.Copy(out, params)
		return out
	}

	f := &fakeGlueEndpoint{databases: map[string][]fakeGlueTable{
		"sales": {
			{Name: "ice_changed", UpdateTime: epoch(after), Parameters: iceberg(nil)},
			{Name: "ice_unchanged", UpdateTime: epoch(before), Parameters: iceberg(map[string]string{"numRows": "3"})},
			{Name: "ice_at_last_fetch", UpdateTime: epoch(lastFetch), Parameters: iceberg(nil)},
			{Name: "ice_no_time", Parameters: map[string]string{"metadata_location": "s3://bucket/ice_no_time/metadata.json"}},
			{Name: "hive_changed", UpdateTime: epoch(after), Parameters: map[string]string{"numRows": "9"}},
		},
	}}

	t.Run("only changed iceberg tables are scanned", func(t *testing.T) {
		scans, db := newIcebergScans(t, map[string][]driver.Value{
			"ice_changed":       {int64(100), int64(2048), committed},
			"ice_no_time":       {int64(7), int64(70), committed},
			"ice_unchanged":     nil,
			"ice_at_last_fetch": nil,
		})
		sc := newFakeGlueScrapper(t, f, db)
		sc.conf.UseIcebergMetricsScan = true

		rows, err := sc.QueryTableMetrics(context.Background(), lastFetch)
		require.NoError(t, err)
		require.Equal(t, []string{"ice_changed", "ice_no_time"}, scans.tables())

		byTable := metricsByTable(rows)
		require.Len(t, byTable, 5, "every table is returned, scanned or not")

		changed := byTable["sales.ice_changed"]
		require.Equal(t, int64(100), *changed.RowCount)
		require.Equal(t, int64(2048), *changed.SizeBytes)
		require.Equal(t, committed, *changed.UpdatedAt, "the snapshot commit time wins over Glue's")

		require.Empty(t, changed.NotReRead)

		unchanged := byTable["sales.ice_unchanged"]
		require.Equal(t, int64(3), *unchanged.RowCount, "Glue's numbers")
		require.Nil(t, unchanged.SizeBytes)
		require.Equal(t, before, *unchanged.UpdatedAt)
		require.Equal(t, []scrapper.TableMetric{scrapper.TableMetricSizeBytes}, unchanged.NotReRead,
			"Glue had the row count, the last scan has the size")

		atLastFetch := byTable["sales.ice_at_last_fetch"]
		require.Nil(t, atLastFetch.RowCount)
		require.Equal(t, lastFetch, *atLastFetch.UpdatedAt)
		require.Equal(t, []scrapper.TableMetric{scrapper.TableMetricRowCount, scrapper.TableMetricSizeBytes}, atLastFetch.NotReRead)

		require.Equal(t, int64(7), *byTable["sales.ice_no_time"].RowCount, "no time to compare: scanned")
		require.Empty(t, byTable["sales.ice_no_time"].NotReRead)
		require.Equal(t, int64(9), *byTable["sales.hive_changed"].RowCount)
		require.Empty(t, byTable["sales.hive_changed"].NotReRead, "a Hive table has no scan to skip")
	})

	t.Run("a first fetch scans every iceberg table", func(t *testing.T) {
		scans, db := newIcebergScans(t, map[string][]driver.Value{
			"ice_changed":       {int64(100), int64(2048), committed},
			"ice_unchanged":     {int64(30), int64(300), before},
			"ice_at_last_fetch": {int64(40), int64(400), lastFetch},
			"ice_no_time":       {int64(7), int64(70), committed},
		})
		sc := newFakeGlueScrapper(t, f, db)
		sc.conf.UseIcebergMetricsScan = true

		rows, err := sc.QueryTableMetrics(context.Background(), time.Time{})
		require.NoError(t, err)
		require.Equal(t, []string{"ice_at_last_fetch", "ice_changed", "ice_no_time", "ice_unchanged"}, scans.tables())
		unchanged := metricsByTable(rows)["sales.ice_unchanged"]
		require.Equal(t, int64(3), *unchanged.RowCount, "Glue's numRows stays ahead of the scan's")
		require.Equal(t, int64(300), *unchanged.SizeBytes)
		for _, row := range rows {
			require.Empty(t, row.NotReRead, "%s was scanned", row.Table)
		}
	})

	allFail := map[string][]driver.Value{"ice_changed": nil, "ice_no_time": nil, "ice_unchanged": nil, "ice_at_last_fetch": nil}

	t.Run("a failed scan keeps the table with what Glue has", func(t *testing.T) {
		scans, db := newIcebergScans(t, allFail)
		sc := newFakeGlueScrapper(t, f, db)
		sc.conf.UseIcebergMetricsScan = true

		rows, err := sc.QueryTableMetrics(context.Background(), lastFetch)
		require.NoError(t, err)
		require.Equal(t, []string{"ice_changed", "ice_no_time"}, scans.tables())
		byTable := metricsByTable(rows)
		require.Equal(t, after, *byTable["sales.ice_changed"].UpdatedAt)
		require.Nil(t, byTable["sales.ice_changed"].RowCount)
		require.Empty(t, byTable["sales.ice_changed"].NotReRead,
			"the table changed, so the last scan no longer holds: a failed scan is not a skipped one")
		require.NotContains(t, byTable, "sales.ice_no_time", "no time and no numbers: nothing to report")
	})

	t.Run("without the opt-in nothing is scanned", func(t *testing.T) {
		scans, db := newIcebergScans(t, allFail)
		sc := newFakeGlueScrapper(t, f, db)

		rows, err := sc.QueryTableMetrics(context.Background(), time.Time{})
		require.NoError(t, err)
		require.Empty(t, scans.tables())
		require.Len(t, rows, 4)
		for _, row := range rows {
			require.Empty(t, row.NotReRead, "%s: no scan was skipped, none was ever asked for", row.Table)
		}
	})
}
