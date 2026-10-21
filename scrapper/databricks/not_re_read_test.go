package databricks

import (
	"context"
	"testing"
	"time"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/getsynq/dwhsupport/scrapper"
)

// A table that has not changed since the last fetch is not analyzed again, and the metrics its
// listing lacks are marked as not re-read, as the last refresh's statistics still hold. A table
// that was refreshed, or whose refresh was never on, marks nothing.
func TestMetricsOfATableNotAnalyzedAgainAreMarkedNotReRead(t *testing.T) {
	lastFetch := time.Now().Add(-time.Hour)
	before := lastFetch.Add(-24 * time.Hour).UnixMilli()

	fake := newFakeWorkspace()
	fake.addCatalog("sales")
	fake.addSchema("sales", "orders")
	for _, name := range []string{"changed", "unchanged", "unchanged_with_stats", "unchanged_view", "no_update_time"} {
		fake.addTable("sales", "orders", name)
	}
	tables := fake.tables["sales.orders"]
	tables[1].UpdatedAt = before
	tables[2].UpdatedAt = before
	tables[2].Properties = map[string]string{"spark.sql.statistics.numRows": "10", "spark.sql.statistics.totalSize": "1000"}
	tables[3].UpdatedAt = before
	tables[3].TableType = servicecatalog.TableTypeView
	tables[4].UpdatedAt = 0
	fake.readProperties = map[string]string{"spark.sql.statistics.numRows": "20", "spark.sql.statistics.totalSize": "2000"}

	byTable := func(rows []*scrapper.TableMetricsRow) map[string]*scrapper.TableMetricsRow {
		return lo.SliceToMap(rows, func(r *scrapper.TableMetricsRow) (string, *scrapper.TableMetricsRow) { return r.Table, r })
	}

	t.Run("with refreshing on", func(t *testing.T) {
		fake.readTables = nil
		s := fake.startWith(t, &DatabricksScrapperConf{RefreshTableMetrics: true})
		rows, err := s.QueryTableMetrics(context.Background(), lastFetch)
		require.NoError(t, err)
		require.Equal(t, []string{"sales.orders.changed"}, fake.readTables)
		got := byTable(rows)

		require.Equal(t, int64(20), *got["changed"].RowCount)
		require.Empty(t, got["changed"].NotReRead)

		require.Nil(t, got["unchanged"].RowCount)
		require.Equal(t, []scrapper.TableMetric{scrapper.TableMetricRowCount, scrapper.TableMetricSizeBytes}, got["unchanged"].NotReRead)

		require.Equal(t, int64(10), *got["unchanged_with_stats"].RowCount, "the listing carries the last refresh's statistics")
		require.Empty(t, got["unchanged_with_stats"].NotReRead)

		require.Empty(t, got["unchanged_view"].NotReRead, "a view has no statistics to refresh")
		require.Empty(t, got["no_update_time"].NotReRead, "without an update time there is no change to compare")
	})

	t.Run("with refreshing off", func(t *testing.T) {
		fake.readTables = nil
		s := fake.startWith(t, &DatabricksScrapperConf{})
		rows, err := s.QueryTableMetrics(context.Background(), lastFetch)
		require.NoError(t, err)
		require.Empty(t, fake.readTables)
		for _, row := range rows {
			require.Empty(t, row.NotReRead, "%s: no refresh was skipped, none was ever asked for", row.Table)
		}
	})
}
