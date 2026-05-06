package athena

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/stretchr/testify/require"
)

// TestAthenaTableMetricsAndConstraints exercises QueryTableMetrics and
// QueryTableConstraints against the dwhtesting Athena seed, in both default
// (Glue-only) and Iceberg-scan modes.
func TestAthenaTableMetricsAndConstraints(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Athena integration test in CI")
	}
	if os.Getenv("ATHENA_ACCESS_KEY_ID") == "" || os.Getenv("ATHENA_SECRET_ACCESS_KEY") == "" {
		t.Skip("ATHENA_ACCESS_KEY_ID / ATHENA_SECRET_ACCESS_KEY env vars not set")
	}

	ctx := context.Background()
	sc, err := newAthenaScrapperFromEnv(ctx)
	require.NoError(t, err)
	defer sc.Close()
	// Limit to the seeded schema so other AWS workloads in the same account
	// don't bloat assertions.
	sc.conf.Scope = &scope.ScopeFilter{Include: []scope.ScopeRule{{Schema: "synq_dwhtesting"}}}

	t.Run("constraints expose hive partition keys (default)", func(t *testing.T) {
		rows, err := sc.QueryTableConstraints(ctx)
		require.NoError(t, err)

		// Hive partitioned tables in the seed: elb_logs_parquet (year/month/day)
		// and metrics_projection (region/dt). Iceberg tables (products,
		// order_items) are NOT in Glue's PartitionKeys — see implementation
		// docstring.
		byTable := groupConstraintsByTable(rows)
		require.Contains(t, byTable, "elb_logs_parquet")
		require.Contains(t, byTable, "metrics_projection")
		require.Equal(t, []string{"year", "month", "day"}, byTable["elb_logs_parquet"])
		require.Equal(t, []string{"region", "dt"}, byTable["metrics_projection"])

		require.NotContains(t, byTable, "products", "Iceberg partitions only show up with UseIcebergMetricsScan")
		require.NotContains(t, byTable, "order_items", "Iceberg partitions only show up with UseIcebergMetricsScan")

		for _, r := range rows {
			require.Equal(t, scrapper.ConstraintTypePartitionBy, r.ConstraintType)
		}
	})

	t.Run("constraints include iceberg partitions when scan is enabled", func(t *testing.T) {
		sc.conf.UseIcebergMetricsScan = true
		defer func() { sc.conf.UseIcebergMetricsScan = false }()

		rows, err := sc.QueryTableConstraints(ctx)
		require.NoError(t, err)
		byTable := groupConstraintsByTable(rows)

		// products is `PARTITIONED BY (category)`, order_items by `day(ordered_at)`.
		// The day() transform field name in Iceberg's $partitions table is
		// typically `<col>_day` (Athena's convention).
		require.Contains(t, byTable, "products", "iceberg products partition column missing")
		require.Equal(t, []string{"category"}, byTable["products"])

		require.Contains(t, byTable, "order_items", "iceberg order_items partition column missing")
		require.NotEmpty(t, byTable["order_items"], "iceberg order_items partition column missing")
	})

	t.Run("metrics from glue parameters only", func(t *testing.T) {
		rows, err := sc.QueryTableMetrics(ctx, time.Time{})
		require.NoError(t, err)
		// Every row that comes back must be backed by at least one populated
		// metric (we drop empty rows in the implementation).
		for _, r := range rows {
			require.True(t, r.RowCount != nil || r.SizeBytes != nil || r.UpdatedAt != nil,
				"row for %s.%s.%s has no metrics", r.Database, r.Schema, r.Table)
		}
	})

	t.Run("iceberg scan opt-in fills row counts", func(t *testing.T) {
		sc.conf.UseIcebergMetricsScan = true
		defer func() { sc.conf.UseIcebergMetricsScan = false }()

		rows, err := sc.QueryTableMetrics(ctx, time.Time{})
		require.NoError(t, err)

		// The seed inserts 3 rows into products and 3 into order_items via Iceberg.
		seen := map[string]*scrapper.TableMetricsRow{}
		for _, r := range rows {
			seen[r.Table] = r
		}
		for _, name := range []string{"products", "order_items"} {
			r, ok := seen[name]
			require.Truef(t, ok, "iceberg table %s missing from metrics", name)
			require.NotNilf(t, r.RowCount, "iceberg table %s has no row count", name)
			require.GreaterOrEqualf(t, *r.RowCount, int64(3), "iceberg table %s row count too low", name)
			require.NotNilf(t, r.SizeBytes, "iceberg table %s has no size", name)
			require.Greaterf(t, *r.SizeBytes, int64(0), "iceberg table %s size is zero", name)
		}
	})

	t.Run("lastFetchTime in the future returns nothing", func(t *testing.T) {
		rows, err := sc.QueryTableMetrics(ctx, time.Now().Add(24*time.Hour))
		require.NoError(t, err)
		require.Empty(t, rows)
	})
}

func groupConstraintsByTable(rows []*scrapper.TableConstraintRow) map[string][]string {
	out := map[string][]string{}
	for _, r := range rows {
		out[r.Table] = append(out[r.Table], r.ColumnName)
	}
	return out
}
