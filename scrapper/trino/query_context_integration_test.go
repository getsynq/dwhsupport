package trino

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/require"
)

// Every SHOW STATS the table metrics send carries our query context, so the
// warehouse's own history attributes it to us. Trino keeps recent queries with
// their text in system.runtime.queries, which is what this reads back.
func TestSelfHostedTrinoTableMetricsCarryQueryContext(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping self-hosted Trino tests in CI")
	}
	if testenv.EnvOrDefault("TRINO_HOST", "") == "" {
		t.Skip("TRINO_HOST env var not set")
	}
	run := fmt.Sprintf("table-metrics-%d", time.Now().UnixNano())
	ctx := querycontext.WithQueryContext(context.Background(), querycontext.QueryContext{"app": "synq", "run": run})

	sc, err := newSelfHostedTrinoScrapperFromEnv(ctx, testenv.EnvOrDefault("TRINO_CATALOG", "tpch"))
	if err != nil {
		t.Skipf("Could not connect to self-hosted Trino: %v", err)
	}
	defer sc.Close()

	rows, err := sc.QueryTableMetrics(ctx, time.Time{})
	require.NoError(t, err)
	require.NotEmpty(t, rows)

	var showStats []string
	require.NoError(t, sc.executor.Select(context.Background(), &showStats,
		"SELECT query FROM system.runtime.queries WHERE query LIKE 'SHOW STATS FOR %' AND query LIKE ?", "%"+run+"%"))
	require.NotEmpty(t, showStats, "no SHOW STATS of this run carries the query context")
}
