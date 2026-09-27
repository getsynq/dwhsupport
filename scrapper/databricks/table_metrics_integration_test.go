package databricks

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/stretchr/testify/require"
)

// TestRefreshTableMetricsOnNamesThatNeedQuoting runs the ANALYZE TABLE refresh
// against a real workspace, on a schema whose name has a dash in it. Unquoted,
// Databricks rejects that name with INVALID_IDENTIFIER, the refresh only logs
// the failure, and the row count a full-scan ANALYZE computes never appears.
//
// It creates and drops its own schema, so DATABRICKS_CATALOG must name a
// catalog the principal may create schemas in.
func TestRefreshTableMetricsOnNamesThatNeedQuoting(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Databricks integration tests in CI")
	}
	catalog := os.Getenv("DATABRICKS_CATALOG")
	if catalog == "" {
		t.Skip("DATABRICKS_CATALOG env var not set")
	}

	ctx := context.Background()
	sc := newIntegrationScrapper(t, ctx)
	t.Cleanup(func() { _ = sc.Close() })
	sc.conf.RefreshTableMetrics = true
	sc.conf.RefreshTableMetricsUseScan = true

	executor, err := sc.Executor()
	require.NoError(t, err)

	schemaName := fmt.Sprintf("dwhsupport_analyze_%d-dash", time.Now().UnixNano())
	schemaSql, err := quotedName(catalog, schemaName)
	require.NoError(t, err)
	tableSql, err := quotedName(catalog, schemaName, "my-table")
	require.NoError(t, err)

	require.NoError(t, executor.Exec(ctx, "CREATE SCHEMA "+schemaSql))
	t.Cleanup(func() { _ = executor.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schemaSql+" CASCADE") })
	require.NoError(t, executor.Exec(ctx, "CREATE TABLE "+tableSql+" (id INT)"))
	require.NoError(t, executor.Exec(ctx, "INSERT INTO "+tableSql+" VALUES (1), (2), (3)"))

	scoped := scope.WithScope(ctx, &scope.ScopeFilter{Include: []scope.ScopeRule{{Database: catalog, Schema: schemaName}}})
	rows, err := sc.QueryTableMetrics(scoped, time.Time{})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "my-table", rows[0].Table)
	require.NotNil(t, rows[0].RowCount, "the row count comes from the ANALYZE TABLE the refresh ran")
	require.EqualValues(t, 3, *rows[0].RowCount)
}
