package athena

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

// With SHOW CREATE enabled and a query context on the call, every Hive table of
// the seed gets its DDL. The executor used to append the context as a block
// comment, which Athena's Hive parser rejects after SHOW CREATE TABLE, so every
// Hive table came back without one; Iceberg tables run on the Trino engine and
// were not affected.
func TestAthenaSqlDefinitionsOfHiveTablesWithQueryContext(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Athena tests in CI")
	}
	if os.Getenv("ATHENA_ACCESS_KEY_ID") == "" || os.Getenv("ATHENA_SECRET_ACCESS_KEY") == "" {
		t.Skip("ATHENA_ACCESS_KEY_ID / ATHENA_SECRET_ACCESS_KEY env vars not set")
	}
	ctx := querycontext.WithQueryContext(context.Background(), querycontext.QueryContext{"source": "synq", "check": "sql_definitions"})
	database := testenv.EnvOrDefault("ATHENA_TEST_DATABASE", "synq_dwhtesting")
	ctx = scope.WithScope(ctx, &scope.ScopeFilter{Include: []scope.ScopeRule{{Schema: database}}})

	sc, err := newAthenaScrapperFromEnv(ctx)
	require.NoError(t, err)
	defer sc.Close()
	sc.conf.UseShowCreateTable = true
	sc.conf.UseShowCreateView = true

	defs, err := sc.QuerySqlDefinitions(ctx)
	require.NoError(t, err)
	byName := lo.KeyBy(defs, func(d *scrapper.SqlDefinitionRow) string { return d.Table })

	for _, hive := range []string{"elb_logs_parquet", "logs_json", "imports_csv", "metrics_projection"} {
		def, ok := byName[hive]
		require.True(t, ok, "Hive table %s has no definition", hive)
		require.Contains(t, def.Sql, "CREATE EXTERNAL TABLE", hive)
	}
	iceberg, ok := byName["products"]
	require.True(t, ok, "Iceberg table products has no definition")
	require.Contains(t, iceberg.Sql, "CREATE TABLE")
	view, ok := byName["active_products"]
	require.True(t, ok, "view active_products has no definition")
	require.True(t, view.IsView)
	require.True(t, strings.Contains(strings.ToUpper(view.Sql), "SELECT"))
}
