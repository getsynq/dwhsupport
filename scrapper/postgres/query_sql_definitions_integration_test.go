package postgres

import (
	"os"
	"strings"
	"testing"

	dwhexecpostgres "github.com/getsynq/dwhsupport/exec/postgres"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

// A materialized view and a view owned by a role that cannot log in get their
// definitions like any other view. The test creates its own schema and role, so
// it needs CREATEROLE and CREATE on the database, and skips without them.
func TestPostgresSqlDefinitionsOfMaterializedViewsAndNoLoginOwners(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Postgres tests in CI")
	}
	ctx := t.Context()
	sc, err := newPostgresScrapperFromEnv(ctx)
	if err != nil {
		t.Skipf("Postgres not reachable: %v", err)
	}
	defer sc.Close()

	const schema = "dwhsupport_sql_definitions_test"
	const owner = "dwhsupport_sql_definitions_owner"
	cleanup := func() {
		_ = sc.executor.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		_ = sc.executor.Exec(ctx, "DROP ROLE IF EXISTS "+owner)
	}
	cleanup()
	t.Cleanup(cleanup)
	for _, stmt := range []string{
		"CREATE ROLE " + owner + " NOLOGIN",
		"CREATE SCHEMA " + schema,
		"CREATE TABLE " + schema + ".orders (id int, amount numeric(10, 2))",
		"CREATE MATERIALIZED VIEW " + schema + ".order_totals AS SELECT id, sum(amount) AS total FROM " + schema + ".orders GROUP BY id",
		"CREATE VIEW " + schema + ".big_orders AS SELECT id FROM " + schema + ".orders WHERE amount > 100",
		"ALTER VIEW " + schema + ".big_orders OWNER TO " + owner,
		"CREATE VIEW " + schema + ".all_orders AS SELECT id, amount FROM " + schema + ".orders",
	} {
		if err := sc.executor.Exec(ctx, stmt); err != nil {
			if dwhexecpostgres.IsPermissionError(err) {
				t.Skipf("the test role cannot create its fixture: %v", err)
			}
			require.NoError(t, err, stmt)
		}
	}

	defs, err := sc.QuerySqlDefinitions(ctx)
	require.NoError(t, err)
	byName := lo.KeyBy(
		lo.Filter(defs, func(d *scrapper.SqlDefinitionRow, _ int) bool { return d.Schema == schema }),
		func(d *scrapper.SqlDefinitionRow) string { return d.Table },
	)
	require.ElementsMatch(t, []string{"order_totals", "big_orders", "all_orders"}, lo.Keys(byName), "a table has no definition")

	mv := byName["order_totals"]
	require.True(t, mv.IsMaterializedView)
	require.False(t, mv.IsView)
	require.Contains(t, strings.ToLower(mv.Sql), "sum(orders.amount)")

	for _, name := range []string{"big_orders", "all_orders"} {
		view := byName[name]
		require.True(t, view.IsView, name)
		require.False(t, view.IsMaterializedView, name)
		require.Contains(t, strings.ToLower(view.Sql), "from "+schema+".orders", name)
	}
}
