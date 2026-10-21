package redshift

import (
	"context"
	"strconv"
	"strings"
	"testing"

	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newRedshiftScrapperFromEnv(t *testing.T, useShowCreateTable bool) *RedshiftScrapper {
	host := testenv.EnvOrDefault("REDSHIFT_HOST", "")
	if host == "" {
		t.Skip("REDSHIFT_HOST env var not set")
	}
	port, err := strconv.Atoi(testenv.EnvOrDefault("REDSHIFT_PORT", "5439"))
	require.NoError(t, err)
	sc, err := NewRedshiftScrapper(t.Context(), &RedshiftScrapperConf{
		RedshiftConf: dwhexecredshift.RedshiftConf{
			Host:     host,
			Port:     port,
			User:     testenv.EnvOrDefault("REDSHIFT_USER", ""),
			Password: testenv.EnvOrDefault("REDSHIFT_PASSWORD", ""),
			Database: testenv.EnvOrDefault("REDSHIFT_DATABASE", "dev"),
		},
		UseShowCreateTable: useShowCreateTable,
	})
	if err != nil {
		t.Skipf("Could not connect to Redshift: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })
	return sc
}

// With UseShowCreateTable every table of the dwhtesting fixture gets its SHOW
// TABLE DDL, while views and materialized views keep their query text.
func TestRedshiftSqlDefinitionsOfTablesAndViews(t *testing.T) {
	sc := newRedshiftScrapperFromEnv(t, true)

	defs, err := sc.QuerySqlDefinitions(scrapper.WithIncompleteResults(t.Context()))
	require.NoError(t, err)
	byName := map[string]*scrapper.SqlDefinitionRow{}
	for _, def := range defs {
		name := def.Schema + "." + def.Table
		require.NotContains(t, byName, name, "listed twice")
		byName[name] = def
		assert.False(t, strings.HasPrefix(def.Table, "mv_tbl__"), "materialized view backing table %s is listed", name)
		assert.Equal(t, sc.conf.Database, def.Database)
	}

	for _, name := range []string{"schema_a.products", "schema_a.order_items", "schema_b.customers", "schema_b.customers_export", "pipelines.events_raw"} {
		def := byName[name]
		if !assert.NotNil(t, def, "%s has no definition", name) {
			continue
		}
		assert.True(t, def.IsTable, name)
		assert.False(t, def.IsView, name)
		assert.True(t, strings.HasPrefix(def.Sql, "CREATE TABLE "+name+" ("), "%s: %s", name, def.Sql)
	}
	assert.Contains(t, byName["schema_a.order_items"].Sql, "FOREIGN KEY (product_id) REFERENCES schema_a.products(id)")

	// SHOW TABLE answers a view with a CREATE TABLE of its columns, which must
	// not replace the view's query.
	for _, name := range []string{"schema_a.active_products", "schema_b.customers_lb", "pipelines.events_hourly"} {
		def := byName[name]
		if !assert.NotNil(t, def, "%s has no definition", name) {
			continue
		}
		assert.False(t, def.IsTable, name)
		assert.NotContains(t, def.Sql, "CREATE TABLE", name)
	}
}

func TestRedshiftSqlDefinitionsWithoutShowTableHaveNoTables(t *testing.T) {
	sc := newRedshiftScrapperFromEnv(t, false)

	defs, err := sc.QuerySqlDefinitions(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, defs)
	for _, def := range defs {
		assert.False(t, def.IsTable, "%s.%s", def.Schema, def.Table)
		assert.NotEqual(t, "products", def.Table)
	}
}

func TestRedshiftSqlDefinitionsOfTablesFollowTheScope(t *testing.T) {
	sc := newRedshiftScrapperFromEnv(t, true)

	ctx := scope.WithScope(context.Background(), &scope.ScopeFilter{
		Include: []scope.ScopeRule{{Schema: "schema_b", Table: "customers_export"}},
	})
	defs, err := sc.QuerySqlDefinitions(ctx)
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, "customers_export", defs[0].Table)
	assert.True(t, strings.HasPrefix(defs[0].Sql, "CREATE TABLE schema_b.customers_export ("), defs[0].Sql)
}
