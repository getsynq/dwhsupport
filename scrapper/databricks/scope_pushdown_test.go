package databricks

import (
	"context"
	"fmt"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/stretchr/testify/require"
)

// TestContextScopeIsPushedDown drives a walk with a per-call scope (scope.WithScope)
// and checks both halves of honouring it: the rows returned are limited to the scope,
// and the catalogs and schemas outside it are never listed in the first place.
//
// The second half is the point of pushing scope down into the walk. ScopedScrapper
// post-filters rows either way, so the rows alone would look right even if the walk
// had read the whole workspace and thrown most of it away — which for a workspace of
// any size is the difference between one catalog's worth of API calls and all of them.
func TestContextScopeIsPushedDown(t *testing.T) {
	fake := newFakeWorkspace()
	for _, catalog := range []string{"wanted", "unwanted"} {
		fake.addCatalog(catalog)
		for s := 0; s < 2; s++ {
			schema := fmt.Sprintf("schema_%d", s)
			fake.addSchema(catalog, schema)
			for tbl := 0; tbl < 2; tbl++ {
				fake.addTable(catalog, schema, fmt.Sprintf("table_%d", tbl))
			}
		}
	}

	scrapper := fake.start(t)
	ctx := scope.WithScope(context.Background(), &scope.ScopeFilter{
		Include: []scope.ScopeRule{{Database: "wanted", Schema: "schema_0"}},
	})

	rows, err := scrapper.QueryTables(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, rows)
	for _, row := range rows {
		require.Equal(t, "wanted", row.Database)
		require.Equal(t, "schema_0", row.Schema)
	}

	require.True(t, fake.requested(&fake.listedCatalogs, "wanted"),
		"the in-scope catalog's schemas have to be listed")
	require.False(t, fake.requested(&fake.listedCatalogs, "unwanted"),
		"an out-of-scope catalog should be skipped before its schemas are listed")
	require.True(t, fake.requested(&fake.listedSchemas, "wanted.schema_0"),
		"the in-scope schema's tables have to be listed")
	require.False(t, fake.requested(&fake.listedSchemas, "wanted.schema_1"),
		"an out-of-scope schema should be skipped before its tables are listed")
}

// TestBlocklistIsStillPushedDown covers the same push-down for the deprecated
// CatalogBlocklist, so migrating the filtering to Scope has not cost callers that
// still configure a blocklist the API calls they were already saving.
func TestBlocklistIsStillPushedDown(t *testing.T) {
	fake := newFakeWorkspace()
	for _, catalog := range []string{"prod", "test_one"} {
		fake.addCatalog(catalog)
		fake.addSchema(catalog, "schema_0")
		fake.addTable(catalog, "schema_0", "table_0")
	}

	scrapper := fake.start(t)
	scrapper.conf.CatalogBlocklist = "test_*"
	scrapper.scope = ScopeFromConf(scrapper.conf)

	rows, err := scrapper.QueryTables(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "prod", rows[0].Database)

	require.True(t, fake.requested(&fake.listedCatalogs, "prod"))
	require.False(t, fake.requested(&fake.listedCatalogs, "test_one"),
		"a blocklisted catalog should be skipped before its schemas are listed")
}
