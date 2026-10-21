package trino

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const pgConnectionFailed = "Error listing schemas for catalog pg: The connection attempt failed."

// ValidateConfiguration names each catalog it will skip together with what Trino
// answered the probe with, since that answer is the only thing that says whether
// to fix the catalog's connector, its database or the integration.
func TestValidateConfigurationSaysWhyEachCatalogIsSkipped(t *testing.T) {
	ctx := context.Background()
	fake := &fakeTrino{t: t, routes: []fakeRoute{
		catalogsRoute(
			[]any{"tpch", "tpch", "tpch"},
			[]any{"pg", "pg", "postgresql"},
			[]any{"broken", "broken", "hive"},
			[]any{"lake", "lake", "iceberg"},
		),
		{match: sqlContains("FROM tpch.information_schema.schemata"), columns: []fakeColumn{{"_col0", "integer"}}, rows: [][]any{{1}}},
		failing(sqlContains("FROM pg.information_schema.schemata"), "EXTERNAL", pgConnectionFailed),
		{
			match: sqlContains("FROM broken.information_schema.schemata"),
			err:   &fakeTrinoError{errorName: "CATALOG_UNAVAILABLE", errorType: "USER_ERROR", message: "Catalog 'broken' failed to initialize"},
		},
		// A catalog whose storage cannot be read is not skipped: its reads report
		// it, with Trino's error, and the probe says nothing of its own.
		failing(sqlContains("FROM lake.information_schema.schemata"), "EXTERNAL", lakeStorageError),
	}}
	scr := newFakeTrinoScrapper(t, ctx, fake, TrinoScrapperConf{Catalogs: []string{"tpch", "pg", "broken", "lake", "missing"}})

	warnings, err := scr.ValidateConfiguration(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"Catalog not found or no permissions to access: missing",
		"Catalog pg is no longer available and will be skipped during data extraction. " +
			"It may need to be reconfigured or reconnected. Trino answered: EXTERNAL: " + pgConnectionFailed,
		"Catalog broken is no longer available and will be skipped during data extraction. " +
			"It may need to be reconfigured or reconnected. Trino answered: USER_ERROR: Catalog 'broken' failed to initialize",
	}, warnings)
}

func TestValidateConfigurationWithEveryCatalogAvailable(t *testing.T) {
	ctx := context.Background()
	fake := &fakeTrino{t: t, routes: []fakeRoute{
		catalogsRoute([]any{"tpch", "tpch", "tpch"}),
		{match: sqlContains("FROM tpch.information_schema.schemata"), columns: []fakeColumn{{"_col0", "integer"}}, rows: [][]any{{1}}},
	}}
	scr := newFakeTrinoScrapper(t, ctx, fake, TrinoScrapperConf{Catalogs: []string{"tpch"}})

	warnings, err := scr.ValidateConfiguration(ctx)
	require.NoError(t, err)
	assert.Empty(t, warnings)
}
