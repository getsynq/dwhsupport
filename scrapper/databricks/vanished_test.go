package databricks

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestQueriesTolerateVanishedObjects drives every scrape that walks Unity Catalog
// against a workspace that drops objects while the walk is in progress: a catalog
// that is gone by the time its schemas are read, and a schema that is gone by the
// time its tables are read.
//
// Both are ordinary races rather than failures — a catalog is listed, and only then
// is each of its schemas read, so anything dropped in between answers "does not
// exist". Treating that as fatal throws away the whole workspace's scrape over one
// disposable schema, and a workspace where CI creates and drops schemas continuously
// can fail every scrape it attempts. What is gone contributes no rows; everything
// else must still be collected.
func TestQueriesTolerateVanishedObjects(t *testing.T) {
	fake := newFakeWorkspace()
	for c := 0; c < 2; c++ {
		catalog := fmt.Sprintf("catalog_%d", c)
		fake.addCatalog(catalog)
		for s := 0; s < 2; s++ {
			schema := fmt.Sprintf("schema_%d", s)
			fake.addSchema(catalog, schema)
			for tbl := 0; tbl < 3; tbl++ {
				fake.addTable(catalog, schema, fmt.Sprintf("table_%d", tbl))
			}
		}
	}
	// Dropped after being listed: one whole catalog, and one schema of a catalog that
	// otherwise survives. The surviving schema of that catalog still has to be read.
	fake.addVanishingCatalog("dropped_catalog")
	fake.addVanishingSchema("catalog_0", "dropped_schema")
	fake.pageSize = 2

	scrapper := fake.start(t)
	ctx := context.Background()

	const (
		// The dropped catalog contributes no schemas; the dropped schema is listed as a
		// schema but contributes no tables.
		wantSchemas = 2*2 + 1
		wantTables  = 2 * 2 * 3
	)

	t.Run("QuerySchemas", func(t *testing.T) {
		rows, err := scrapper.QuerySchemas(ctx)
		require.NoError(t, err)
		require.Len(t, rows, wantSchemas)
	})

	t.Run("QueryTables", func(t *testing.T) {
		rows, err := scrapper.QueryTables(ctx)
		require.NoError(t, err)
		require.Len(t, rows, wantTables)
	})

	t.Run("QueryCatalog", func(t *testing.T) {
		rows, err := scrapper.QueryCatalog(ctx)
		require.NoError(t, err)
		// One row per column, and the fake gives every table a single column.
		require.Len(t, rows, wantTables)
	})

	t.Run("QuerySqlDefinitions", func(t *testing.T) {
		rows, err := scrapper.QuerySqlDefinitions(ctx)
		require.NoError(t, err)
		require.Len(t, rows, wantTables)
	})

	t.Run("QueryTableConstraints", func(t *testing.T) {
		// The fake gives every table one partitioning column and no primary key.
		rows, err := scrapper.QueryTableConstraints(ctx)
		require.NoError(t, err)
		require.Len(t, rows, wantTables)
	})

	t.Run("QueryTableMetrics", func(t *testing.T) {
		rows, err := scrapper.QueryTableMetrics(ctx, time.Now())
		require.NoError(t, err)
		require.Len(t, rows, wantTables)
	})
}

// TestQueriesFailOnUnreadableSchema holds the other side of the line drawn above: a
// schema the caller may not read is not a schema that is gone. Reading no tables for
// it would report it as empty, and a catalog that reports its tables as gone is
// indistinguishable downstream from one whose tables were really dropped — so it has
// to fail the scrape instead.
func TestQueriesFailOnUnreadableSchema(t *testing.T) {
	fake := newFakeWorkspace()
	fake.addCatalog("catalog_0")
	fake.addSchema("catalog_0", "schema_0")
	fake.addTable("catalog_0", "schema_0", "table_0")
	fake.addUnreadableSchema("catalog_0", "denied_schema")

	scrapper := fake.start(t)
	ctx := context.Background()

	for name, query := range map[string]func() error{
		"QueryTables": func() error {
			_, err := scrapper.QueryTables(ctx)
			return err
		},
		"QueryCatalog": func() error {
			_, err := scrapper.QueryCatalog(ctx)
			return err
		},
		"QuerySqlDefinitions": func() error {
			_, err := scrapper.QuerySqlDefinitions(ctx)
			return err
		},
		"QueryTableConstraints": func() error {
			_, err := scrapper.QueryTableConstraints(ctx)
			return err
		},
		"QueryTableMetrics": func() error {
			_, err := scrapper.QueryTableMetrics(ctx, time.Now())
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.ErrorContains(t, query(), "USE SCHEMA")
		})
	}
}
