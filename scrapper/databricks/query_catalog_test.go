package databricks

import (
	"context"
	"testing"

	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	"github.com/getsynq/dwhsupport/lazy"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// TestQueryCatalogWithoutTags reads the catalog of a workspace whose tags cannot be
// read. Tags come from each catalog's information_schema over a SQL warehouse, which
// can be refused, stopped or unreachable while the Unity Catalog listing works. The
// tables and columns must still be returned, only without tags.
func TestQueryCatalogWithoutTags(t *testing.T) {
	fake := newFakeWorkspace()
	fake.addCatalog("catalog_0")
	fake.addSchema("catalog_0", "schema_0")
	fake.addTable("catalog_0", "schema_0", "table_0")

	scrapper := fake.startWith(t, &DatabricksScrapperConf{FetchTableTags: true})
	scrapper.lazyExecutor = lazy.New(func() (*dwhexecdatabricks.DatabricksExecutor, error) {
		return nil, errors.New("SQL warehouse is not reachable")
	})

	rows, err := scrapper.QueryCatalog(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "table_0", rows[0].Table)
	require.Empty(t, rows[0].TableTags)
	require.Empty(t, rows[0].ColumnTags)
}
