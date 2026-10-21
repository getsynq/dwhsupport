package trino

import (
	"context"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A catalog backed by MySQL or MariaDB lists a view as a BASE TABLE with the
// comment "VIEW". Any other catalog keeps a comment that says "VIEW".
func TestViewCommentOfMySQLCatalogIsDropped(t *testing.T) {
	ctx := context.Background()
	catalogs := [][]any{{"shop", "shop", "mysql"}, {"maria", "maria", "mariadb"}, {"lake", "lake", "iceberg"}}
	var routes []fakeRoute
	routes = append(routes, catalogsRoute(catalogs...))
	for _, c := range catalogs {
		name := c[0].(string)
		routes = append(routes,
			fakeRoute{
				match:   sqlContains("FROM " + name + ".information_schema.tables t\nJOIN " + name + ".information_schema.columns"),
				columns: catalogColumns,
				rows: [][]any{
					{name, "s", "products", "id", 1, "integer", "Unique product identifier", "Product catalog"},
					{name, "s", "active_products", "id", 1, "integer", nil, "VIEW"},
					{name, "s", "plain", "id", 1, "integer", nil, ""},
				},
			},
			fakeRoute{
				match:   sqlContains("FROM " + name + ".information_schema.tables t"),
				columns: tableColumns,
				rows: [][]any{
					{name, "s", "products", "BASE TABLE", "Product catalog", true, false},
					{name, "s", "active_products", "BASE TABLE", "VIEW", true, false},
					{name, "s", "plain", "BASE TABLE", nil, true, false},
				},
			},
		)
	}
	fake := &fakeTrino{t: t, routes: routes}
	scr := newFakeTrinoScrapper(t, ctx, fake, TrinoScrapperConf{Catalogs: []string{"shop", "maria", "lake"}, FetchTableComments: true})

	tables, err := scr.QueryTables(ctx)
	require.NoError(t, err)
	descriptions := map[string]*string{}
	for _, row := range tables {
		descriptions[row.Database+"."+row.Table] = row.Description
	}
	for _, name := range []string{"shop", "maria"} {
		assert.Nil(t, descriptions[name+".active_products"], name)
		assert.Equal(t, lo.ToPtr("Product catalog"), descriptions[name+".products"], name)
		assert.Nil(t, descriptions[name+".plain"], name)
	}
	assert.Equal(t, lo.ToPtr("VIEW"), descriptions["lake.active_products"], "a comment outside MySQL is kept")

	columns, err := scr.QueryCatalog(ctx)
	require.NoError(t, err)
	byTable := map[string]*scrapper.CatalogColumnRow{}
	for _, row := range columns {
		byTable[row.Database+"."+row.Table] = row
	}
	for _, name := range []string{"shop", "maria"} {
		assert.Equal(t, lo.ToPtr(""), byTable[name+".active_products"].TableComment, name)
		assert.Equal(t, lo.ToPtr("Product catalog"), byTable[name+".products"].TableComment, name)
		assert.Equal(t, lo.ToPtr("Unique product identifier"), byTable[name+".products"].Comment, name)
	}
	assert.Equal(t, lo.ToPtr("VIEW"), byTable["lake.active_products"].TableComment, "a comment outside MySQL is kept")
}
