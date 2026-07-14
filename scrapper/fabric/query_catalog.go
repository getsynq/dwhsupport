package fabric

import (
	"context"
	_ "embed"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_catalog.sql
var queryCatalogSql string

func (e *FabricScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	return queryEachDatabase(ctx, e,
		func(db string) string {
			return expandDatabase(scope.AppendScopeConditions(ctx, queryCatalogSql, "", "s.name", "t.name"), db)
		},
		func(row *scrapper.CatalogColumnRow, db string) {
			row.Instance = e.conf.Host
			row.Database = db
		},
	)
}
