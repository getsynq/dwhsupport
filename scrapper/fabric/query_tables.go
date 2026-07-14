package fabric

import (
	"context"
	_ "embed"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_tables.sql
var queryTablesSql string

func (e *FabricScrapper) QueryTables(ctx context.Context, opts ...scrapper.QueryTablesOption) ([]*scrapper.TableRow, error) {
	ctx = e.withEffectiveScope(ctx)
	return queryEachDatabase(ctx, e,
		func(db string) string {
			return expandDatabase(scope.AppendScopeConditions(ctx, queryTablesSql, "", "s.name", "o.name"), db)
		},
		func(row *scrapper.TableRow, db string) {
			row.Instance = e.conf.Host
			row.Database = db
		},
	)
}
