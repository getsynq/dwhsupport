package fabric

import (
	"context"
	_ "embed"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_sql_definitions.sql
var querySqlDefinitionsSql string

func (e *FabricScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	ctx = e.withEffectiveScope(ctx)
	return queryEachDatabase(ctx, e,
		func(db string) string {
			return expandDatabase(scope.AppendScopeConditions(ctx, querySqlDefinitionsSql, "", "s.name", "v.name"), db)
		},
		func(row *scrapper.SqlDefinitionRow, db string) {
			row.Instance = e.conf.Host
			row.Database = db
		},
	)
}
