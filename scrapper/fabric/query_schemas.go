package fabric

import (
	"context"
	_ "embed"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_schemas.sql
var querySchemasSql string

func (e *FabricScrapper) QuerySchemas(ctx context.Context) ([]*scrapper.SchemaRow, error) {
	return queryEachDatabase(ctx, e,
		func(db string) string {
			return expandDatabase(scope.AppendSchemaScopeConditions(ctx, querySchemasSql, "", "s.name"), db)
		},
		func(row *scrapper.SchemaRow, db string) {
			row.Instance = e.conf.Host
			row.Database = db
		},
	)
}
