package fabric

import (
	"context"
	_ "embed"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_table_constraints.sql
var queryTableConstraintsSql string

func (e *FabricScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	return queryEachDatabase(ctx, e,
		func(db string) string {
			return expandDatabase(scope.AppendScopeConditions(ctx, queryTableConstraintsSql, "", "tc.TABLE_SCHEMA", "tc.TABLE_NAME"), db)
		},
		func(row *scrapper.TableConstraintRow, db string) {
			row.Instance = e.conf.Host
			row.Database = db
		},
	)
}
