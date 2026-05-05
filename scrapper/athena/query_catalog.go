package athena

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_catalog.sql
var queryCatalogSQL string

func (e *AthenaScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	db := e.executor.GetDb()
	query := scope.AppendScopeConditions(ctx, queryCatalogSQL, "c.table_catalog", "c.table_schema", "c.table_name")
	return stdsql.QueryMany(ctx, db, query,
		dwhexec.WithPostProcessors(func(row *scrapper.CatalogColumnRow) (*scrapper.CatalogColumnRow, error) {
			row.Instance = e.executor.Instance()
			return row, nil
		}),
	)
}
