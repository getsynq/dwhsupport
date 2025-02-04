package duckdb

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_catalog.sql
var queryCatalogSql string

func (e *DuckDBScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.ColumnCatalogRow, error) {
	return stdsql.QueryMany[scrapper.ColumnCatalogRow](ctx, e.executor.GetDb(), queryCatalogSql,
		dwhexec.WithPostProcessors(func(row *scrapper.ColumnCatalogRow) (*scrapper.ColumnCatalogRow, error) {
			row.Database = e.conf.Database
			return row, nil
		}),
	)
}
