package duckdb

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
	_ "github.com/marcboeker/go-duckdb"
)

//go:embed query_catalog.sql
var queryCatalogSql string

func (e *DuckDBScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	return stdsql.QueryMany[scrapper.CatalogColumnRow](ctx, e.executor.GetDb(), queryCatalogSql,
		dwhexec.WithPostProcessors(func(row *scrapper.CatalogColumnRow) (*scrapper.CatalogColumnRow, error) {
			row.Instance = e.conf.MotherduckAccount
			return row, nil
		}),
	)
}
