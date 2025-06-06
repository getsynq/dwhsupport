package postgres

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_catalog.sql
var queryCatalogSql string

func (e *PostgresScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	return stdsql.QueryMany[scrapper.CatalogColumnRow](ctx, e.executor.GetDb(), queryCatalogSql,
		dwhexec.WithPostProcessors(func(row *scrapper.CatalogColumnRow) (*scrapper.CatalogColumnRow, error) {
			row.Database = e.conf.Database
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
