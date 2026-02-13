package mssql

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_catalog.sql
var queryCatalogSql string

func (e *MSSQLScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	return dwhexecmssql.NewQuerier[scrapper.CatalogColumnRow](e.executor).QueryMany(ctx, queryCatalogSql,
		dwhexec.WithPostProcessors(func(row *scrapper.CatalogColumnRow) (*scrapper.CatalogColumnRow, error) {
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
