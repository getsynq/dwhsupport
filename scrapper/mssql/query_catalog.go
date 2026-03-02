package mssql

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_catalog.sql
var queryCatalogSql string

func (e *MSSQLScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	sql := scope.AppendScopeConditions(ctx, queryCatalogSql, "", "s.name", "t.name")
	return dwhexecmssql.NewQuerier[scrapper.CatalogColumnRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.CatalogColumnRow) (*scrapper.CatalogColumnRow, error) {
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
