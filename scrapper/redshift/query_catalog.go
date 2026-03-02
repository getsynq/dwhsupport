package redshift

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_catalog.sql
var queryCatalogSql string

func (e *RedshiftScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	sql := scope.AppendScopeConditions(ctx, queryCatalogSql, "", "schema_name", "table_name")
	return stdsql.QueryMany[scrapper.CatalogColumnRow](ctx, e.executor.GetDb(), sql,
		dwhexec.WithArgs[scrapper.CatalogColumnRow](e.conf.Database),
		dwhexec.WithPostProcessors(func(row *scrapper.CatalogColumnRow) (*scrapper.CatalogColumnRow, error) {
			row.Database = e.conf.Database
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
