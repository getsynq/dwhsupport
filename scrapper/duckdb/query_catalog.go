package duckdb

import (
	"context"
	_ "embed"

	_ "github.com/duckdb/duckdb-go/v2"
	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_catalog.sql
var queryCatalogSql string

func (e *DuckDBScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	sql := scope.AppendScopeConditions(ctx, queryCatalogSql, "r.database_name", "r.schema_name", "r.table_name")
	return stdsql.QueryMany[scrapper.CatalogColumnRow](ctx, e.executor.GetDb(), sql,
		dwhexec.WithPostProcessors(func(row *scrapper.CatalogColumnRow) (*scrapper.CatalogColumnRow, error) {
			row.Instance = e.conf.MotherduckAccount
			return row, nil
		}),
	)
}
