package db2

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecdb2 "github.com/getsynq/dwhsupport/exec/db2"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

// The database of every row is CURRENT SERVER, the name the server knows the
// database by, rather than the configured name or alias.

//go:embed query_catalog.sql
var queryCatalogSql string

func (e *Db2Scrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	sql := scope.AppendScopeConditions(ctx, queryCatalogSql, "", "c.TABSCHEMA", "c.TABNAME")
	return dwhexecdb2.NewQuerier[scrapper.CatalogColumnRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.CatalogColumnRow) (*scrapper.CatalogColumnRow, error) {
			row.Instance = e.conf.Hostname
			return row, nil
		}),
	)
}
