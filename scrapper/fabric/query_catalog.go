package fabric

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_catalog.sql
var queryCatalogSql string

func (e *FabricScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	sql := scope.AppendScopeConditions(ctx, queryCatalogSql, "", "s.name", "t.name")
	return dwhexecfabric.NewQuerier[scrapper.CatalogColumnRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.CatalogColumnRow) (*scrapper.CatalogColumnRow, error) {
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
