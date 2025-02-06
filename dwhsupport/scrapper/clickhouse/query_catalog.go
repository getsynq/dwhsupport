package clickhouse

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_catalog.sql
var queryCatalogSql string

func (e *ClickhouseScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {

	return dwhexecclickhouse.NewQuerier[scrapper.CatalogColumnRow](e.executor).QueryMany(ctx, queryCatalogSql,
		dwhexec.WithPostProcessors[scrapper.CatalogColumnRow](func(row *scrapper.CatalogColumnRow) (*scrapper.CatalogColumnRow, error) {
			row.Database = e.conf.Hostname
			if len(e.conf.DatabaseName) > 0 {
				row.Database = e.conf.DatabaseName
			}
			return row, nil
		}),
	)
}
