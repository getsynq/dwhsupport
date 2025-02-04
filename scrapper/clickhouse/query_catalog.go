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

func (e *ClickhouseScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.ColumnCatalogRow, error) {

	return dwhexecclickhouse.NewQuerier[scrapper.ColumnCatalogRow](e.executor).QueryMany(ctx, queryCatalogSql,
		dwhexec.WithPostProcessors[scrapper.ColumnCatalogRow](func(row *scrapper.ColumnCatalogRow) (*scrapper.ColumnCatalogRow, error) {
			row.Database = e.conf.DatabaseName
			return row, nil
		}),
	)
}
