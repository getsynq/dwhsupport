package mysql

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecmysql "github.com/getsynq/dwhsupport/exec/mysql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_catalog.sql
var queryCatalogSql string

func (e *MySQLScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.ColumnCatalogRow, error) {
	return dwhexecmysql.NewQuerier[scrapper.ColumnCatalogRow](e.executor).QueryMany(ctx, queryCatalogSql,
		dwhexec.WithPostProcessors(func(row *scrapper.ColumnCatalogRow) (*scrapper.ColumnCatalogRow, error) {
			row.Database = e.conf.Host
			return row, nil
		}),
	)
}
