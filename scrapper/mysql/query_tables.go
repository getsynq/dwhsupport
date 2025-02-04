package mysql

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecmysql "github.com/getsynq/dwhsupport/exec/mysql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_tables.sql
var queryTablesSql string

func (e *MySQLScrapper) QueryTables(ctx context.Context) ([]*scrapper.TableRow, error) {
	return dwhexecmysql.NewQuerier[scrapper.TableRow](e.executor).QueryMany(ctx, queryTablesSql,
		dwhexec.WithPostProcessors(
			func(row *scrapper.TableRow) (*scrapper.TableRow, error) {
				row.Database = e.conf.Host
				return row, nil
			},
		),
	)
}
