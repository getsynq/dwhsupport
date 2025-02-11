package clickhouse

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_tables.sql
var queryTablesSql string

func (e *ClickhouseScrapper) QueryTables(ctx context.Context) ([]*scrapper.TableRow, error) {
	return dwhexecclickhouse.NewQuerier[scrapper.TableRow](e.executor).QueryMany(ctx, queryTablesSql,
		dwhexec.WithPostProcessors[scrapper.TableRow](func(row *scrapper.TableRow) (*scrapper.TableRow, error) {
			row.Database = e.conf.Hostname
			if len(e.conf.DatabaseName) > 0 {
				row.Database = e.conf.DatabaseName
			}
			return row, nil
		}),
	)
}
