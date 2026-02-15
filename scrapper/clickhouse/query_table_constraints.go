package clickhouse

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_table_constraints.sql
var queryTableConstraintsSql string

func (e *ClickhouseScrapper) QueryTableConstraints(ctx context.Context, database string, schema string, table string) ([]*scrapper.TableConstraintRow, error) {
	return dwhexecclickhouse.NewQuerier[scrapper.TableConstraintRow](e.executor).QueryMany(ctx, queryTableConstraintsSql,
		dwhexec.WithArgs[scrapper.TableConstraintRow](schema, table),
		dwhexec.WithPostProcessors[scrapper.TableConstraintRow](func(row *scrapper.TableConstraintRow) (*scrapper.TableConstraintRow, error) {
			row.Database = e.conf.Hostname
			if len(e.conf.DatabaseName) > 0 {
				row.Database = e.conf.DatabaseName
			}
			return row, nil
		}),
	)
}
