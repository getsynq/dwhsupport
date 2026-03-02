package clickhouse

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_table_constraints.sql
var queryTableConstraintsSql string

func (e *ClickhouseScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	sql := scope.AppendScopeConditions(ctx, queryTableConstraintsSql, "", "schema", "table")
	return dwhexecclickhouse.NewQuerier[scrapper.TableConstraintRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors[scrapper.TableConstraintRow](func(row *scrapper.TableConstraintRow) (*scrapper.TableConstraintRow, error) {
			row.Database = e.conf.Hostname
			if len(e.conf.DatabaseName) > 0 {
				row.Database = e.conf.DatabaseName
			}
			return row, nil
		}),
	)
}
