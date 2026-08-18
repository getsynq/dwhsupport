package clickhouse

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_tables.sql
var queryTablesSql string

func (e *ClickhouseScrapper) QueryTables(ctx context.Context, opts ...scrapper.QueryTablesOption) ([]*scrapper.TableRow, error) {
	sql := scope.AppendScopeConditions(ctx, e.systemTablesSql(queryTablesSql), "", "tbls.database", "tbls.name")
	return dwhexecclickhouse.NewQuerier[scrapper.TableRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors[scrapper.TableRow](func(row *scrapper.TableRow) (*scrapper.TableRow, error) {
			row.Instance, row.Database = e.rowIdentity()
			return row, nil
		}),
	)
}
