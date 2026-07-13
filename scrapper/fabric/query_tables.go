package fabric

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_tables.sql
var queryTablesSql string

func (e *FabricScrapper) QueryTables(ctx context.Context, opts ...scrapper.QueryTablesOption) ([]*scrapper.TableRow, error) {
	sql := scope.AppendScopeConditions(ctx, queryTablesSql, "", "s.name", "o.name")
	return dwhexecfabric.NewQuerier[scrapper.TableRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.TableRow) (*scrapper.TableRow, error) {
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
