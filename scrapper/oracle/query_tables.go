package oracle

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_tables.sql
var queryTablesSql string

func (e *OracleScrapper) QueryTables(ctx context.Context) ([]*scrapper.TableRow, error) {
	sql := scope.AppendScopeConditions(ctx, queryTablesSql, "", "o.OWNER", "o.OBJECT_NAME")
	return dwhexecoracle.NewQuerier[scrapper.TableRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.TableRow) (*scrapper.TableRow, error) {
			row.Database = e.conf.ServiceName
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
