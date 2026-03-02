package oracle

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_table_constraints.sql
var queryTableConstraintsSql string

func (e *OracleScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	sql := scope.AppendScopeConditions(ctx, queryTableConstraintsSql, "", "cc.owner", "cc.table_name")
	return dwhexecoracle.NewQuerier[scrapper.TableConstraintRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.TableConstraintRow) (*scrapper.TableConstraintRow, error) {
			row.Database = e.conf.ServiceName
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
