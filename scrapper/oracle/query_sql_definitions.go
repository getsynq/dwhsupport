package oracle

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_sql_definitions.sql
var querySqlDefinitionsSql string

func (e *OracleScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	sql := scope.AppendScopeConditions(ctx, querySqlDefinitionsSql, "", "v.OWNER", "v.VIEW_NAME")
	return dwhexecoracle.NewQuerier[scrapper.SqlDefinitionRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.SqlDefinitionRow) (*scrapper.SqlDefinitionRow, error) {
			row.Database = e.conf.ServiceName
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
