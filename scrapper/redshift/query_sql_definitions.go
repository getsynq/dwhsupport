package redshift

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_sql_definitions.sql
var querySqlDefinitionsSql string

func (e *RedshiftScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	sql := scope.AppendScopeConditions(ctx, querySqlDefinitionsSql, "", "table_schema", "table_name")
	return stdsql.QueryMany[scrapper.SqlDefinitionRow](ctx, e.executor.GetDb(), sql,
		dwhexec.WithArgs[scrapper.SqlDefinitionRow](e.conf.Database),
		dwhexec.WithPostProcessors(func(row *scrapper.SqlDefinitionRow) (*scrapper.SqlDefinitionRow, error) {
			if row.Schema == "pg_automv" {
				return nil, nil
			}
			row.Database = e.conf.Database
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
