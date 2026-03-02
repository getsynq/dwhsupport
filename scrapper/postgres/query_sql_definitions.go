package postgres

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecpostgres "github.com/getsynq/dwhsupport/exec/postgres"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	_ "github.com/lib/pq"
)

//go:embed query_sql_definitions.sql
var querySqlDefinitionsSql string

func (e *PostgresScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	sql := scope.AppendScopeConditions(ctx, querySqlDefinitionsSql, "", "table_schema", "table_name")
	return dwhexecpostgres.NewQuerier[scrapper.SqlDefinitionRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithArgs[scrapper.SqlDefinitionRow](e.conf.Database),
		dwhexec.WithPostProcessors(func(row *scrapper.SqlDefinitionRow) (*scrapper.SqlDefinitionRow, error) {
			row.Database = e.conf.Database
			row.Instance = e.conf.Host
			return row, nil
		}))
}
