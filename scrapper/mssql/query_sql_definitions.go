package mssql

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_sql_definitions.sql
var querySqlDefinitionsSql string

func (e *MSSQLScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	return dwhexecmssql.NewQuerier[scrapper.SqlDefinitionRow](e.executor).QueryMany(ctx, querySqlDefinitionsSql,
		dwhexec.WithPostProcessors(func(row *scrapper.SqlDefinitionRow) (*scrapper.SqlDefinitionRow, error) {
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
