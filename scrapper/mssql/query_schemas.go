package mssql

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_schemas.sql
var querySchemasSql string

func (e *MSSQLScrapper) QuerySchemas(ctx context.Context) ([]*scrapper.SchemaRow, error) {
	sql := scope.AppendSchemaScopeConditions(ctx, querySchemasSql, "", "s.name")
	return dwhexecmssql.NewQuerier[scrapper.SchemaRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.SchemaRow) (*scrapper.SchemaRow, error) {
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
