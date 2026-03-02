package duckdb

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecduckdb "github.com/getsynq/dwhsupport/exec/duckdb"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_sql_definitions.sql
var querySqlDefinitionsSql string

func (e *DuckDBScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	sql := scope.AppendScopeConditions(ctx, querySqlDefinitionsSql, "r.database_name", "r.schema_name", "r.table_name")
	return dwhexecduckdb.NewQuerier[scrapper.SqlDefinitionRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.SqlDefinitionRow) (*scrapper.SqlDefinitionRow, error) {
			row.Instance = e.conf.MotherduckAccount
			return row, nil
		}))
}
