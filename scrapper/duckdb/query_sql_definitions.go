package duckdb

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecduckdb "github.com/getsynq/dwhsupport/exec/duckdb"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_sql_definitions.sql
var querySqlDefinitionsSql string

func (e *DuckDBScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	return dwhexecduckdb.NewQuerier[scrapper.SqlDefinitionRow](e.executor).QueryMany(ctx, querySqlDefinitionsSql,
		dwhexec.WithPostProcessors(func(row *scrapper.SqlDefinitionRow) (*scrapper.SqlDefinitionRow, error) {
			row.Instance = e.conf.MotherduckAccount
			return row, nil
		}))
}
