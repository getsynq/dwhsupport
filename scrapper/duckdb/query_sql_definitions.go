package duckdb

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecduckdb "github.com/getsynq/dwhsupport/exec/duckdb"
	"github.com/getsynq/dwhsupport/scrapper"
	_ "github.com/lib/pq"
)

//go:embed query_sql_definitions.sql
var querySqlDefinitionsSql string

func (e *DuckDBScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	return dwhexecduckdb.NewQuerier[scrapper.SqlDefinitionRow](e.executor).QueryMany(ctx, querySqlDefinitionsSql,
		dwhexec.WithArgs[scrapper.SqlDefinitionRow](e.conf.Database),
		dwhexec.WithPostProcessors(func(row *scrapper.SqlDefinitionRow) (*scrapper.SqlDefinitionRow, error) {
			row.Database = e.conf.Database
			return row, nil
		}))
}
