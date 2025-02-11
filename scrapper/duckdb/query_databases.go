package duckdb

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
	_ "github.com/marcboeker/go-duckdb"
)

//go:embed query_databases.sql
var queryDatabasesSql string

func (e *DuckDBScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	return stdsql.QueryMany[scrapper.DatabaseRow](ctx, e.executor.GetDb(), queryDatabasesSql,
		dwhexec.WithPostProcessors(func(row *scrapper.DatabaseRow) (*scrapper.DatabaseRow, error) {
			row.Instance = e.conf.MotherduckAccount
			return row, nil
		}),
	)
}
