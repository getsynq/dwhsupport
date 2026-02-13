package mssql

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_databases.sql
var queryDatabasesSql string

func (e *MSSQLScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	return dwhexecmssql.NewQuerier[scrapper.DatabaseRow](e.executor).QueryMany(ctx, queryDatabasesSql,
		dwhexec.WithPostProcessors(func(row *scrapper.DatabaseRow) (*scrapper.DatabaseRow, error) {
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
