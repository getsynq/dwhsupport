package db2

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecdb2 "github.com/getsynq/dwhsupport/exec/db2"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_databases.sql
var queryDatabasesSql string

func (e *Db2Scrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	return dwhexecdb2.NewQuerier[scrapper.DatabaseRow](e.executor).QueryMany(ctx, queryDatabasesSql,
		dwhexec.WithPostProcessors(func(row *scrapper.DatabaseRow) (*scrapper.DatabaseRow, error) {
			row.Instance = e.conf.Hostname
			return row, nil
		}),
	)
}
