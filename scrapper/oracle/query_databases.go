package oracle

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_databases.sql
var queryDatabasesSql string

func (e *OracleScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	return dwhexecoracle.NewQuerier[scrapper.DatabaseRow](e.executor).QueryMany(ctx, queryDatabasesSql,
		dwhexec.WithPostProcessors(func(row *scrapper.DatabaseRow) (*scrapper.DatabaseRow, error) {
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
