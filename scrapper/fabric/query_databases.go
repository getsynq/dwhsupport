package fabric

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_databases.sql
var queryDatabasesSql string

func (e *FabricScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	return dwhexecfabric.NewQuerier[scrapper.DatabaseRow](e.executor).QueryMany(ctx, queryDatabasesSql,
		dwhexec.WithPostProcessors(func(row *scrapper.DatabaseRow) (*scrapper.DatabaseRow, error) {
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
