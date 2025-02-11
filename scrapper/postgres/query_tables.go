package postgres

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecpostgres "github.com/getsynq/dwhsupport/exec/postgres"
	"github.com/getsynq/dwhsupport/scrapper"
	_ "github.com/lib/pq"
)

//go:embed query_tables.sql
var queryTablesSql string

func (e *PostgresScrapper) QueryTables(ctx context.Context) ([]*scrapper.TableRow, error) {
	return dwhexecpostgres.NewQuerier[scrapper.TableRow](e.executor).QueryMany(ctx, queryTablesSql,
		dwhexec.WithPostProcessors(func(row *scrapper.TableRow) (*scrapper.TableRow, error) {
			row.Database = e.conf.Database
			row.Instance = e.conf.Host
			return row, nil
		}))
}
