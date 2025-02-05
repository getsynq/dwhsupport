package redshift

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_tables.sql
var tablesSql string

func (e *RedshiftScrapper) QueryTables(ctx context.Context) ([]*scrapper.TableRow, error) {

	return stdsql.QueryMany[scrapper.TableRow](ctx, e.executor.GetDb(), tablesSql,
		dwhexec.WithArgs[scrapper.TableRow](e.conf.Database),
		dwhexec.WithPostProcessors(func(row *scrapper.TableRow) (*scrapper.TableRow, error) {
			if row.Schema == "pg_automv" {
				return nil, nil
			}
			row.Database = e.conf.Database
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
