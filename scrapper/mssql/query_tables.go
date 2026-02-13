package mssql

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_tables.sql
var queryTablesSql string

func (e *MSSQLScrapper) QueryTables(ctx context.Context) ([]*scrapper.TableRow, error) {
	return dwhexecmssql.NewQuerier[scrapper.TableRow](e.executor).QueryMany(ctx, queryTablesSql,
		dwhexec.WithPostProcessors(func(row *scrapper.TableRow) (*scrapper.TableRow, error) {
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
