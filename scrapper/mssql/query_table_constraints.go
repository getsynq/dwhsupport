package mssql

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_table_constraints.sql
var queryTableConstraintsSql string

func (e *MSSQLScrapper) QueryTableConstraints(ctx context.Context, database string, schema string, table string) ([]*scrapper.TableConstraintRow, error) {
	return dwhexecmssql.NewQuerier[scrapper.TableConstraintRow](e.executor).QueryMany(ctx, queryTableConstraintsSql,
		dwhexec.WithArgs[scrapper.TableConstraintRow](schema, table),
		dwhexec.WithPostProcessors(func(row *scrapper.TableConstraintRow) (*scrapper.TableConstraintRow, error) {
			row.Database = e.conf.Database
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
