package postgres

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_table_constraints.sql
var queryTableConstraintsSql string

func (e *PostgresScrapper) QueryTableConstraints(ctx context.Context, database string, schema string, table string) ([]*scrapper.TableConstraintRow, error) {
	return stdsql.QueryMany[scrapper.TableConstraintRow](ctx, e.executor.GetDb(), queryTableConstraintsSql,
		dwhexec.WithArgs[scrapper.TableConstraintRow](schema, table),
		dwhexec.WithPostProcessors(func(row *scrapper.TableConstraintRow) (*scrapper.TableConstraintRow, error) {
			row.Database = e.conf.Database
			row.Instance = e.conf.Host
			if row.ConstraintType == "UNIQUE" {
				row.ConstraintType = scrapper.ConstraintTypeUniqueIndex
			}
			return row, nil
		}),
	)
}
