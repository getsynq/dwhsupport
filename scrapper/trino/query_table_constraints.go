package trino

import (
	"context"
	_ "embed"
	"strings"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_table_constraints.sql
var queryTableConstraintsSql string

func (e *TrinoScrapper) QueryTableConstraints(
	ctx context.Context,
	database string,
	schema string,
	table string,
) ([]*scrapper.TableConstraintRow, error) {
	query := strings.ReplaceAll(queryTableConstraintsSql, "{{catalog}}", database)

	return stdsql.QueryMany[scrapper.TableConstraintRow](ctx, e.executor.GetDb(), query,
		dwhexec.WithArgs[scrapper.TableConstraintRow](schema, table),
		dwhexec.WithPostProcessors(func(row *scrapper.TableConstraintRow) (*scrapper.TableConstraintRow, error) {
			row.Instance = e.conf.Host
			if row.ConstraintType == "UNIQUE" {
				row.ConstraintType = scrapper.ConstraintTypeUniqueIndex
			}
			return row, nil
		}),
	)
}
