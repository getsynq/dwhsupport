package oracle

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_table_constraints.sql
var queryTableConstraintsSql string

func (e *OracleScrapper) QueryTableConstraints(ctx context.Context, database string, schema string, table string) ([]*scrapper.TableConstraintRow, error) {
	return dwhexecoracle.NewQuerier[scrapper.TableConstraintRow](e.executor).QueryMany(ctx, queryTableConstraintsSql,
		dwhexec.WithArgs[scrapper.TableConstraintRow](schema, table),
		dwhexec.WithPostProcessors(func(row *scrapper.TableConstraintRow) (*scrapper.TableConstraintRow, error) {
			row.Database = e.conf.ServiceName
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
