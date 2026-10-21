package athena

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_tables.sql
var queryTablesSQL string

func (e *AthenaScrapper) QueryTables(ctx context.Context, opts ...scrapper.QueryTablesOption) ([]*scrapper.TableRow, error) {
	db := e.executor.GetDb()
	query := scope.AppendScopeConditions(ctx, queryTablesSQL, "t.table_catalog", "t.table_schema", "t.table_name")
	rows, err := stdsql.QueryMany(ctx, db, query,
		dwhexec.WithPostProcessors(func(row *scrapper.TableRow) (*scrapper.TableRow, error) {
			row.Instance = e.executor.Instance()
			return row, nil
		}),
	)
	if err != nil {
		return nil, err
	}
	comments, omissions, err := e.glueTableComments(ctx)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if comment, ok := comments[newGlueTableKey(row.Schema, row.Table)]; ok {
			row.Description = &comment
		}
	}
	return withOmittedComments(ctx, rows, omissions)
}
