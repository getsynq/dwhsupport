package athena

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_catalog.sql
var queryCatalogSQL string

func (e *AthenaScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	db := e.executor.GetDb()
	query := scope.AppendScopeConditions(ctx, queryCatalogSQL, "c.table_catalog", "c.table_schema", "c.table_name")
	rows, err := stdsql.QueryMany(ctx, db, query,
		dwhexec.WithPostProcessors(func(row *scrapper.CatalogColumnRow) (*scrapper.CatalogColumnRow, error) {
			row.Instance = e.executor.Instance()
			return row.ResolveNumeric(e.DialectType()), nil
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
			row.TableComment = &comment
		}
	}
	return withOmittedComments(ctx, rows, omissions)
}
