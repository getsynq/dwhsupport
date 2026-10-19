package trino

import (
	"context"
	_ "embed"
	"strings"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_catalog.sql
var queryCatalogSQL string

func (e *TrinoScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	acceptedCatalogs, err := e.acceptedCatalogs(ctx)
	if err != nil {
		return nil, err
	}
	return readCatalogs(ctx, acceptedCatalogs, scrapper.OmittedColumns, e.queryCatalogColumns)
}

func (e *TrinoScrapper) queryCatalogColumns(
	ctx context.Context,
	catalog *TrinoCatalog,
) ([]*scrapper.CatalogColumnRow, []scrapper.Omission, error) {
	var rows []*scrapper.CatalogColumnRow
	omissions, err := e.withoutCommentsOnFailure(ctx, catalog.CatalogName, func(withComments bool) error {
		res, err := e.queryCatalogColumnsWith(ctx, catalog.CatalogName, withComments)
		rows = res
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	return rows, omissions, nil
}

func (e *TrinoScrapper) queryCatalogColumnsWith(
	ctx context.Context,
	catalogName string,
	withComments bool,
) ([]*scrapper.CatalogColumnRow, error) {
	query := strings.Replace(queryCatalogSQL, "{{catalog}}", catalogName, -1)

	// Conditionally add table comments JOIN based on feature flag
	if withComments {
		query = strings.Replace(
			query,
			"{{table_comments_join}}",
			"LEFT JOIN system.metadata.table_comments tc ON t.table_catalog = tc.catalog_name AND t.table_schema = tc.schema_name AND t.table_name = tc.table_name",
			-1,
		)
		query = strings.Replace(query, "{{table_comment_expression}}",
			"coalesce(tc.comment, '')", -1)
	} else {
		query = strings.Replace(query, "{{table_comments_join}}", "", -1)
		query = strings.Replace(query, "{{table_comment_expression}}", noCommentExpression(e.conf.FetchTableComments), -1)
	}
	query = scope.AppendScopeConditions(ctx, query, "", "t.table_schema", "t.table_name")
	return stdsql.QueryMany(ctx, e.executor.GetDb(), query,
		dwhexec.WithPostProcessors(func(row *scrapper.CatalogColumnRow) (*scrapper.CatalogColumnRow, error) {
			row.Instance = e.conf.Host
			return row.ResolveNumeric(e.DialectType()), nil
		}),
	)
}
