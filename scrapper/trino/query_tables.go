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

//go:embed query_tables.sql
var queryTablesSQL string

func (e *TrinoScrapper) QueryTables(ctx context.Context, opts ...scrapper.QueryTablesOption) ([]*scrapper.TableRow, error) {
	acceptedCatalogs, err := e.acceptedCatalogs(ctx)
	if err != nil {
		return nil, err
	}
	return readCatalogs(
		ctx,
		acceptedCatalogs,
		scrapper.OmittedObjects,
		func(ctx context.Context, catalog *TrinoCatalog) ([]*scrapper.TableRow, []scrapper.Omission, error) {
			return e.queryTables(ctx, catalog.CatalogName)
		},
	)
}

func (e *TrinoScrapper) queryTables(ctx context.Context, catalogName string) ([]*scrapper.TableRow, []scrapper.Omission, error) {
	var res []*scrapper.TableRow
	omissions, err := e.withoutCommentsOnFailure(ctx, catalogName, func(withComments bool) error {
		rows, err := e.queryTablesWith(ctx, catalogName, withComments)
		res = rows
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	return res, omissions, nil
}

func (e *TrinoScrapper) queryTablesWith(ctx context.Context, catalogName string, withComments bool) ([]*scrapper.TableRow, error) {
	query := queryTablesSQL
	db := e.executor.GetDb()

	catalogQuery := strings.Replace(query, "{{catalog}}", catalogName, -1)

	// Conditionally add table comments JOIN based on feature flag
	if withComments {
		catalogQuery = strings.Replace(
			catalogQuery,
			"{{table_comments_join}}",
			"LEFT JOIN system.metadata.table_comments c\n  ON t.table_catalog = c.catalog_name\n  AND t.table_schema = c.schema_name\n  AND t.table_name = c.table_name",
			-1,
		)
		catalogQuery = strings.Replace(catalogQuery, "{{table_comment_expression}}", "c.comment", -1)
	} else {
		catalogQuery = strings.Replace(catalogQuery, "{{table_comments_join}}", "", -1)
		catalogQuery = strings.Replace(catalogQuery, "{{table_comment_expression}}", noCommentExpression(e.conf.FetchTableComments), -1)
	}

	// Conditionally add materialized views JOIN based on feature flag
	if e.conf.FetchMaterializedViews {
		catalogQuery = strings.Replace(
			catalogQuery,
			"{{materialized_views_join}}",
			"LEFT JOIN system.metadata.materialized_views mv\n  ON t.table_catalog = mv.catalog_name\n    AND t.table_schema = mv.schema_name\n    AND t.table_name = mv.name",
			-1,
		)
		catalogQuery = strings.Replace(catalogQuery, "{{table_type_expression}}",
			"(CASE WHEN mv.name IS NOT NULL THEN 'MATERIALIZED VIEW'\n    ELSE t.table_type END)", -1)
		catalogQuery = strings.Replace(catalogQuery, "{{is_table_expression}}",
			"mv.name is null AND t.table_type = 'BASE TABLE'", -1)
		catalogQuery = strings.Replace(catalogQuery, "{{is_view_expression}}",
			"mv.name is not null OR t.table_type = 'VIEW'", -1)
	} else {
		catalogQuery = strings.Replace(catalogQuery, "{{materialized_views_join}}", "", -1)
		catalogQuery = strings.Replace(catalogQuery, "{{table_type_expression}}", "t.table_type", -1)
		catalogQuery = strings.Replace(catalogQuery, "{{is_table_expression}}", "t.table_type = 'BASE TABLE'", -1)
		catalogQuery = strings.Replace(catalogQuery, "{{is_view_expression}}", "t.table_type = 'VIEW'", -1)
	}

	catalogQuery = scope.AppendScopeConditions(ctx, catalogQuery, "", "t.table_schema", "t.table_name")
	res, err := stdsql.QueryMany(ctx, db, catalogQuery,
		dwhexec.WithPostProcessors(func(row *scrapper.TableRow) (*scrapper.TableRow, error) {
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
	if err != nil {
		return nil, err
	}
	return res, nil
}
