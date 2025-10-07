package trino

import (
	"context"
	_ "embed"
	"strings"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_tables.sql
var queryTablesSQL string

func (e *TrinoScrapper) QueryTables(ctx context.Context) ([]*scrapper.TableRow, error) {
	var out []*scrapper.TableRow

	availableCatalogs, err := e.allAvailableCatalogs.Get()
	if err != nil {
		return nil, err
	}

	for _, catalog := range availableCatalogs {
		if !catalog.IsAccepted {
			continue
		}
		res, err := e.queryTables(ctx, catalog.CatalogName)
		if err != nil {
			return nil, err
		}
		out = append(out, res...)
	}
	return out, nil
}

func (e *TrinoScrapper) queryTables(ctx context.Context, catalogName string) ([]*scrapper.TableRow, error) {
	query := queryTablesSQL
	db := e.executor.GetDb()

	catalogQuery := strings.Replace(query, "{{catalog}}", catalogName, -1)

	// Conditionally add table comments JOIN based on feature flag
	if e.conf.FetchTableComments {
		catalogQuery = strings.Replace(
			catalogQuery,
			"{{table_comments_join}}",
			"LEFT JOIN system.metadata.table_comments c\n  ON t.table_catalog = c.catalog_name\n  AND t.table_schema = c.schema_name\n  AND t.table_name = c.table_name",
			-1,
		)
		catalogQuery = strings.Replace(catalogQuery, "{{table_comment_expression}}", "c.comment", -1)
	} else {
		catalogQuery = strings.Replace(catalogQuery, "{{table_comments_join}}", "", -1)
		catalogQuery = strings.Replace(catalogQuery, "{{table_comment_expression}}", "''", -1)
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
