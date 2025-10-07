package trino

import (
	"strings"
)

// NOTE: Query building validation is now handled by TestQuerySnapshotGeneration
// using snapshot testing which is more maintainable than hardcoded string comparisons

// Helper method to build tables query for testing
func (e *TrinoScrapper) buildTablesQuery(catalogName string) string {
	query := queryTablesSQL
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

	return catalogQuery
}

// Helper method to build SQL definitions query for testing
func (e *TrinoScrapper) buildSqlDefinitionsQuery(catalogName string) string {
	query := querySqlDefinitionsSQL
	catalogQuery := strings.Replace(query, "{{catalog}}", catalogName, -1)

	// Conditionally add materialized views JOIN based on feature flag
	if e.conf.FetchMaterializedViews {
		catalogQuery = strings.Replace(
			catalogQuery,
			"{{materialized_views_join}}",
			"LEFT JOIN system.metadata.materialized_views mv ON t.database = mv.catalog_name AND t.schema = mv.schema_name AND t.table_name = mv.name",
			-1,
		)
		catalogQuery = strings.Replace(catalogQuery, "{{is_materialized_view_expression}}",
			"(mv.name IS NOT NULL)", -1)
		catalogQuery = strings.Replace(catalogQuery, "{{sql_expression}}",
			"coalesce(mv.definition, v.view_definition, '')", -1)
	} else {
		catalogQuery = strings.Replace(catalogQuery, "{{materialized_views_join}}", "", -1)
		catalogQuery = strings.Replace(catalogQuery, "{{is_materialized_view_expression}}", "false", -1)
		catalogQuery = strings.Replace(catalogQuery, "{{sql_expression}}",
			"coalesce(v.view_definition, '')", -1)
	}

	return catalogQuery
}
