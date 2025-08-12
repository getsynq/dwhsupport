package trino

import (
	"strings"
	"testing"

	"github.com/getsynq/dwhsupport/exec/trino"
	"github.com/stretchr/testify/assert"
)

func TestQueryBuilding(t *testing.T) {
	tests := []struct {
		name                   string
		fetchMaterializedViews bool
		expectedTablesQuery    string
		expectedSqlDefsQuery   string
	}{
		{
			name:                   "MaterializedViews Disabled",
			fetchMaterializedViews: false,
			expectedTablesQuery: `SELECT 
    t.table_catalog as database,
    t.table_schema as schema,
    t.table_name as "table",
    t.table_type AS "table_type",
    '' as description,
    t.table_type = 'BASE TABLE' as is_table,
    t.table_type = 'VIEW'  as is_view
FROM test_catalog.information_schema.tables t

WHERE t.table_schema NOT IN ('information_schema')`,
			expectedSqlDefsQuery: `with tables as (
    select
        table_catalog as database,
        table_schema as schema,
        table_name,
        table_type
    from test_catalog.information_schema.tables
    where table_schema not in ('information_schema')
)
select
    t.database,
    t.schema,
    t.table_name as "table",
    (t.table_type = 'VIEW' AND v.view_definition IS NOT NULL) as is_view,
    false as is_materialized_view,
    coalesce(v.view_definition, '') as sql
from tables t
left join test_catalog.information_schema.views v
    on t.schema = v.table_schema and t.table_name = v.table_name

order by t.database, t.schema, t.table_name`,
		},
		{
			name:                   "MaterializedViews Enabled",
			fetchMaterializedViews: true,
			expectedTablesQuery: `SELECT 
    t.table_catalog as database,
    t.table_schema as schema,
    t.table_name as "table",
    (CASE WHEN mv.name IS NOT NULL THEN 'MATERIALIZED VIEW' ELSE t.table_type END) AS "table_type",
    '' as description,
    (CASE WHEN mv.name IS NOT NULL THEN 'MATERIALIZED VIEW' ELSE t.table_type END) = 'BASE TABLE' as is_table,
    (CASE WHEN mv.name IS NOT NULL THEN 'MATERIALIZED VIEW' ELSE t.table_type END) = 'VIEW'  as is_view
FROM test_catalog.information_schema.tables t
LEFT JOIN system.metadata.materialized_views mv ON t.table_catalog = mv.catalog_name AND t.table_schema = mv.schema_name AND t.table_name = mv.name
WHERE t.table_schema NOT IN ('information_schema')`,
			expectedSqlDefsQuery: `with tables as (
    select
        table_catalog as database,
        table_schema as schema,
        table_name,
        table_type
    from test_catalog.information_schema.tables
    where table_schema not in ('information_schema')
)
select
    t.database,
    t.schema,
    t.table_name as "table",
    (t.table_type = 'VIEW' AND v.view_definition IS NOT NULL) as is_view,
    (mv.name IS NOT NULL) as is_materialized_view,
    coalesce(mv.definition, v.view_definition, '') as sql
from tables t
left join test_catalog.information_schema.views v
    on t.schema = v.table_schema and t.table_name = v.table_name
LEFT JOIN system.metadata.materialized_views mv ON t.database = mv.catalog_name AND t.schema = mv.schema_name AND t.table_name = mv.name
order by t.database, t.schema, t.table_name`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create scrapper configuration
			conf := &TrinoScrapperConf{
				TrinoConf: &trino.TrinoConf{
					Host: "test-host",
					Port: 443,
				},
				Catalogs:               []string{"test_catalog"},
				FetchMaterializedViews: tt.fetchMaterializedViews,
			}

			scrapper := &TrinoScrapper{
				conf: conf,
			}

			// Test tables query building
			actualTablesQuery := scrapper.buildTablesQuery("test_catalog")
			assert.Equal(t, tt.expectedTablesQuery, actualTablesQuery, "Tables query should match expected")

			// Test SQL definitions query building
			actualSqlDefsQuery := scrapper.buildSqlDefinitionsQuery("test_catalog")
			assert.Equal(t, tt.expectedSqlDefsQuery, actualSqlDefsQuery, "SQL definitions query should match expected")
		})
	}
}

// Helper method to build tables query for testing
func (e *TrinoScrapper) buildTablesQuery(catalogName string) string {
	query := queryTablesSQL
	catalogQuery := strings.Replace(query, "{{catalog}}", catalogName, -1)

	// Conditionally add materialized views JOIN based on feature flag
	if e.conf.FetchMaterializedViews {
		catalogQuery = strings.Replace(catalogQuery, "{{materialized_views_join}}",
			"LEFT JOIN system.metadata.materialized_views mv ON t.table_catalog = mv.catalog_name AND t.table_schema = mv.schema_name AND t.table_name = mv.name", -1)
		catalogQuery = strings.Replace(catalogQuery, "{{table_type_expression}}",
			"(CASE WHEN mv.name IS NOT NULL THEN 'MATERIALIZED VIEW' ELSE t.table_type END)", -1)
	} else {
		catalogQuery = strings.Replace(catalogQuery, "{{materialized_views_join}}", "", -1)
		catalogQuery = strings.Replace(catalogQuery, "{{table_type_expression}}", "t.table_type", -1)
	}

	return catalogQuery
}

// Helper method to build SQL definitions query for testing
func (e *TrinoScrapper) buildSqlDefinitionsQuery(catalogName string) string {
	query := querySqlDefinitionsSQL
	catalogQuery := strings.Replace(query, "{{catalog}}", catalogName, -1)

	// Conditionally add materialized views JOIN based on feature flag
	if e.conf.FetchMaterializedViews {
		catalogQuery = strings.Replace(catalogQuery, "{{materialized_views_join}}",
			"LEFT JOIN system.metadata.materialized_views mv ON t.database = mv.catalog_name AND t.schema = mv.schema_name AND t.table_name = mv.name", -1)
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
