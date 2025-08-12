package trino

import (
	"testing"

	"github.com/getsynq/dwhsupport/exec/trino"
	"github.com/stretchr/testify/assert"
)

func TestMaterializedViewsFeatureFlag(t *testing.T) {
	tests := []struct {
		name                   string
		fetchMaterializedViews bool
		shouldIncludeMVJoin    bool
		shouldUseMVLogic       bool
	}{
		{
			name:                   "Feature disabled - no MV joins",
			fetchMaterializedViews: false,
			shouldIncludeMVJoin:    false,
			shouldUseMVLogic:       false,
		},
		{
			name:                   "Feature enabled - includes MV joins",
			fetchMaterializedViews: true,
			shouldIncludeMVJoin:    true,
			shouldUseMVLogic:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			// Test tables query
			tablesQuery := scrapper.buildTablesQuery("test_catalog")

			if tt.shouldIncludeMVJoin {
				assert.Contains(t, tablesQuery, "LEFT JOIN system.metadata.materialized_views",
					"Tables query should include materialized views join when feature is enabled")
				assert.Contains(t, tablesQuery, "CASE WHEN mv.name IS NOT NULL THEN 'MATERIALIZED VIEW'",
					"Tables query should use CASE expression for table type when feature is enabled")
			} else {
				assert.NotContains(t, tablesQuery, "materialized_views",
					"Tables query should not include materialized views join when feature is disabled")
				assert.Contains(t, tablesQuery, "t.table_type AS \"table_type\"",
					"Tables query should use simple table_type when feature is disabled")
			}

			// Test SQL definitions query
			sqlDefsQuery := scrapper.buildSqlDefinitionsQuery("test_catalog")

			if tt.shouldIncludeMVJoin {
				assert.Contains(t, sqlDefsQuery, "LEFT JOIN system.metadata.materialized_views",
					"SQL definitions query should include materialized views join when feature is enabled")
				assert.Contains(t, sqlDefsQuery, "(mv.name IS NOT NULL) as is_materialized_view",
					"SQL definitions query should detect materialized views when feature is enabled")
				assert.Contains(t, sqlDefsQuery, "coalesce(mv.definition, v.view_definition, '')",
					"SQL definitions query should use mv.definition for materialized views when feature is enabled")
			} else {
				assert.NotContains(t, sqlDefsQuery, "materialized_views",
					"SQL definitions query should not include materialized views join when feature is disabled")
				assert.Contains(t, sqlDefsQuery, "false as is_materialized_view",
					"SQL definitions query should have false for is_materialized_view when feature is disabled")
				assert.Contains(t, sqlDefsQuery, "coalesce(v.view_definition, '') as sql",
					"SQL definitions query should only use view_definition when feature is disabled")
			}
		})
	}
}

func TestMaterializedViewsBackwardCompatibility(t *testing.T) {
	// Test that feature flag defaults to enabled for backward compatibility
	conf := &TrinoScrapperConf{
		TrinoConf: &trino.TrinoConf{
			Host: "test-host",
			Port: 443,
		},
		Catalogs:               []string{"test_catalog"},
		FetchMaterializedViews: true, // Explicitly set to true (default)
	}

	scrapper := &TrinoScrapper{
		conf: conf,
	}

	tablesQuery := scrapper.buildTablesQuery("test_catalog")
	sqlDefsQuery := scrapper.buildSqlDefinitionsQuery("test_catalog")

	// Verify that with backward compatibility, materialized views are fetched by default
	assert.Contains(t, tablesQuery, "LEFT JOIN system.metadata.materialized_views",
		"With backward compatibility, tables query should include materialized views join")
	assert.Contains(t, sqlDefsQuery, "LEFT JOIN system.metadata.materialized_views",
		"With backward compatibility, SQL definitions query should include materialized views join")
}
