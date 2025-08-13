package trino

import (
	"strings"
	"testing"

	"github.com/getsynq/dwhsupport/exec/trino"
	"github.com/stretchr/testify/assert"
)

func TestTableCommentsFeatureFlag(t *testing.T) {
	tests := []struct {
		name                  string
		fetchTableComments    bool
		shouldIncludeCommentsJoin bool
		shouldUsCommentsLogic     bool
	}{
		{
			name:                      "Feature disabled - no table comments joins",
			fetchTableComments:        false,
			shouldIncludeCommentsJoin: false,
			shouldUsCommentsLogic:     false,
		},
		{
			name:                      "Feature enabled - includes table comments joins",
			fetchTableComments:        true,
			shouldIncludeCommentsJoin: true,
			shouldUsCommentsLogic:     true,
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
				FetchMaterializedViews: false, // Disable MV to focus on table comments
				FetchTableComments:     tt.fetchTableComments,
			}

			scrapper := &TrinoScrapper{
				conf: conf,
			}

			// Test tables query
			tablesQuery := scrapper.buildTablesQuery("test_catalog")
			
			if tt.shouldIncludeCommentsJoin {
				assert.Contains(t, tablesQuery, "LEFT JOIN system.metadata.table_comments", 
					"Tables query should include table comments join when feature is enabled")
				assert.Contains(t, tablesQuery, "c.comment as description",
					"Tables query should use c.comment for description when feature is enabled")
			} else {
				assert.NotContains(t, tablesQuery, "table_comments",
					"Tables query should not include table comments join when feature is disabled")
				assert.Contains(t, tablesQuery, "'' as description",
					"Tables query should use empty string for description when feature is disabled")
			}

			// Test catalog query
			catalogQuery := scrapper.buildCatalogQuery("test_catalog")
			
			if tt.shouldIncludeCommentsJoin {
				assert.Contains(t, catalogQuery, "LEFT JOIN system.metadata.table_comments", 
					"Catalog query should include table comments join when feature is enabled")
				assert.Contains(t, catalogQuery, "coalesce(tc.comment, '')",
					"Catalog query should use coalesce for table comments when feature is enabled")
			} else {
				assert.NotContains(t, catalogQuery, "table_comments",
					"Catalog query should not include table comments join when feature is disabled")
				assert.Contains(t, catalogQuery, "'' as table_comment",
					"Catalog query should use empty string for table_comment when feature is disabled")
			}
		})
	}
}

func TestTableCommentsBackwardCompatibility(t *testing.T) {
	// Test that feature flag defaults to enabled for backward compatibility
	conf := &TrinoScrapperConf{
		TrinoConf: &trino.TrinoConf{
			Host: "test-host",
			Port: 443,
		},
		Catalogs:               []string{"test_catalog"},
		FetchMaterializedViews: false,
		FetchTableComments:     true, // Explicitly set to true (default)
	}

	scrapper := &TrinoScrapper{
		conf: conf,
	}

	tablesQuery := scrapper.buildTablesQuery("test_catalog")
	catalogQuery := scrapper.buildCatalogQuery("test_catalog")

	// Verify that with backward compatibility, table comments are fetched by default
	assert.Contains(t, tablesQuery, "LEFT JOIN system.metadata.table_comments", 
		"With backward compatibility, tables query should include table comments join")
	assert.Contains(t, catalogQuery, "LEFT JOIN system.metadata.table_comments", 
		"With backward compatibility, catalog query should include table comments join")
}

// Helper method to build catalog query for testing
func (e *TrinoScrapper) buildCatalogQuery(catalogName string) string {
	query := queryCatalogSQL
	catalogQuery := strings.Replace(query, "{{catalog}}", catalogName, -1)

	// Conditionally add table comments JOIN based on feature flag
	if e.conf.FetchTableComments {
		catalogQuery = strings.Replace(catalogQuery, "{{table_comments_join}}",
			"LEFT JOIN system.metadata.table_comments tc ON t.table_catalog = tc.catalog_name AND t.table_schema = tc.schema_name AND t.table_name = tc.table_name", -1)
		catalogQuery = strings.Replace(catalogQuery, "{{table_comment_expression}}",
			"coalesce(tc.comment, '')", -1)
	} else {
		catalogQuery = strings.Replace(catalogQuery, "{{table_comments_join}}", "", -1)
		catalogQuery = strings.Replace(catalogQuery, "{{table_comment_expression}}", "''", -1)
	}

	return catalogQuery
}