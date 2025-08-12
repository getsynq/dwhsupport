package trino

import (
	"testing"

	"github.com/getsynq/dwhsupport/exec/trino"
	"github.com/gkampitakis/go-snaps/snaps"
)

func TestQuerySnapshotGeneration(t *testing.T) {
	tests := []struct {
		name                   string
		fetchMaterializedViews bool
		fetchTableComments     bool
	}{
		{
			name:                   "AllFeaturesDisabled",
			fetchMaterializedViews: false,
			fetchTableComments:     false,
		},
		{
			name:                   "OnlyTableComments",
			fetchMaterializedViews: false,
			fetchTableComments:     true,
		},
		{
			name:                   "OnlyMaterializedViews",
			fetchMaterializedViews: true,
			fetchTableComments:     false,
		},
		{
			name:                   "AllFeaturesEnabled",
			fetchMaterializedViews: true,
			fetchTableComments:     true,
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
				FetchTableComments:     tt.fetchTableComments,
			}

			scrapper := &TrinoScrapper{
				conf: conf,
			}

			// Test tables query
			tablesQuery := scrapper.buildTablesQuery("test_catalog")
			snaps.WithConfig(snaps.Filename("query_tables")).MatchSnapshot(t, tablesQuery)

			// Test SQL definitions query
			sqlDefsQuery := scrapper.buildSqlDefinitionsQuery("test_catalog")
			snaps.WithConfig(snaps.Filename("query_sql_definitions")).MatchSnapshot(t, sqlDefsQuery)
		})
	}
}
