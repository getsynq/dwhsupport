package bigquery

import (
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

// Every table, snapshot and clone of the seed gets its DDL, whether the role
// may read the region's INFORMATION_SCHEMA.TABLES or was granted only on the
// datasets. The dwhtesting service account is the second kind (dataViewer on
// each dataset): point BIGQUERY_CREDENTIALS_FILE at its key to run this against
// that case.
func TestBigQuerySqlDefinitionsOfEveryTableKind(t *testing.T) {
	skipIfNoBigQuery(t)
	ctx := t.Context()
	sc, err := newBigQueryScrapperFromEnv(ctx)
	require.NoError(t, err)
	defer sc.Close()

	defs, err := sc.QuerySqlDefinitions(ctx)
	require.NoError(t, err)
	dataset := testenv.EnvOrDefault("BIGQUERY_DATASET_A", defaultDatasetA)
	byName := lo.KeyBy(
		lo.Filter(defs, func(d *scrapper.SqlDefinitionRow, _ int) bool { return d.Schema == dataset }),
		func(d *scrapper.SqlDefinitionRow) string { return d.Table },
	)

	for name, ddl := range map[string]string{
		"products":           "CREATE TABLE",
		"customers":          "CREATE TABLE",
		"products_snapshot":  "CREATE SNAPSHOT TABLE",
		"products_clone":     "CREATE TABLE",
		"active_products":    "CREATE VIEW",
		"product_revenue_mv": "CREATE MATERIALIZED VIEW",
	} {
		def, ok := byName[name]
		require.True(t, ok, "%s has no definition", name)
		require.Contains(t, def.Sql, ddl, name)
	}
	require.True(t, byName["active_products"].IsView)
	require.False(t, byName["products"].IsView)
}
