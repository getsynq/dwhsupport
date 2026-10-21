package bigquery

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

// definitionsByName keys the rows by dataset.table.
func definitionsByName(rows []*scrapper.SqlDefinitionRow) map[string]*scrapper.SqlDefinitionRow {
	return lo.KeyBy(rows, func(r *scrapper.SqlDefinitionRow) string { return r.Schema + "." + r.Table })
}

// A role granted only on the datasets may not read the region's
// INFORMATION_SCHEMA.TABLES, but may read each dataset's, which has the DDL of
// every table, snapshot and clone. The tables API it used to fall back to has
// only the queries of views.
func TestSqlDefinitionsReadEachDatasetWhenTheRegionIsRefused(t *testing.T) {
	fake := &fakeMetadataServer{answersQueries: true, datasets: []fakeDataset{
		salesDataset(),
		{id: "crm", tables: []fakeTable{fakeBaseTable("customers")}},
		{id: "empty"},
	}}
	bq := newFakeMetadataScrapperOn(t, fake)

	rows, err := bq.QuerySqlDefinitions(context.Background())
	require.NoError(t, err)
	byName := definitionsByName(rows)
	require.ElementsMatch(t, []string{"sales.orders", "sales.orders_view", "crm.customers"}, lo.Keys(byName))

	require.Equal(t, "CREATE TABLE `test-project.sales.orders` (id INT64);", byName["sales.orders"].Sql)
	require.False(t, byName["sales.orders"].IsView)
	require.Equal(t, fakeProject, byName["sales.orders"].Database)
	require.True(t, byName["sales.orders_view"].IsView)
	require.Contains(t, byName["crm.customers"].Sql, "CREATE TABLE")

	queried := fake.queried()
	require.Len(t, queried, 4, "the region, then each dataset once: %v", queried)
	require.Contains(t, queried[0], "region-")
}

// A dataset that refuses even its own INFORMATION_SCHEMA.TABLES is read through
// the tables API, which gives only its views' queries. The tables' DDL is then
// missing, and a caller that accepts incomplete results is told so, so it does
// not read their missing DDL as removed.
func TestSqlDefinitionsFallBackToTheTablesApiForADatasetThatRefusesItsQuery(t *testing.T) {
	restricted := fakeDataset{
		id:                "restricted",
		tablesQueryStatus: http.StatusForbidden,
		tables:            []fakeTable{fakeBaseTable("ledger"), fakeView("ledger_view")},
	}

	t.Run("incomplete results accepted", func(t *testing.T) {
		bq := newFakeMetadataScrapperOn(t, &fakeMetadataServer{answersQueries: true, datasets: []fakeDataset{salesDataset(), restricted}})
		rows, err := bq.QuerySqlDefinitions(scrapper.WithIncompleteResults(context.Background()))

		require.ElementsMatch(t, []string{"sales.orders", "sales.orders_view", "restricted.ledger_view"}, lo.Keys(definitionsByName(rows)))
		require.Equal(t, []string{"definitions test-project.restricted"}, omissionScopes(err))
		incomplete, _ := scrapper.AsIncomplete(err)
		require.Contains(t, incomplete.Omissions[0].Err.Error(), "INFORMATION_SCHEMA.TABLES")
	})

	t.Run("incomplete results not accepted", func(t *testing.T) {
		bq := newFakeMetadataScrapperOn(t, &fakeMetadataServer{answersQueries: true, datasets: []fakeDataset{salesDataset(), restricted}})
		rows, err := bq.QuerySqlDefinitions(context.Background())

		require.NoError(t, err)
		require.ElementsMatch(t, []string{"sales.orders", "sales.orders_view", "restricted.ledger_view"}, lo.Keys(definitionsByName(rows)))
	})
}

// A dataset whose query fails for any other reason than a refusal is left out
// whole when the caller accepts incomplete results, and fails the call when it
// does not, like a dataset whose table listing fails.
func TestSqlDefinitionsOfADatasetWhoseQueryFails(t *testing.T) {
	broken := fakeDataset{id: "broken", tablesQueryStatus: http.StatusBadRequest, tables: []fakeTable{fakeBaseTable("t")}}

	t.Run("incomplete results accepted", func(t *testing.T) {
		bq := newFakeMetadataScrapperOn(t, &fakeMetadataServer{answersQueries: true, datasets: []fakeDataset{salesDataset(), broken}})
		rows, err := bq.QuerySqlDefinitions(scrapper.WithIncompleteResults(context.Background()))

		require.ElementsMatch(t, []string{"sales.orders", "sales.orders_view"}, lo.Keys(definitionsByName(rows)))
		require.Equal(t, []string{"definitions test-project.broken"}, omissionScopes(err))
	})

	t.Run("incomplete results not accepted", func(t *testing.T) {
		bq := newFakeMetadataScrapperOn(t, &fakeMetadataServer{answersQueries: true, datasets: []fakeDataset{salesDataset(), broken}})
		_, err := bq.QuerySqlDefinitions(context.Background())

		require.Error(t, err)
		_, incomplete := scrapper.AsIncomplete(err)
		require.False(t, incomplete)
	})
}

// BigQuery's own hidden datasets and the datasets the configuration leaves out
// are not queried.
func TestSqlDefinitionsQueryOnlyTheDatasetsInScope(t *testing.T) {
	fake := &fakeMetadataServer{answersQueries: true, datasets: []fakeDataset{
		salesDataset(),
		{id: "_script" + strings.Repeat("a", 10), tables: []fakeTable{fakeBaseTable("tmp")}},
		{id: "crm", tables: []fakeTable{fakeBaseTable("customers")}},
	}}
	bq := newFakeMetadataScrapperOn(t, fake)
	bq.conf.Datasets = []string{"sales", "_script" + strings.Repeat("a", 10)}
	bq.scope = ScopeFromConf(bq.conf)

	rows, err := bq.QuerySqlDefinitions(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"sales.orders", "sales.orders_view"}, lo.Keys(definitionsByName(rows)))
	for _, q := range fake.queried() {
		require.NotContains(t, q, "crm")
		require.NotContains(t, q, "_script")
	}
}
