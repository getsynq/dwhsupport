package bigquery

import (
	"context"
	"fmt"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/require"
)

// The tables of one dataset are read concurrently, and each starts from the
// dataset's labels, so each needs its own slice to add its own labels to.
func TestTablesOfOneDatasetKeepTheirOwnLabels(t *testing.T) {
	var tables []fakeTable
	for i := range 16 {
		tables = append(tables, fakeTable{
			id:   fmt.Sprintf("t%d", i),
			json: fmt.Sprintf(`"type":"TABLE","labels":{"table":"t%d"},"schema":{"fields":[{"name":"id","type":"INTEGER"}]}`, i),
		})
	}
	// Three labels leave room in the dataset's slice for a fourth.
	scr := newFakeMetadataScrapper(t, fakeDataset{id: "crm", labels: `{"team":"data","env":"prod","tier":"gold"}`, tables: tables})
	rows, err := scr.QueryCatalog(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 16)
	for _, row := range rows {
		require.ElementsMatch(t, []*scrapper.Tag{
			{TagName: "team", TagValue: "data", Kind: scrapper.TagKindLabel},
			{TagName: "env", TagValue: "prod", Kind: scrapper.TagKindLabel},
			{TagName: "tier", TagValue: "gold", Kind: scrapper.TagKindLabel},
			{TagName: "table", TagValue: row.Table, Kind: scrapper.TagKindLabel},
		}, row.TableTags, row.Table)
	}
}

// A table label replaces the dataset label with the same key: BigQuery keeps
// the two apart, and the table's is the one that describes the table.
func TestATableLabelReplacesTheDatasetLabelWithTheSameKey(t *testing.T) {
	scr := newFakeMetadataScrapper(t, fakeDataset{
		id:     "crm",
		labels: `{"owner":"data-eng","env":"prod"}`,
		tables: []fakeTable{
			{id: "orders", json: `"type":"TABLE","labels":{"owner":"analytics"},"schema":{"fields":[{"name":"id","type":"INTEGER"}]}`},
			{id: "customers", json: `"type":"TABLE","schema":{"fields":[{"name":"id","type":"INTEGER"}]}`},
		},
	})
	want := map[string][]*scrapper.Tag{
		"orders": {
			{TagName: "env", TagValue: "prod", Kind: scrapper.TagKindLabel},
			{TagName: "owner", TagValue: "analytics", Kind: scrapper.TagKindLabel},
		},
		"customers": {
			{TagName: "env", TagValue: "prod", Kind: scrapper.TagKindLabel},
			{TagName: "owner", TagValue: "data-eng", Kind: scrapper.TagKindLabel},
		},
	}

	tables, err := scr.QueryTables(context.Background())
	require.NoError(t, err)
	require.Len(t, tables, 2)
	for _, row := range tables {
		require.ElementsMatch(t, want[row.Table], row.Tags, "QueryTables %s", row.Table)
	}

	columns, err := scr.QueryCatalog(context.Background())
	require.NoError(t, err)
	require.Len(t, columns, 2)
	for _, row := range columns {
		require.ElementsMatch(t, want[row.Table], row.TableTags, "QueryCatalog %s", row.Table)
	}
}
