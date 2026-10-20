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
			{TagName: "label.team", TagValue: "data"},
			{TagName: "label.env", TagValue: "prod"},
			{TagName: "label.tier", TagValue: "gold"},
			{TagName: "label.table", TagValue: row.Table},
		}, row.TableTags, row.Table)
	}
}
