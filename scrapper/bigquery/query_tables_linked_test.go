package bigquery

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/require"
)

const fakeSchema = `"schema":{"fields":[{"name":"id","type":"INTEGER"}]}`

func fakeLinkedTable(id string) fakeTable {
	return fakeTable{id: id, json: `"type":"TABLE",` + fakeSchema}
}

// linkedTo is the JSON datasets.get answers a linked dataset with: BigQuery
// names the source project by its number.
func linkedTo(project, dataset string) string {
	return `"type":"LINKED","linkedDatasetSource":{"sourceDataset":{"projectId":"` + project + `","datasetId":"` + dataset + `"}}`
}

func linkedFromByTable(t *testing.T, rows []*scrapper.TableRow) map[string]*scrapper.TableLinkedFrom {
	t.Helper()
	got := map[string]*scrapper.TableLinkedFrom{}
	for _, row := range rows {
		got[row.Schema+"."+row.Table] = row.LinkedFrom
	}
	return got
}

// Each table of a linked dataset names the table of the same name in the
// source dataset, its project named by id although BigQuery gives its number.
// The projects are listed once per scan, however many datasets are linked.
func TestLinkedDatasetTablesNameTheirSourceTable(t *testing.T) {
	fake := &fakeMetadataServer{
		projects: []string{
			`{"id":"publisher","numericId":"123456789012","projectReference":{"projectId":"publisher"}}`,
			`{"id":"test-project","numericId":"42","projectReference":{"projectId":"test-project"}}`,
		},
		datasets: []fakeDataset{
			{id: "sales", tables: []fakeTable{fakeLinkedTable("orders")}},
			{id: "shared_sales", linked: linkedTo("123456789012", "sales"), tables: []fakeTable{
				fakeLinkedTable("orders"), fakeLinkedTable("customers"),
			}},
			{id: "shared_events", linked: linkedTo("123456789012", "events"), tables: []fakeTable{fakeLinkedTable("clicks")}},
			// A source already named by id is taken as it is.
			{id: "shared_by_id", linked: linkedTo("named-project", "raw"), tables: []fakeTable{fakeLinkedTable("logs")}},
		},
	}
	scr := newFakeMetadataScrapperOn(t, fake)

	rows, err := scr.QueryTables(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[string]*scrapper.TableLinkedFrom{
		"sales.orders":           nil,
		"shared_sales.orders":    {Database: "publisher", Schema: "sales", Table: "orders"},
		"shared_sales.customers": {Database: "publisher", Schema: "sales", Table: "customers"},
		"shared_events.clicks":   {Database: "publisher", Schema: "events", Table: "clicks"},
		"shared_by_id.logs":      {Database: "named-project", Schema: "raw", Table: "logs"},
	}, linkedFromByTable(t, rows))
	require.Equal(t, 1, fake.projectListings())
}

// A scan with no linked dataset never lists the projects.
func TestNoLinkedDatasetListsNoProjects(t *testing.T) {
	fake := &fakeMetadataServer{datasets: []fakeDataset{{id: "sales", tables: []fakeTable{fakeLinkedTable("orders")}}}}
	scr := newFakeMetadataScrapperOn(t, fake)

	rows, err := scr.QueryTables(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[string]*scrapper.TableLinkedFrom{"sales.orders": nil}, linkedFromByTable(t, rows))
	require.Zero(t, fake.projectListings())
}

// A linked dataset whose source cannot be named, or that no longer serves it,
// names no source, and none of it fails the scan.
func TestLinkedDatasetWithoutANameableSourceNamesNone(t *testing.T) {
	tests := []struct {
		name string
		fake *fakeMetadataServer
	}{
		{
			name: "the caller has no role on the source project",
			fake: &fakeMetadataServer{
				projects: []string{`{"id":"test-project","numericId":"42","projectReference":{"projectId":"test-project"}}`},
				datasets: []fakeDataset{{id: "shared", linked: linkedTo("123456789012", "sales"), tables: []fakeTable{fakeLinkedTable("orders")}}},
			},
		},
		{
			name: "the projects cannot be listed",
			fake: &fakeMetadataServer{
				projectsStatus: http.StatusForbidden,
				datasets:       []fakeDataset{{id: "shared", linked: linkedTo("123456789012", "sales"), tables: []fakeTable{fakeLinkedTable("orders")}}},
			},
		},
		{
			name: "the read of the link is refused",
			fake: &fakeMetadataServer{
				datasets: []fakeDataset{{
					id: "shared", linked: linkedTo("named-project", "sales"), linkedStatus: http.StatusForbidden,
					tables: []fakeTable{fakeLinkedTable("orders")},
				}},
			},
		},
		{
			name: "the publisher unlinked the dataset",
			fake: &fakeMetadataServer{
				datasets: []fakeDataset{{
					id: "shared", linked: linkedTo("named-project", "sales") + `,"linkedDatasetMetadata":{"linkState":"UNLINKED"}`,
					tables: []fakeTable{fakeLinkedTable("orders")},
				}},
			},
		},
		{
			name: "the link names no source",
			fake: &fakeMetadataServer{
				datasets: []fakeDataset{{id: "shared", linked: `"type":"LINKED"`, tables: []fakeTable{fakeLinkedTable("orders")}}},
			},
		},
		{
			name: "the source names no dataset",
			fake: &fakeMetadataServer{
				datasets: []fakeDataset{{id: "shared", linked: linkedTo("named-project", ""), tables: []fakeTable{fakeLinkedTable("orders")}}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scr := newFakeMetadataScrapperOn(t, tt.fake)
			rows, err := scr.QueryTables(context.Background())
			require.NoError(t, err)
			require.Equal(t, map[string]*scrapper.TableLinkedFrom{"shared.orders": nil}, linkedFromByTable(t, rows))
		})
	}
}

// A row in no link keeps its JSON as it was, and a link's source survives the
// round trip a stored row takes.
func TestTableLinkedFromJSON(t *testing.T) {
	plain, err := json.Marshal(&scrapper.TableRow{Database: "p", Schema: "s", Table: "t"})
	require.NoError(t, err)
	require.NotContains(t, string(plain), "linked_from")

	row := &scrapper.TableRow{Database: "p", Schema: "s", Table: "t", LinkedFrom: &scrapper.TableLinkedFrom{
		Database: "publisher", Schema: "sales", Table: "t",
	}}
	encoded, err := json.Marshal(row)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"linked_from":{"database":"publisher","schema":"sales","table":"t"}`)
	var decoded scrapper.TableRow
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Equal(t, row.LinkedFrom, decoded.LinkedFrom)
}
