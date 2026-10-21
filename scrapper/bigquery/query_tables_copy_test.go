package bigquery

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

// A table snapshot and a table clone name the table they were copied from,
// and the time of it they hold.
func TestSnapshotsAndClonesNameTheirBaseTable(t *testing.T) {
	// The snapshot copies a shard older than the latest, which the scan does
	// not report on its own: it reports the sharded table in its place.
	shard := time.Now().UTC().AddDate(0, 0, -3).Format("20060102")
	latestShard := time.Now().UTC().AddDate(0, 0, -1).Format("20060102")
	const schema = `"schema":{"fields":[{"name":"id","type":"INTEGER"}]}`
	scr := newFakeMetadataScrapper(t,
		fakeDataset{id: "sales", tables: []fakeTable{
			{id: "products", json: `"type":"TABLE",` + schema},
			{id: "products_snapshot", json: `"type":"SNAPSHOT",` + schema + `,"snapshotDefinition":{` +
				`"baseTableReference":{"projectId":"test-project","datasetId":"sales","tableId":"products"},` +
				`"snapshotTime":"2024-03-15T10:20:30.123Z"}`},
			{id: "products_clone", json: `"type":"TABLE",` + schema + `,"cloneDefinition":{` +
				`"baseTableReference":{"projectId":"other-project","datasetId":"warehouse","tableId":"products"},` +
				`"cloneTime":"2024-03-16T08:00:00Z"}`},
			{id: "events_snapshot", json: `"type":"SNAPSHOT",` + schema + `,"snapshotDefinition":{` +
				`"baseTableReference":{"projectId":"test-project","datasetId":"raw","tableId":"events_` + shard + `"},` +
				`"snapshotTime":"2024-03-15T10:20:30Z"}`},
			{id: "orphan_snapshot", json: `"type":"SNAPSHOT",` + schema + `,"snapshotDefinition":{"snapshotTime":"2024-03-15T10:20:30Z"}`},
			{id: "products_view", json: `"type":"VIEW","view":{"query":"SELECT 1"},` + schema},
		}},
		fakeDataset{id: "raw", tables: []fakeTable{
			{id: "events_" + shard, json: `"type":"TABLE",` + schema},
			{id: "events_" + latestShard, json: `"type":"TABLE",` + schema},
		}},
	)

	rows, err := scr.QueryTables(context.Background())
	require.NoError(t, err)
	got := map[string]*scrapper.TableCopyOf{}
	var rawTables []string
	for _, row := range rows {
		if row.Schema == "raw" {
			rawTables = append(rawTables, row.Table)
			continue
		}
		got[row.Table] = row.CopyOf
	}
	// The base of events_snapshot is named the way the scan names it.
	require.Equal(t, []string{"events"}, rawTables)

	require.Equal(t, map[string]*scrapper.TableCopyOf{
		"products": nil,
		"products_snapshot": {
			Kind: scrapper.TableCopySnapshot, Database: "test-project", Schema: "sales", Table: "products",
			At: lo.ToPtr(time.Date(2024, 3, 15, 10, 20, 30, 123_000_000, time.UTC)),
		},
		"products_clone": {
			Kind: scrapper.TableCopyClone, Database: "other-project", Schema: "warehouse", Table: "products",
			At: lo.ToPtr(time.Date(2024, 3, 16, 8, 0, 0, 0, time.UTC)),
		},
		"events_snapshot": {
			Kind: scrapper.TableCopySnapshot, Database: "test-project", Schema: "raw", Table: "events",
			At: lo.ToPtr(time.Date(2024, 3, 15, 10, 20, 30, 0, time.UTC)),
		},
		// A snapshot whose base the API does not name copies nothing we can point at.
		"orphan_snapshot": nil,
		"products_view":   nil,
	}, got)
}

// A row that is no copy keeps its JSON as it was, and a copy's base survives
// the round trip a stored row takes.
func TestTableCopyOfJSON(t *testing.T) {
	plain, err := json.Marshal(&scrapper.TableRow{Database: "p", Schema: "s", Table: "t"})
	require.NoError(t, err)
	require.NotContains(t, string(plain), "copy_of")

	row := &scrapper.TableRow{Database: "p", Schema: "s", Table: "t", CopyOf: &scrapper.TableCopyOf{
		Kind: scrapper.TableCopyClone, Database: "p", Schema: "s", Table: "base",
		At: lo.ToPtr(time.Date(2024, 3, 16, 8, 0, 0, 0, time.UTC)),
	}}
	encoded, err := json.Marshal(row)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"copy_of":{"kind":"clone","database":"p","schema":"s","table":"base","at":"2024-03-16T08:00:00Z"}`)
	var decoded scrapper.TableRow
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Equal(t, row.CopyOf, decoded.CopyOf)
}
