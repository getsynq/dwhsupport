package bigquery

import (
	"context"
	"strings"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

// queriesReading returns the queries that read the given INFORMATION_SCHEMA view.
func queriesReading(queries []string, view string) []string {
	var matching []string
	for _, q := range queries {
		if strings.Contains(q, "INFORMATION_SCHEMA."+view) {
			matching = append(matching, q)
		}
	}
	return matching
}

// Every refused query is a failed job in the customer's audit log. A role
// granted only on the datasets is refused the region's INFORMATION_SCHEMA on
// every refresh, so once the scrapper saw a refusal it reads each dataset
// straight away. TABLES and ROUTINES need different permissions, so a refusal
// of one does not stop the scrapper from reading the other's region.
func TestARefusedRegionReadIsNotRepeated(t *testing.T) {
	fake := &fakeMetadataServer{answersQueries: true, datasets: []fakeDataset{
		salesDataset(),
		{id: "crm", tables: []fakeTable{fakeBaseTable("customers")}},
	}}
	bq := newFakeMetadataScrapperOn(t, fake)
	ctx := context.Background()

	for range 2 {
		rows, err := bq.QuerySqlDefinitions(ctx)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"sales.orders", "sales.orders_view", "crm.customers"}, lo.Keys(definitionsByName(rows)))
	}
	for range 2 {
		_, err := bq.QueryRoutines(ctx)
		require.NoError(t, err)
	}

	region := fake.regionQueries()
	require.Len(t, queriesReading(region, "TABLES"), 1, "the region's TABLES is read once: %v", region)
	require.Len(t, queriesReading(region, "ROUTINES"), 1, "the region's ROUTINES is read once: %v", region)

	all := fake.queried()
	require.Len(t, queriesReading(all, "TABLES"), 1+2*2, "the region once, then each dataset on every call")
	require.Len(t, queriesReading(all, "ROUTINES"), 1+2*2, "the region once, then each dataset on every call")
}

// BigQuery answers a quota or rate limit with a 403 too. No grant lifts it, so
// it is neither a reason to read each dataset (many more jobs against the same
// quota) nor remembered as a refusal: the next call reads the region again.
func TestARegionReadOverQuotaIsNotARefusal(t *testing.T) {
	fake := &fakeMetadataServer{answersQueries: true, regionQuotaExceeded: true, datasets: []fakeDataset{
		salesDataset(),
		{id: "crm", tables: []fakeTable{fakeBaseTable("customers")}},
	}}
	bq := newFakeMetadataScrapperOn(t, fake)
	ctx := context.Background()

	for range 2 {
		_, err := bq.QuerySqlDefinitions(ctx)
		require.Error(t, err)
		require.True(t, isQuotaError(err), "the quota error is returned: %v", err)
	}
	for range 2 {
		_, err := bq.QueryRoutines(ctx)
		require.Error(t, err)
		require.True(t, isQuotaError(err), "the quota error is returned: %v", err)
	}

	all := fake.queried()
	require.Equal(t, all, fake.regionQueries(), "no dataset is read")
	require.Len(t, queriesReading(all, "TABLES"), 2, "the region's TABLES is read on every call")
	require.Len(t, queriesReading(all, "ROUTINES"), 2, "the region's ROUTINES is read on every call")
}
