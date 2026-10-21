package bigquery

import (
	"context"
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

// TestBigQueryPolicyTags reads, with FetchPolicyTags on, the dwhtesting
// fixture table policy_tagged, whose column email carries the policy tag
// High > Email of the taxonomy dwhtesting_sensitivity and nickname carries
// Low. It needs the Data Catalog
// API in the project and datacatalog.taxonomies.get for the test's service
// account, which also covers listing the taxonomy's policy tags.
func TestBigQueryPolicyTags(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping BigQuery integration tests in CI")
	}
	if os.Getenv("BIGQUERY_CREDENTIALS_FILE") == "" {
		t.Skip("BIGQUERY_CREDENTIALS_FILE env var not set")
	}
	ctx := context.Background()
	sc, err := newBigQueryScrapperFromEnv(ctx)
	if err != nil {
		t.Skipf("Could not connect to BigQuery: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })
	sc.conf.FetchPolicyTags = true
	sc.newDataCatalog = dataCatalogServiceFor(sc.conf)

	rows, err := sc.QueryCatalog(ctx)
	require.NoError(t, err)
	byColumn := lo.SliceToMap(
		lo.Filter(rows, func(r *scrapper.CatalogColumnRow, _ int) bool { return r.Table == "policy_tagged" }),
		func(r *scrapper.CatalogColumnRow) (string, []*scrapper.Tag) { return r.Column, r.ColumnTags },
	)
	if len(byColumn) == 0 {
		t.Skip("the policy_tagged fixture is not in the dwhtesting dataset, run seed_policy_tags.sh")
	}
	require.Equal(t, map[string][]*scrapper.Tag{
		"id":       nil,
		"email":    {{TagName: "dwhtesting_sensitivity", TagValue: "High:Email", Kind: scrapper.TagKindPolicyTag}},
		"nickname": {{TagName: "dwhtesting_sensitivity", TagValue: "Low", Kind: scrapper.TagKindPolicyTag}},
	}, byColumn)
}
