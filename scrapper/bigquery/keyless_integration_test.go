package bigquery

import (
	"context"
	"os"
	"strings"
	"testing"

	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Keyless BigQuery against the dwhtesting fixture:
//
//	getsynq/cloud/dev-infra/dwhtesting/lib/bigquery/seed_keyless.sh
//
// The chain starts at the identity of BIGQUERY_KEYLESS_CREDENTIALS_FILE, which has the part of the
// process's own default credentials. The workspace service account has no BigQuery access, the
// customer's service account can read the dwhtesting datasets.
//
// Required environment, besides BIGQUERY_PROJECT_ID:
//
//	BIGQUERY_KEYLESS_CREDENTIALS_FILE          — key of the identity that starts the chain
//	BIGQUERY_KEYLESS_WORKSPACE_SERVICE_ACCOUNT — may impersonate the customer's service account
//	BIGQUERY_KEYLESS_CUSTOMER_SERVICE_ACCOUNT  — has the BigQuery grants
//
// Tests are skipped when CI=1 or when any of them is unset.

type keylessFixture struct {
	project   string
	workspace string
	customer  string
}

func keylessFixtureFromEnv(t *testing.T) keylessFixture {
	t.Helper()
	if os.Getenv("CI") != "" {
		t.Skip("Skipping BigQuery integration tests in CI")
	}
	f := keylessFixture{
		project:   os.Getenv("BIGQUERY_PROJECT_ID"),
		workspace: os.Getenv("BIGQUERY_KEYLESS_WORKSPACE_SERVICE_ACCOUNT"),
		customer:  os.Getenv("BIGQUERY_KEYLESS_CUSTOMER_SERVICE_ACCOUNT"),
	}
	credentials := os.Getenv("BIGQUERY_KEYLESS_CREDENTIALS_FILE")
	if f.project == "" || f.workspace == "" || f.customer == "" || credentials == "" {
		t.Skip("the keyless BigQuery fixture env vars are not set")
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credentials)
	return f
}

// connectKeyless scrapes the dwhtesting datasets as the last service account of chain. A refused hop
// fails either the connection check or the first query, so both are returned as one error.
func connectKeyless(t *testing.T, f keylessFixture, chain ...string) (*BigQueryScrapper, error) {
	t.Helper()
	sc, err := NewBigQueryScrapper(context.Background(), &BigQueryScrapperConf{
		BigQueryConf: dwhexecbigquery.BigQueryConf{
			ProjectId:                  f.project,
			Region:                     testenv.EnvOrDefault("BIGQUERY_REGION", "EU"),
			ImpersonateServiceAccounts: chain,
		},
		Datasets: []string{
			testenv.EnvOrDefault("BIGQUERY_DATASET_A", defaultDatasetA),
			testenv.EnvOrDefault("BIGQUERY_DATASET_B", defaultDatasetB),
		},
	})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = sc.Close() })
	return sc, nil
}

func sessionUser(t *testing.T, sc *BigQueryScrapper) string {
	t.Helper()
	ctx := context.Background()
	rows, err := sc.RunRawQuery(ctx, "SELECT SESSION_USER() AS user")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	row, err := rows.Next(ctx)
	require.NoError(t, err)
	user, ok := scrapper.AsString(row[0].Value)
	require.True(t, ok)
	return user
}

func requireScrapesDwhtesting(t *testing.T, sc *BigQueryScrapper) {
	t.Helper()
	tables, err := sc.QueryTables(context.Background())
	require.NoError(t, err)
	names := map[string]bool{}
	for _, table := range tables {
		names[table.Table] = true
	}
	assert.True(t, names["products"], "the dwhtesting seed's products table, got %v", names)
}

func TestBigQueryKeyless_Impersonation(t *testing.T) {
	f := keylessFixtureFromEnv(t)
	sc, err := connectKeyless(t, f, f.workspace, f.customer)
	require.NoError(t, err)

	requireScrapesDwhtesting(t, sc)
	assert.Equal(t, f.customer, sessionUser(t, sc), "queries run as the customer's service account")
}

// The workspace service account was granted the BigQuery roles itself: a single hop.
func TestBigQueryKeyless_DirectGrant(t *testing.T) {
	f := keylessFixtureFromEnv(t)
	sc, err := connectKeyless(t, f, f.customer)
	require.NoError(t, err)

	requireScrapesDwhtesting(t, sc)
	assert.Equal(t, f.customer, sessionUser(t, sc))
}

func TestBigQueryKeyless_MissingTokenCreatorGrant(t *testing.T) {
	f := keylessFixtureFromEnv(t)
	err := firstKeylessError(t, f, f.customer, f.workspace)

	impErr, ok := dwhexecbigquery.AsImpersonationError(err)
	require.True(t, ok, "got %v", err)
	assert.Equal(t, f.workspace, impErr.ServiceAccount)
	assert.Equal(t, f.customer, impErr.Caller)
	assert.True(t, impErr.Denied())
	assert.True(t, dwhexecbigquery.IsAuthError(err))
	assert.False(t, dwhexecbigquery.IsPermissionError(err))
}

func TestBigQueryKeyless_MissingServiceAccount(t *testing.T) {
	f := keylessFixtureFromEnv(t)
	missing := "quality-keyless-missing@" + strings.SplitN(f.customer, "@", 2)[1]
	err := firstKeylessError(t, f, f.workspace, missing)

	impErr, ok := dwhexecbigquery.AsImpersonationError(err)
	require.True(t, ok, "got %v", err)
	assert.Equal(t, missing, impErr.ServiceAccount)
	assert.Equal(t, f.workspace, impErr.Caller)
	assert.True(t, impErr.Denied())
}

// The chain works, but the account at its end has no BigQuery roles: a missing grant, not a
// refused hop.
func TestBigQueryKeyless_MissingBigQueryRoles(t *testing.T) {
	f := keylessFixtureFromEnv(t)
	err := firstKeylessError(t, f, f.workspace)

	_, isImpersonation := dwhexecbigquery.AsImpersonationError(err)
	assert.False(t, isImpersonation, "got %v", err)
	assert.True(t, dwhexecbigquery.IsPermissionError(err), "got %v", err)
}

// firstKeylessError is the error of connecting through chain and scraping the tables.
func firstKeylessError(t *testing.T, f keylessFixture, chain ...string) error {
	t.Helper()
	sc, err := connectKeyless(t, f, chain...)
	if err != nil {
		return err
	}
	_, err = sc.QueryTables(context.Background())
	require.Error(t, err)
	return err
}
