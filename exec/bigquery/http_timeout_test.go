package bigquery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

// hungServerClient returns a BigQuery client pointed at a server that never
// answers, reproducing the metadata call that wedged the catalog scrape. The
// handler unblocks as soon as the client cancels/times out the request (so the
// server shuts down cleanly), letting us measure how long the *client* waits.
func hungServerClient(t *testing.T) *bigquery.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	client, err := bigquery.NewClient(
		context.Background(),
		"test-project",
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{Timeout: 200 * time.Millisecond}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestMetadataRespectsContextDeadline is the fix: a per-call context deadline
// stops the BigQuery client's internal retry loop against a stalled endpoint, so
// the call returns promptly instead of hanging until the job-level deadline. A
// per-round-trip http.Client.Timeout alone is NOT enough — the client just
// retries each fast-failing attempt forever until the context is cancelled.
func TestMetadataRespectsContextDeadline(t *testing.T) {
	client := hungServerClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	_, err := client.Dataset("d").Table("t").Metadata(ctx)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Less(t, elapsed, 30*time.Second, "per-call context deadline must bound the metadata call")
}
