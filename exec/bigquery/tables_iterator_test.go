package bigquery

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// TestTablesIteratorDoesNotAdvancePastError documents the BigQuery iterator
// behaviour that caused multi-hour catalog hangs: when tables.list returns a
// non-Done error (e.g. a linked/shared dataset whose source was deleted returns
// 404), the iterator does NOT advance — every subsequent Next() returns the same
// error. A listing loop that does `continue` on such an error therefore
// busy-loops forever. The scrapper must return on any non-Done error instead.
func TestTablesIteratorDoesNotAdvancePastError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write(
			[]byte(
				`{"error":{"code":404,"message":"Not found: Dataset p:linked","errors":[{"reason":"notFound","message":"Not found: Dataset p:linked"}]}}`,
			),
		)
	}))
	defer srv.Close()

	client, err := bigquery.NewClient(
		context.Background(),
		"test-project",
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{Timeout: 5 * time.Second}),
	)
	require.NoError(t, err)
	defer client.Close()

	it := client.Dataset("linked").Tables(context.Background())
	_, err1 := it.Next()
	_, err2 := it.Next()

	require.Error(t, err1)
	require.False(t, errors.Is(err1, iterator.Done), "error must not be Done")
	// The decisive assertion: the iterator stays in its error state, it does not
	// advance to Done. `continue`-ing here is an infinite loop.
	require.Error(t, err2)
	require.False(t, errors.Is(err2, iterator.Done), "iterator must not advance to Done after an error")
}
