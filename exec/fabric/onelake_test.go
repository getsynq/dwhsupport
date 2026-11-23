package fabric

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListDirectorySendsBeginFromOnlyWithTheFirstPage(t *testing.T) {
	var queries []string
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		queries = append(queries, q.Get("beginFrom")+"|"+q.Get("continuation"))
		switch q.Get("continuation") {
		case "":
			// OneLake can answer a page with no entry and a continuation.
			rw.Header().Set("x-ms-continuation", "c1")
			_, _ = rw.Write([]byte(`{"paths":[]}`))
		case "c1":
			_, _ = rw.Write([]byte(`{"paths":[{"name":"item/log/00000000000000000007.json","lastModified":"Mon, 23 Nov 2026 21:05:51 GMT"}]}`))
		}
	}))
	defer srv.Close()

	c := NewOneLakeClientWith(&staticToken{}, srv.URL, srv.Client())
	paths, err := c.ListDirectory(context.Background(), "ws", "item", "log", ListOptions{BeginFrom: "00000000000000000005"})
	require.NoError(t, err)
	require.Len(t, paths, 1)
	require.Equal(t, "00000000000000000007.json", paths[0].Name)
	// The continuation holds the position.
	require.Equal(t, []string{"00000000000000000005|", "|c1"}, queries)
}
