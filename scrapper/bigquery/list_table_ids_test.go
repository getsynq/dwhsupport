package bigquery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/api/option"
)

// TestCollectTableIDsPagesAndEmitsEvents verifies that collectTableIDs iterates
// every tables.list page and records progress/done events on the active span
// carrying the running table count, so a stalled or oversized listing is
// observable in traces.
func TestCollectTableIDsPagesAndEmitsEvents(t *testing.T) {
	// Force a progress event on every table so we can assert the running count.
	prev := tablesListProgressInterval
	tablesListProgressInterval = 0
	defer func() { tablesListProgressInterval = prev }()

	// Two pages: page 1 returns t1,t2 + a next-page token; page 2 returns t3 and
	// no token (terminal).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("pageToken") == "" {
			_, _ = w.Write([]byte(`{"kind":"bigquery#tableList","tables":[
				{"tableReference":{"projectId":"p","datasetId":"ds","tableId":"t1"}},
				{"tableReference":{"projectId":"p","datasetId":"ds","tableId":"t2"}}
			],"nextPageToken":"page2"}`))
			return
		}
		_, _ = w.Write([]byte(`{"kind":"bigquery#tableList","tables":[
			{"tableReference":{"projectId":"p","datasetId":"ds","tableId":"t3"}}
		]}`))
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

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	ctx, span := tp.Tracer("test").Start(context.Background(), "test")

	ids, err := collectTableIDs(ctx, "ds", client.Dataset("ds").Tables(ctx))
	require.NoError(t, err)
	require.Equal(t, []string{"t1", "t2", "t3"}, ids)

	span.End()
	recorded := sr.Ended()
	require.Len(t, recorded, 1)
	events := recorded[0].Events()

	// With a zero throttle interval every table emits a progress event (3),
	// followed by exactly one done event.
	var progress, done int
	for _, e := range events {
		switch e.Name {
		case "bigquery.tables.list.progress":
			progress++
		case "bigquery.tables.list.done":
			done++
		}
	}
	require.Equal(t, 3, progress, "one progress event per table at zero throttle")
	require.Equal(t, 1, done, "exactly one done event")

	// The done event reports the final table total across both pages.
	doneAttrs := attrMap(t, events, "bigquery.tables.list.done")
	require.Equal(t, int64(3), doneAttrs["bq.tables_list.tables"])
	require.Equal(t, "ds", doneAttrs["bq.dataset"])
}

// attrMap returns the attributes of the first event with the given name as a
// name->value map (ints as int64, strings as string).
func attrMap(t *testing.T, events []sdktrace.Event, name string) map[string]any {
	t.Helper()
	for _, e := range events {
		if e.Name != name {
			continue
		}
		out := make(map[string]any, len(e.Attributes))
		for _, kv := range e.Attributes {
			switch kv.Value.Type() {
			case attribute.INT64:
				out[string(kv.Key)] = kv.Value.AsInt64()
			default:
				out[string(kv.Key)] = kv.Value.AsString()
			}
		}
		return out
	}
	t.Fatalf("event %q not found", name)
	return nil
}
