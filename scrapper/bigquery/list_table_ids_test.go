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

// TestCollectTableIDsReturnsOnErrorWithoutLooping locks in the safety-critical
// invariant the instrumentation is built around: a non-Done iterator error is
// terminal and must be returned immediately. The BigQuery iterator never
// advances past such an error, so re-reading would busy-loop — the root cause of
// the multi-hour catalog hangs. No done event must be recorded for a failed
// listing (only a hung/oversized listing is distinguished by progress events).
func TestCollectTableIDsReturnsOnErrorWithoutLooping(t *testing.T) {
	prev := tablesListProgressInterval
	tablesListProgressInterval = 0
	defer func() { tablesListProgressInterval = prev }()

	// Page 1 returns one table + a next-page token; page 2 fails with a
	// non-retryable 400 (a 5xx would be retried by the BigQuery client and never
	// surface to the iterator as a terminal error).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("pageToken") == "" {
			_, _ = w.Write([]byte(`{"kind":"bigquery#tableList","tables":[
				{"tableReference":{"projectId":"p","datasetId":"ds","tableId":"t1"}}
			],"nextPageToken":"page2"}`))
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"code":400,"message":"boom"}}`))
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

	// Must return the error promptly rather than busy-looping on the stuck
	// iterator. A generous deadline detects a loop without making the test slow.
	done := make(chan struct{})
	var ids []string
	var collectErr error
	go func() {
		ids, collectErr = collectTableIDs(ctx, "ds", client.Dataset("ds").Tables(ctx))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("collectTableIDs did not return on iterator error (busy-loop?)")
	}

	require.Error(t, collectErr)
	require.Nil(t, ids)

	span.End()
	recorded := sr.Ended()
	require.Len(t, recorded, 1)
	for _, e := range recorded[0].Events() {
		require.NotEqual(t, "bigquery.tables.list.done", e.Name,
			"no done event must be recorded for a failed listing")
	}
}

// TestCollectTableIDsThrottlesProgressEvents verifies the throttle gate: with a
// non-zero interval and instant in-memory paging, the per-table emission is
// suppressed, so far fewer progress events are recorded than tables listed.
func TestCollectTableIDsThrottlesProgressEvents(t *testing.T) {
	// A large interval means no table elapses enough time to emit a progress
	// event during instant httptest paging — exercising the throttle gate that a
	// zero interval bypasses.
	prev := tablesListProgressInterval
	tablesListProgressInterval = time.Hour
	defer func() { tablesListProgressInterval = prev }()

	const total = 5
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"bigquery#tableList","tables":[
			{"tableReference":{"projectId":"p","datasetId":"ds","tableId":"t1"}},
			{"tableReference":{"projectId":"p","datasetId":"ds","tableId":"t2"}},
			{"tableReference":{"projectId":"p","datasetId":"ds","tableId":"t3"}},
			{"tableReference":{"projectId":"p","datasetId":"ds","tableId":"t4"}},
			{"tableReference":{"projectId":"p","datasetId":"ds","tableId":"t5"}}
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
	require.Len(t, ids, total)

	span.End()
	recorded := sr.Ended()
	require.Len(t, recorded, 1)

	var progress, done int
	for _, e := range recorded[0].Events() {
		switch e.Name {
		case "bigquery.tables.list.progress":
			progress++
		case "bigquery.tables.list.done":
			done++
		}
	}
	require.Less(t, progress, total, "throttle must suppress most per-table progress events")
	require.Equal(t, 1, done, "exactly one done event regardless of throttling")
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
