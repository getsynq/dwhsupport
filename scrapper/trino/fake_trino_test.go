package trino

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	dwhexectrino "github.com/getsynq/dwhsupport/exec/trino"
)

// fakeTrino answers the Trino client protocol from canned results, so a test can
// put a scrapper in front of a server failing in a chosen way without a warehouse.
// Each request's SQL is offered to the routes in order, and the first that matches
// answers it.
type fakeTrino struct {
	t      *testing.T
	routes []fakeRoute

	mu      sync.Mutex
	pending map[string]map[string]any
}

type fakeRoute struct {
	match   func(sql string) bool
	columns []fakeColumn
	rows    [][]any
	// err fails the query with Trino's own error envelope.
	err *fakeTrinoError
}

type fakeColumn struct {
	name     string
	trinoTyp string
}

type fakeTrinoError struct {
	errorType string
	message   string
}

func (f *fakeTrino) start() *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(f.serve))
	f.t.Cleanup(server.Close)
	return server
}

// serve answers the way Trino does: the POST that submits a statement returns
// only a nextUri, and the result (or the failure) comes from following it.
func (f *fakeTrino) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/statement/executing/") {
		id := strings.TrimPrefix(r.URL.Path, "/v1/statement/executing/")
		f.mu.Lock()
		response, ok := f.pending[id]
		delete(f.pending, id)
		f.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
		return
	}
	if r.Method != http.MethodPost || r.URL.Path != "/v1/statement" {
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sql := string(body)

	for _, route := range f.routes {
		if !route.match(sql) {
			continue
		}
		f.mu.Lock()
		if f.pending == nil {
			f.pending = map[string]map[string]any{}
		}
		id := "q" + strconv.Itoa(len(f.pending)) + "_" + strconv.FormatInt(time.Now().UnixNano(), 36)
		f.mu.Unlock()
		response := map[string]any{"id": id, "stats": map[string]any{"state": "FINISHED"}}
		if route.err != nil {
			response["stats"] = map[string]any{"state": "FAILED"}
			response["error"] = map[string]any{
				"message":   route.err.message,
				"errorName": route.err.errorType,
				"errorType": route.err.errorType,
			}
		} else {
			columns := make([]map[string]any, len(route.columns))
			for i, c := range route.columns {
				columns[i] = map[string]any{
					"name":          c.name,
					"type":          c.trinoTyp,
					"typeSignature": map[string]any{"rawType": c.trinoTyp, "arguments": []any{}},
				}
			}
			response["columns"] = columns
			data := route.rows
			if data == nil {
				data = [][]any{}
			}
			response["data"] = data
		}
		f.mu.Lock()
		f.pending[id] = response
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":      id,
			"nextUri": "http://" + r.Host + "/v1/statement/executing/" + id,
			"stats":   map[string]any{"state": "QUEUED"},
		})
		return
	}
	f.t.Errorf("fake Trino has no route for query:\n%s", sql)
	http.Error(w, "no route", http.StatusInternalServerError)
}

func newFakeTrinoScrapper(t *testing.T, ctx context.Context, fake *fakeTrino, conf TrinoScrapperConf) *TrinoScrapper {
	t.Helper()
	server := fake.start()
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}
	conf.TrinoConf = &dwhexectrino.TrinoConf{Host: u.Hostname(), Port: port, Plaintext: true, User: "test"}
	scr, err := NewTrinoScrapper(ctx, &conf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = scr.Close() })
	return scr
}

func sqlContains(parts ...string) func(string) bool {
	return func(sql string) bool {
		for _, p := range parts {
			if !strings.Contains(sql, p) {
				return false
			}
		}
		return true
	}
}

func sqlLacks(match func(string) bool, absent ...string) func(string) bool {
	return func(sql string) bool {
		if !match(sql) {
			return false
		}
		for _, a := range absent {
			if strings.Contains(sql, a) {
				return false
			}
		}
		return true
	}
}
