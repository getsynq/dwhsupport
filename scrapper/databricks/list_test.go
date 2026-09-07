package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	"github.com/stretchr/testify/require"
)

// TestQueriesPageUnityCatalogListings drives every scrape that walks Unity Catalog
// against a workspace that answers the way the real API does: a listing request
// naming no page size is refused outright (#UC-PGRQD) rather than served in one
// response, and a paged one is spread over as many responses as the server likes.
//
// A scrape that sends no page size therefore reads nothing at all, and one that
// sends a page size but ignores the page tokens silently reads only the first page
// of every catalog, schema and table listing — a partial catalog, which downstream
// is indistinguishable from tables having been dropped.
func TestQueriesPageUnityCatalogListings(t *testing.T) {
	fake := newFakeWorkspace()
	for c := 0; c < 3; c++ {
		catalog := fmt.Sprintf("catalog_%d", c)
		fake.addCatalog(catalog)
		for s := 0; s < 3; s++ {
			schema := fmt.Sprintf("schema_%d", s)
			fake.addSchema(catalog, schema)
			for tbl := 0; tbl < 5; tbl++ {
				fake.addTable(catalog, schema, fmt.Sprintf("table_%d", tbl))
			}
		}
	}
	// Small enough that every listing in the walk needs more than one response.
	fake.pageSize = 2

	scrapper := fake.start(t)
	ctx := context.Background()

	const (
		wantCatalogs = 3
		wantSchemas  = wantCatalogs * 3
		wantTables   = wantSchemas * 5
	)

	t.Run("QuerySchemas", func(t *testing.T) {
		rows, err := scrapper.QuerySchemas(ctx)
		require.NoError(t, err)
		require.Len(t, rows, wantSchemas)
	})

	t.Run("QueryTables", func(t *testing.T) {
		rows, err := scrapper.QueryTables(ctx)
		require.NoError(t, err)
		require.Len(t, rows, wantTables)
	})

	t.Run("QueryCatalog", func(t *testing.T) {
		rows, err := scrapper.QueryCatalog(ctx)
		require.NoError(t, err)
		// One row per column, and the fake gives every table a single column.
		require.Len(t, rows, wantTables)
	})

	t.Run("QuerySqlDefinitions", func(t *testing.T) {
		rows, err := scrapper.QuerySqlDefinitions(ctx)
		require.NoError(t, err)
		require.Len(t, rows, wantTables)
	})

	t.Run("QueryTableConstraints", func(t *testing.T) {
		// The fake gives every table one partitioning column and no primary key.
		rows, err := scrapper.QueryTableConstraints(ctx)
		require.NoError(t, err)
		require.Len(t, rows, wantTables)
	})

	t.Run("QueryTableMetrics", func(t *testing.T) {
		rows, err := scrapper.QueryTableMetrics(ctx, time.Now())
		require.NoError(t, err)
		require.Len(t, rows, wantTables)
	})
}

// fakeWorkspace serves the Unity Catalog listing endpoints the scrapes call.
type fakeWorkspace struct {
	catalogs []servicecatalog.CatalogInfo
	schemas  map[string][]servicecatalog.SchemaInfo
	tables   map[string][]servicecatalog.TableInfo
	// pageSize, when positive, is how many rows one listing response carries, so a
	// caller has to follow the page tokens to see the rest.
	pageSize int
	// vanishedCatalogs holds catalogs whose schema listing answers as though the
	// catalog had been dropped; vanishedSchemas the same for a schema's table
	// listing, keyed "catalog.schema".
	vanishedCatalogs map[string]bool
	vanishedSchemas  map[string]bool
	// deniedSchemas holds schemas whose table listing answers as though the caller
	// were not allowed to read them, keyed "catalog.schema".
	deniedSchemas map[string]bool
	// refuse, when set, decides whether the workspace answers a request by refusing it
	// for exceeding its control-plane quota rather than serving it. It runs in front of
	// every endpoint, the way a real limiter does.
	refuse func(r *http.Request) *refusedRequest
	// pacing, when set, is installed as the workspace's pacing before a scrapper is
	// built against it, so a test converges in milliseconds rather than in
	// production-sized waits.
	pacing *dwhexecdatabricks.Pacing
	// serverUrl is what the fake came up on, which is also the workspace URL a scrapper
	// is configured with and the key its throttle lives under.
	serverUrl string
	// served counts the requests the fake answered, refusals included.
	served int
	// readProperties, when set, is what a per-table read answers with on top of what the
	// listing carries — the statistics a metrics scrape goes back to each table for.
	readProperties map[string]string
	// listedCatalogs records the catalog of every schema listing served, and
	// listedSchemas the "catalog.schema" of every table listing, so a test can tell
	// what a scrape asked the API for rather than only what it returned.
	listedCatalogs []string
	listedSchemas  []string
	// readTables records the full name of every per-table read served.
	readTables []string

	mu sync.Mutex
}

func newFakeWorkspace() *fakeWorkspace {
	return &fakeWorkspace{
		schemas:          map[string][]servicecatalog.SchemaInfo{},
		tables:           map[string][]servicecatalog.TableInfo{},
		vanishedCatalogs: map[string]bool{},
		vanishedSchemas:  map[string]bool{},
		deniedSchemas:    map[string]bool{},
	}
}

func (f *fakeWorkspace) addCatalog(name string) {
	f.catalogs = append(f.catalogs, servicecatalog.CatalogInfo{
		Name: name, FullName: name, CatalogType: servicecatalog.CatalogTypeManagedCatalog,
	})
}

func (f *fakeWorkspace) addSchema(catalog, schema string) {
	f.schemas[catalog] = append(f.schemas[catalog], servicecatalog.SchemaInfo{
		Name: schema, CatalogName: catalog, FullName: catalog + "." + schema,
	})
}

func (f *fakeWorkspace) addTable(catalog, schema, table string) {
	key := catalog + "." + schema
	f.tables[key] = append(f.tables[key], servicecatalog.TableInfo{
		Name: table, CatalogName: catalog, SchemaName: schema, FullName: key + "." + table,
		TableType: servicecatalog.TableTypeManaged,
		UpdatedAt: time.Now().UnixMilli(),
		Columns: []servicecatalog.ColumnInfo{
			{Name: "id", TypeText: "bigint", Position: 0, PartitionIndex: 1},
		},
	})
}

// addVanishingCatalog adds a catalog that lists like any other but has been dropped
// by the time its schemas are read — the race a scrape runs against a workspace whose
// catalogs come and go while the walk is in progress.
func (f *fakeWorkspace) addVanishingCatalog(name string) {
	f.addCatalog(name)
	f.vanishedCatalogs[name] = true
}

// addVanishingSchema adds a schema that lists like any other but has been dropped by
// the time its tables are read.
func (f *fakeWorkspace) addVanishingSchema(catalog, schema string) {
	f.addSchema(catalog, schema)
	f.vanishedSchemas[catalog+"."+schema] = true
}

// addUnreadableSchema adds a schema that lists like any other but whose tables the
// caller is not allowed to read — unreadable rather than gone, which must not be
// mistaken for empty.
func (f *fakeWorkspace) addUnreadableSchema(catalog, schema string) {
	f.addSchema(catalog, schema)
	f.deniedSchemas[catalog+"."+schema] = true
}

func (f *fakeWorkspace) start(t *testing.T) *DatabricksScrapper {
	return f.startWith(t, &DatabricksScrapperConf{})
}

// startWith brings the fake up and builds a scrapper against it with a configuration
// of the test's choosing.
func (f *fakeWorkspace) startWith(t *testing.T, conf *DatabricksScrapperConf) *DatabricksScrapper {
	t.Helper()
	scrapper, err := f.tryStart(t, conf)
	require.NoError(t, err)
	return scrapper
}

// tryStart is startWith for a test that expects building the scrapper to fail — a
// workspace that refuses the ping it comes up with.
func (f *fakeWorkspace) tryStart(t *testing.T, conf *DatabricksScrapperConf) (*DatabricksScrapper, error) {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/2.1/unity-catalog/catalogs", func(w http.ResponseWriter, r *http.Request) {
		if f.rejectsUnpaginated(t, w, r, "ListCatalogs") {
			return
		}
		page, next := f.page(t, r, len(f.catalogs))
		f.writeJson(t, w, http.StatusOK, "catalogs", f.catalogs[page.from:page.to], next)
	})
	mux.HandleFunc("/api/2.1/unity-catalog/schemas", func(w http.ResponseWriter, r *http.Request) {
		if f.rejectsUnpaginated(t, w, r, "ListSchemas") {
			return
		}
		catalog := r.URL.Query().Get("catalog_name")
		f.record(&f.listedCatalogs, catalog)
		if f.vanishedCatalogs[catalog] {
			f.writeVanished(t, w, "CATALOG_DOES_NOT_EXIST", fmt.Sprintf("Catalog '%s' does not exist.", catalog))
			return
		}
		schemas := f.schemas[catalog]
		page, next := f.page(t, r, len(schemas))
		f.writeJson(t, w, http.StatusOK, "schemas", schemas[page.from:page.to], next)
	})
	mux.HandleFunc("/api/2.1/unity-catalog/tables", func(w http.ResponseWriter, r *http.Request) {
		if f.rejectsUnpaginated(t, w, r, "ListTables") {
			return
		}
		key := r.URL.Query().Get("catalog_name") + "." + r.URL.Query().Get("schema_name")
		f.record(&f.listedSchemas, key)
		if f.vanishedSchemas[key] {
			f.writeVanished(t, w, "SCHEMA_DOES_NOT_EXIST", fmt.Sprintf("Schema '%s' does not exist.", key))
			return
		}
		if f.deniedSchemas[key] {
			f.writeApiError(t, w, http.StatusForbidden, "PERMISSION_DENIED",
				fmt.Sprintf("User does not have USE SCHEMA on Schema '%s'.", key))
			return
		}
		tables := f.tables[r.URL.Query().Get("catalog_name")+"."+r.URL.Query().Get("schema_name")]
		page, next := f.page(t, r, len(tables))
		f.writeJson(t, w, http.StatusOK, "tables", tables[page.from:page.to], next)
	})
	// One table's own metadata, which is what a metrics scrape reads per table on top of
	// the listings.
	mux.HandleFunc("/api/2.1/unity-catalog/tables/", func(w http.ResponseWriter, r *http.Request) {
		fullName := strings.TrimPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/")
		f.record(&f.readTables, fullName)
		table, found := f.table(fullName)
		if !found {
			f.writeVanished(t, w, "TABLE_DOES_NOT_EXIST", fmt.Sprintf("Table '%s' does not exist.", fullName))
			return
		}
		if len(f.readProperties) > 0 {
			table.Properties = f.readProperties
		}
		f.writeJson(t, w, http.StatusOK, "", table, "")
	})
	// The ping and the warehouse lookup a scrapper makes on its way up. Answering no
	// warehouse means no SQL executor, which is what keeps these tests to the REST
	// client the pacing lives in.
	mux.HandleFunc("/api/2.0/preview/scim/v2/Me", func(w http.ResponseWriter, r *http.Request) {
		f.writeJson(t, w, http.StatusOK, "", map[string]any{"id": "1", "userName": "tester"}, "")
	})
	mux.HandleFunc("/api/2.0/preview/sql/data_sources", func(w http.ResponseWriter, r *http.Request) {
		f.writeJson(t, w, http.StatusOK, "", []any{}, "")
	})
	mux.HandleFunc("/api/2.0/sql/warehouses", func(w http.ResponseWriter, r *http.Request) {
		f.writeJson(t, w, http.StatusOK, "warehouses", []any{}, "")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected Databricks request: %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusNotImplemented)
	})

	server := httptest.NewServer(f.gate(t, mux))
	t.Cleanup(server.Close)
	f.serverUrl = server.URL

	if f.pacing != nil {
		dwhexecdatabricks.UsePacing(server.URL, *f.pacing)
	}

	conf.DatabricksConf = dwhexecdatabricks.DatabricksConf{
		WorkspaceUrl: server.URL,
		Auth:         dwhexecdatabricks.NewTokenAuth("test-token"),
	}
	// Built the way the product builds it, so what these tests drive is the paced client
	// a scrape actually gets rather than one assembled here.
	return NewDatabricksScrapper(context.Background(), conf)
}

// refusedRequest is one refusal the fake answers with.
type refusedRequest struct {
	status     int
	retryAfter string
	errorCode  string
	message    string
}

// rateLimited is the refusal a workspace over its control-plane quota answers with.
func rateLimited() *refusedRequest {
	return &refusedRequest{
		status:    http.StatusTooManyRequests,
		errorCode: "REQUEST_LIMIT_EXCEEDED",
		message:   "Rate limit exceeded. Please try again later.",
	}
}

// gate puts the refusal hook in front of every endpoint the fake serves.
func (f *fakeWorkspace) gate(t *testing.T, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.served++
		refuse := f.refuse
		f.mu.Unlock()

		if refuse != nil {
			if refused := refuse(r); refused != nil {
				if refused.retryAfter != "" {
					w.Header().Set("Retry-After", refused.retryAfter)
				}
				f.writeApiError(t, w, refused.status, refused.errorCode, refused.message)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// servedRequests is how many requests the fake answered, refusals included.
func (f *fakeWorkspace) servedRequests() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.served
}

// table returns one table by its full name.
func (f *fakeWorkspace) table(fullName string) (servicecatalog.TableInfo, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, tables := range f.tables {
		for _, table := range tables {
			if table.FullName == fullName {
				return table, true
			}
		}
	}
	return servicecatalog.TableInfo{}, false
}

// record appends to one of the request logs. Page tokens mean one listing can be
// served over several requests, so an entry can repeat.
func (f *fakeWorkspace) record(into *[]string, key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	*into = append(*into, key)
}

// requested reports whether any listing named key.
func (f *fakeWorkspace) requested(log *[]string, key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, seen := range *log {
		if seen == key {
			return true
		}
	}
	return false
}

type pageRange struct{ from, to int }

// page resolves the slice of a listing the request asked for, and the token that
// reaches the next one.
func (f *fakeWorkspace) page(t *testing.T, r *http.Request, total int) (pageRange, string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.pageSize <= 0 {
		return pageRange{0, total}, ""
	}
	from := 0
	if token := r.URL.Query().Get("page_token"); token != "" {
		parsed, err := strconv.Atoi(token)
		if err != nil {
			t.Errorf("unexpected page token %q", token)
		}
		from = parsed
	}
	to := min(from+f.pageSize, total)
	if to < total {
		return pageRange{from, to}, strconv.Itoa(to)
	}
	return pageRange{from, to}, ""
}

// rejectsUnpaginated answers the way Unity Catalog answers a listing request that
// asked for no page size: refused, because the result set is larger than one
// response carries.
func (f *fakeWorkspace) rejectsUnpaginated(t *testing.T, w http.ResponseWriter, r *http.Request, operation string) bool {
	t.Helper()
	if r.URL.Query().Has("max_results") {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	body := map[string]any{
		"error_code": "INVALID_PARAMETER_VALUE",
		"message": fmt.Sprintf(
			"The %s result set is too large to return in a single response. Please adopt the "+
				"paginated version of this API by setting max_results and paging through results "+
				"with page_token. Error code #UC-PGRQD", operation,
		),
	}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		t.Errorf("failed to encode fake Databricks response: %v", err)
	}
	return true
}

// writeVanished answers the way Unity Catalog answers a listing that names a catalog
// or schema which no longer exists.
func (f *fakeWorkspace) writeVanished(t *testing.T, w http.ResponseWriter, errorCode, message string) {
	t.Helper()
	f.writeApiError(t, w, http.StatusNotFound, errorCode, message)
}

func (f *fakeWorkspace) writeApiError(t *testing.T, w http.ResponseWriter, status int, errorCode, message string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body := map[string]any{"error_code": errorCode, "message": message}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		t.Errorf("failed to encode fake Databricks response: %v", err)
	}
}

// writeJson answers with rows under key, or with rows as the whole body when key is
// empty — the shape of every endpoint that returns one object rather than a listing.
func (f *fakeWorkspace) writeJson(t *testing.T, w http.ResponseWriter, status int, key string, rows any, nextPageToken string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if key == "" {
		if err := json.NewEncoder(w).Encode(rows); err != nil {
			t.Errorf("failed to encode fake Databricks response: %v", err)
		}
		return
	}
	body := map[string]any{key: rows}
	if nextPageToken != "" {
		body["next_page_token"] = nextPageToken
	}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		t.Errorf("failed to encode fake Databricks response: %v", err)
	}
}
