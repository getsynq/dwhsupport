package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go"
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

	mu sync.Mutex
}

func newFakeWorkspace() *fakeWorkspace {
	return &fakeWorkspace{
		schemas:          map[string][]servicecatalog.SchemaInfo{},
		tables:           map[string][]servicecatalog.TableInfo{},
		vanishedCatalogs: map[string]bool{},
		vanishedSchemas:  map[string]bool{},
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

func (f *fakeWorkspace) start(t *testing.T) *DatabricksScrapper {
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
		if f.vanishedSchemas[key] {
			f.writeVanished(t, w, "SCHEMA_DOES_NOT_EXIST", fmt.Sprintf("Schema '%s' does not exist.", key))
			return
		}
		tables := f.tables[r.URL.Query().Get("catalog_name")+"."+r.URL.Query().Get("schema_name")]
		page, next := f.page(t, r, len(tables))
		f.writeJson(t, w, http.StatusOK, "tables", tables[page.from:page.to], next)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected Databricks request: %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusNotImplemented)
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client, err := databricks.NewWorkspaceClient(&databricks.Config{Host: server.URL, Token: "test-token"})
	require.NoError(t, err)

	conf := &DatabricksScrapperConf{
		DatabricksConf: dwhexecdatabricks.DatabricksConf{WorkspaceUrl: server.URL},
	}
	return &DatabricksScrapper{client: client, conf: conf, scope: ScopeFromConf(conf)}
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
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	body := map[string]any{"error_code": errorCode, "message": message}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		t.Errorf("failed to encode fake Databricks response: %v", err)
	}
}

func (f *fakeWorkspace) writeJson(t *testing.T, w http.ResponseWriter, status int, key string, rows any, nextPageToken string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body := map[string]any{key: rows}
	if nextPageToken != "" {
		body["next_page_token"] = nextPageToken
	}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		t.Errorf("failed to encode fake Databricks response: %v", err)
	}
}
