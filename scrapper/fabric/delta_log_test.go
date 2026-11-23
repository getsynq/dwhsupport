package fabric

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/getsynq/dwhsupport/lazy"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func readCommitFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile("testdata/delta_log/" + name + ".json")
	require.NoError(t, err)
	return body
}

var written = time.Date(2026, 11, 23, 19, 28, 6, 0, time.UTC)

// The fixtures are commits Fabric wrote: a Data Factory Copy into a Lakehouse
// table, and the Warehouse engine for one table that went through INSERT,
// UPDATE, DELETE and TRUNCATE.
func TestParseDeltaCommit(t *testing.T) {
	tests := []struct {
		fixture    string
		dataChange bool
		operation  string
		inserted   *int64
		deleted    *int64
	}{
		{fixture: "lakehouse_copy_create", operation: "OTHER"},
		// A Copy records "Update" for an append, with no metrics: the actions tell it is an insert.
		{fixture: "lakehouse_copy_append", dataChange: true, operation: "INSERT", inserted: lo.ToPtr[int64](8)},
		{fixture: "warehouse_create", operation: "OTHER"},
		{fixture: "warehouse_insert", dataChange: true, operation: "INSERT", inserted: lo.ToPtr[int64](4)},
		// UPDATE of one row: the deletion vector of the old file marks it deleted, a new file holds it.
		{fixture: "warehouse_update", dataChange: true, operation: "OTHER", inserted: lo.ToPtr[int64](1), deleted: lo.ToPtr[int64](1)},
		// DELETE of two rows: the deletion vector grows from 1 to 3.
		{fixture: "warehouse_delete", dataChange: true, operation: "DELETE", deleted: lo.ToPtr[int64](2)},
		// TRUNCATE removes whole files with no record count, so the deleted rows are not known.
		{fixture: "warehouse_truncate", dataChange: true, operation: "DELETE"},
	}
	for _, tt := range tests {
		t.Run(tt.fixture, func(t *testing.T) {
			commit, err := parseDeltaCommit(3, written, readCommitFixture(t, tt.fixture))
			require.NoError(t, err)
			require.Equal(t, int64(3), commit.Version)
			require.Equal(t, written, commit.Time)
			require.Equal(t, tt.dataChange, commit.DataChange)
			require.Equal(t, tt.operation, commit.Operation)
			require.Equal(t, tt.inserted, commit.RowsInserted)
			require.Equal(t, tt.deleted, commit.RowsDeleted)
			require.Nil(t, commit.RowsUpdated)
		})
	}
}

// A Spark writer (a notebook, a Spark job) records its own metrics. These
// commits follow the Delta protocol and the metric names of DESCRIBE HISTORY.
func TestParseDeltaCommitSparkMetrics(t *testing.T) {
	add := `{"add":{"path":"a.parquet","dataChange":true,"stats":"{\"numRecords\":10}"}}`
	remove := `{"remove":{"path":"b.parquet","dataChange":true}}`
	tests := []struct {
		name      string
		body      string
		operation string
		inserted  *int64
		updated   *int64
		deleted   *int64
	}{
		{
			name:      "append",
			body:      add + "\n" + `{"commitInfo":{"operation":"WRITE","operationMetrics":{"numFiles":"1","numOutputRows":"10"}}}`,
			operation: "INSERT",
			inserted:  lo.ToPtr[int64](10),
		},
		{
			name: "merge",
			body: add + "\n" + remove + "\n" +
				`{"commitInfo":{"operation":"MERGE","operationMetrics":{"numOutputRows":"12","numTargetRowsInserted":"3","numTargetRowsUpdated":"5","numTargetRowsDeleted":"1"}}}`,
			operation: "MERGE",
			inserted:  lo.ToPtr[int64](3),
			updated:   lo.ToPtr[int64](5),
			deleted:   lo.ToPtr[int64](1),
		},
		{
			// numOutputRows of an UPDATE counts the copied rows too, so it is not taken as inserted.
			name:      "update",
			body:      add + "\n" + remove + "\n" + `{"commitInfo":{"operation":"UPDATE","operationMetrics":{"numOutputRows":"10","numUpdatedRows":"2"}}}`,
			operation: "UPDATE",
			updated:   lo.ToPtr[int64](2),
		},
		{
			name:      "delete",
			body:      remove + "\n" + `{"commitInfo":{"operation":"DELETE","operationMetrics":{"numDeletedRows":7}}}`,
			operation: "DELETE",
			deleted:   lo.ToPtr[int64](7),
		},
		{
			name:      "unknown operation",
			body:      add + "\n" + `{"commitInfo":{"operation":"RESTORE","operationMetrics":{"numRestoredFiles":"1"}}}`,
			operation: "OTHER",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			commit, err := parseDeltaCommit(1, written, []byte(tt.body))
			require.NoError(t, err)
			require.True(t, commit.DataChange)
			require.Equal(t, tt.operation, commit.Operation)
			require.Equal(t, tt.inserted, commit.RowsInserted)
			require.Equal(t, tt.updated, commit.RowsUpdated)
			require.Equal(t, tt.deleted, commit.RowsDeleted)
		})
	}
}

func TestParseDeltaCommitEdges(t *testing.T) {
	t.Run("a compaction changes no data", func(t *testing.T) {
		body := `{"remove":{"path":"a.parquet","dataChange":false}}
{"add":{"path":"b.parquet","dataChange":false,"stats":"{\"numRecords\":10}"}}
{"commitInfo":{"operation":"OPTIMIZE","operationMetrics":{"numAddedFiles":"1"}}}`
		commit, err := parseDeltaCommit(5, written, []byte(body))
		require.NoError(t, err)
		require.False(t, commit.DataChange)
		require.Nil(t, commit.RowsInserted)
	})
	t.Run("an in-commit timestamp is the time of the commit", func(t *testing.T) {
		body := `{"commitInfo":{"inCommitTimestamp":1795462081123,"operation":"WRITE"}}
{"add":{"path":"a.parquet","dataChange":true,"stats":"{\"numRecords\":1}"}}`
		commit, err := parseDeltaCommit(5, written, []byte(body))
		require.NoError(t, err)
		require.Equal(t, time.UnixMilli(1795462081123).UTC(), commit.Time)
	})
	t.Run("a new file with no record count leaves the inserted rows unknown", func(t *testing.T) {
		body := `{"add":{"path":"a.parquet","dataChange":true,"stats":"{\"numRecords\":3}"}}
{"add":{"path":"b.parquet","dataChange":true}}`
		commit, err := parseDeltaCommit(5, written, []byte(body))
		require.NoError(t, err)
		require.Equal(t, "INSERT", commit.Operation)
		require.Nil(t, commit.RowsInserted)
	})
	t.Run("a removed file with a record count counts its live rows", func(t *testing.T) {
		body := `{"remove":{"path":"a.parquet","dataChange":true,"stats":"{\"numRecords\":5}","deletionVector":{"cardinality":2}}}`
		commit, err := parseDeltaCommit(5, written, []byte(body))
		require.NoError(t, err)
		require.Equal(t, "DELETE", commit.Operation)
		require.Equal(t, lo.ToPtr[int64](3), commit.RowsDeleted)
	})
	t.Run("an empty commit changes no data", func(t *testing.T) {
		commit, err := parseDeltaCommit(5, written, nil)
		require.NoError(t, err)
		require.False(t, commit.DataChange)
	})
	t.Run("a line that is not JSON fails", func(t *testing.T) {
		_, err := parseDeltaCommit(5, written, []byte("{\"add\":"))
		require.Error(t, err)
	})
}

func TestTableDirs(t *testing.T) {
	// A Warehouse, or a Lakehouse with schemas: every schema is a directory.
	require.Equal(t, []string{"Tables/sales/orders"}, tableDirs(true, "sales", "orders"))
	require.Equal(t, []string{"Tables/dbo/orders", "Tables/orders"}, tableDirs(true, "dbo", "orders"))
	// A Lakehouse with no schemas: its tables are at the top, and SQL shows them in dbo.
	require.Equal(t, []string{"Tables/orders", "Tables/dbo/orders"}, tableDirs(false, "dbo", "orders"))
	require.Equal(t, []string{"Tables/sales/orders"}, tableDirs(false, "sales", "orders"))
}

func TestStorageItemsByName(t *testing.T) {
	lake := &dwhexecfabric.StorageItem{ID: "aaaa", Type: dwhexecfabric.ItemTypeLakehouse, DisplayName: "lake"}
	sales := &dwhexecfabric.StorageItem{ID: "cccc", Type: dwhexecfabric.ItemTypeWarehouse, DisplayName: "sales"}
	got := storageItemsByName([]*dwhexecfabric.StorageItem{
		lake,
		sales,
		{ID: "eeee", Type: dwhexecfabric.ItemTypeWarehouse, DisplayName: "twice"},
		{ID: "ffff", Type: dwhexecfabric.ItemTypeMirroredDatabase, DisplayName: "twice"},
	})
	require.Equal(t, map[string]*dwhexecfabric.StorageItem{"lake": lake, "sales": sales, "twice": nil}, got)
}

// fakeWorkspace is the Fabric REST API and OneLake of one workspace.
type fakeWorkspace struct {
	t           *testing.T
	workspaceID string
	items       []*dwhexecfabric.StorageItem
	itemsStatus int
	// files maps "<item>/<path>" to a file.
	files map[string]fakeFile
	// refused maps a "<item>/<directory>" prefix to the status OneLake answers for it.
	refused  map[string]int
	pageSize int

	mu        sync.Mutex
	itemsHits int
	reads     []string
	// listings counts the listing requests, and listed the entries they gave.
	listings int
	listed   int
	// delay holds each OneLake request, and maxInFlight is the most requests
	// that OneLake served at the same time.
	delay       time.Duration
	inFlight    int
	maxInFlight int
}

func (w *fakeWorkspace) listingCost() (requests, entries int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.listings, w.listed
}

type fakeFile struct {
	body    []byte
	written time.Time
}

type fakeCredential struct{}

func (fakeCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func (w *fakeWorkspace) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/v1/") {
		w.serveAPI(rw, r)
		return
	}
	w.serveLake(rw, r)
}

// typedListings maps the path of each typed listing to its item type.
var typedListings = map[string]string{
	"lakehouses":        dwhexecfabric.ItemTypeLakehouse,
	"warehouses":        dwhexecfabric.ItemTypeWarehouse,
	"mirroredDatabases": dwhexecfabric.ItemTypeMirroredDatabase,
	"sqlDatabases":      dwhexecfabric.ItemTypeSQLDatabase,
}

func (w *fakeWorkspace) serveAPI(rw http.ResponseWriter, r *http.Request) {
	listing, ok := strings.CutPrefix(r.URL.Path, "/v1/workspaces/"+w.workspaceID+"/")
	itemType, known := typedListings[listing]
	require.True(w.t, ok && known, "unexpected API call %s", r.URL.Path)
	if listing == "lakehouses" {
		// The first of the four typed listings counts the listing.
		w.mu.Lock()
		w.itemsHits++
		w.mu.Unlock()
	}
	if w.itemsStatus != 0 {
		rw.WriteHeader(w.itemsStatus)
		_, _ = rw.Write([]byte(`{"errorCode":"InsufficientWorkspaceRole","message":"no"}`))
		return
	}
	var value []map[string]any
	for _, it := range w.items {
		if it.Type == itemType {
			value = append(value, map[string]any{
				"id":          it.ID,
				"displayName": it.DisplayName,
				"properties":  map[string]any{"defaultSchema": it.DefaultSchema},
			})
		}
	}
	_ = json.NewEncoder(rw).Encode(map[string]any{"value": value})
}

func (w *fakeWorkspace) refusal(p string) int {
	for prefix, status := range w.refused {
		if strings.HasPrefix(p, prefix) {
			return status
		}
	}
	return 0
}

func lakeError(rw http.ResponseWriter, status int, code string) {
	rw.Header().Set("x-ms-error-code", code)
	rw.WriteHeader(status)
	_, _ = fmt.Fprintf(rw, `{"error":{"code":%q,"message":"fake"}}`, code)
}

func (w *fakeWorkspace) serveLake(rw http.ResponseWriter, r *http.Request) {
	w.mu.Lock()
	w.inFlight++
	w.maxInFlight = max(w.maxInFlight, w.inFlight)
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.inFlight--
		w.mu.Unlock()
	}()
	time.Sleep(w.delay)
	segments := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/lake/"), "/", 2)
	require.Equal(w.t, w.workspaceID, segments[0], "OneLake read outside the workspace")
	if len(segments) == 2 {
		w.mu.Lock()
		w.reads = append(w.reads, segments[1])
		w.mu.Unlock()
		if status := w.refusal(segments[1]); status != 0 {
			lakeError(rw, status, "AuthorizationPermissionMismatch")
			return
		}
		f, ok := w.files[segments[1]]
		if !ok {
			lakeError(rw, http.StatusNotFound, "PathNotFound")
			return
		}
		_, _ = rw.Write(f.body)
		return
	}

	q := r.URL.Query()
	require.Equal(w.t, "filesystem", q.Get("resource"))
	require.Equal(w.t, "false", q.Get("recursive"))
	dir := q.Get("directory")
	if status := w.refusal(dir); status != 0 {
		lakeError(rw, status, "AuthorizationPermissionMismatch")
		return
	}
	children := map[string]map[string]any{}
	for p, f := range w.files {
		rest, ok := strings.CutPrefix(p, dir+"/")
		if !ok {
			continue
		}
		name, _, nested := strings.Cut(rest, "/")
		entry := map[string]any{"name": dir + "/" + name, "lastModified": f.written.Format(http.TimeFormat)}
		if nested {
			entry["isDirectory"] = "true"
		} else {
			entry["contentLength"] = fmt.Sprint(len(f.body))
		}
		children[name] = entry
	}
	if len(children) == 0 {
		lakeError(rw, http.StatusNotFound, "PathNotFound")
		return
	}
	names := lo.Keys(children)
	sort.Strings(names)
	// beginFrom starts the listing at the first name at or after it, as OneLake does.
	start := sort.SearchStrings(names, q.Get("beginFrom"))
	if c := q.Get("continuation"); c != "" {
		_, _ = fmt.Sscan(c, &start)
	}
	pageSize := w.pageSize
	if m, _ := strconv.Atoi(q.Get("maxResults")); m > 0 && (pageSize == 0 || m < pageSize) {
		pageSize = m
	}
	end := len(names)
	if pageSize > 0 && start+pageSize < end {
		end = start + pageSize
		rw.Header().Set("x-ms-continuation", fmt.Sprint(end))
	}
	var paths []map[string]any
	for _, n := range names[start:end] {
		paths = append(paths, children[n])
	}
	w.mu.Lock()
	w.listings++
	w.listed += len(paths)
	w.mu.Unlock()
	_ = json.NewEncoder(rw).Encode(map[string]any{"paths": paths})
}

func (w *fakeWorkspace) commit(item, dir string, version int64, at time.Time, body string) {
	w.files[item+"/"+dir+"/_delta_log/"+fmt.Sprintf("%020d.json", version)] = fakeFile{body: []byte(body), written: at}
}

func (w *fakeWorkspace) readsOf(suffix string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(lo.Filter(w.reads, func(p string, _ int) bool { return strings.HasSuffix(p, suffix) }))
}

func (w *fakeWorkspace) reader(t *testing.T) *deltaLogReader {
	srv := httptest.NewServer(w)
	t.Cleanup(srv.Close)
	return &deltaLogReader{
		workspaceID: w.workspaceID,
		lake:        dwhexecfabric.NewOneLakeClientWith(fakeCredential{}, srv.URL+"/lake", srv.Client()),
		items:       dwhexecfabric.NewAPIClientWith(fakeCredential{}, srv.URL+"/v1", srv.Client()),
		slots:       make(chan struct{}, maxDeltaLogConcurrency),
	}
}

const (
	lakeItem  = "1a1a1a1a-0000-0000-0000-000000000001"
	salesItem = "2b2b2b2b-0000-0000-0000-000000000002"
)

var (
	t0           = time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)
	appendCommit = `{"add":{"path":"%s.parquet","dataChange":true,"stats":"{\"numRecords\":%d}"}}`
	compaction   = `{"remove":{"path":"x.parquet","dataChange":false}}
{"add":{"path":"y.parquet","dataChange":false,"stats":"{\"numRecords\":3}"}}
{"commitInfo":{"operation":"OPTIMIZE","operationMetrics":{"numAddedFiles":"1"}}}`
	createCommit = `{"metaData":{"id":"m","format":{"provider":"parquet"},"schemaString":"{}","partitionColumns":[]}}`
)

// seedWorkspace has a Lakehouse "lake" with no schemas and a Warehouse "sales".
func seedWorkspace(t *testing.T) *fakeWorkspace {
	w := &fakeWorkspace{
		t:           t,
		workspaceID: "43531ef0-2f75-4b08-9e6d-e157dbc6c6fa",
		items: []*dwhexecfabric.StorageItem{
			// A Lakehouse with no schemas has no default schema.
			{ID: lakeItem, Type: dwhexecfabric.ItemTypeLakehouse, DisplayName: "lake"},
			{ID: salesItem, Type: dwhexecfabric.ItemTypeWarehouse, DisplayName: "sales"},
		},
		files: map[string]fakeFile{},
	}
	// lake: dbo.orders is at Tables/orders.
	w.commit(lakeItem, "Tables/orders", 0, t0, createCommit)
	w.commit(lakeItem, "Tables/orders", 1, t0.Add(time.Hour), fmt.Sprintf(appendCommit, "o1", 8))
	// sales: sales.orders was loaded, then compacted. The compaction is not a load.
	w.commit(salesItem, "Tables/sales/orders", 0, t0, createCommit)
	w.commit(salesItem, "Tables/sales/orders", 1, t0.Add(2*time.Hour), fmt.Sprintf(appendCommit, "s1", 4))
	w.commit(salesItem, "Tables/sales/orders", 2, t0.Add(3*time.Hour), compaction)
	// sales: dbo.empty was created and never written.
	w.commit(salesItem, "Tables/dbo/empty", 0, t0.Add(4*time.Hour), createCommit)
	return w
}

func metricsRoutes(database string, tables ...[2]string) []fakeRoute {
	db := "[" + database + "]."
	var listRows, countRows [][]driver.Value
	for i, tbl := range tables {
		listRows = append(listRows, []driver.Value{tbl[0], tbl[1]})
		countRows = append(countRows, []driver.Value{tbl[0], tbl[1], int64(10 * (i + 1))})
	}
	return []fakeRoute{
		{match: sqlContains(db + "sys.tables t"), columns: []string{"schema", "table"}, rows: listRows},
		{match: sqlContains("COUNT_BIG(*)", db), columns: []string{"schema", "table", "row_count"}, rows: countRows},
	}
}

func metricsScrapper(t *testing.T, w *fakeWorkspace, routes ...[]fakeRoute) *FabricScrapper {
	sc, _ := newFakeFabricScrapper(t, routes...)
	reader := w.reader(t)
	sc.deltaLog = lazy.New(func() (*deltaLogReader, error) { return reader, nil })
	sc.conf.FetchLastLoadTime = true
	return sc
}

func updatedAtByTable(rows []*scrapper.TableMetricsRow) map[string]*time.Time {
	out := map[string]*time.Time{}
	for _, r := range rows {
		out[r.Database+"."+r.Schema+"."+r.Table] = r.UpdatedAt
	}
	return out
}

func TestQueryTableMetricsReadsTheLastChangeFromTheDeltaLog(t *testing.T) {
	w := seedWorkspace(t)
	sc := metricsScrapper(t, w,
		databasesRoute("lake", "sales"),
		metricsRoutes("lake", [2]string{"dbo", "orders"}),
		metricsRoutes("sales", [2]string{"sales", "orders"}, [2]string{"dbo", "empty"}, [2]string{"sales", "gone"}),
	)

	rows, err := sc.QueryTableMetrics(context.Background(), time.Time{})
	require.NoError(t, err)
	require.Equal(t, map[string]*time.Time{
		"lake.dbo.orders":    lo.ToPtr(t0.Add(time.Hour)),
		"sales.sales.orders": lo.ToPtr(t0.Add(2 * time.Hour)),
		"sales.dbo.empty":    lo.ToPtr(t0.Add(4 * time.Hour)),
		// The table has no Delta log, so it has no last change.
		"sales.sales.gone": nil,
	}, updatedAtByTable(rows))
	for _, r := range rows {
		require.NotNil(t, r.RowCount, "the row count stays")
	}
	require.Equal(t, 1, w.itemsHits, "one items listing for all databases")
}

func TestQueryTableMetricsKeepsRowCountsWhenOneLakeFails(t *testing.T) {
	routes := [][]fakeRoute{
		databasesRoute("lake", "sales"),
		metricsRoutes("lake", [2]string{"dbo", "orders"}),
		metricsRoutes("sales", [2]string{"sales", "orders"}),
	}
	tests := []struct {
		name  string
		setup func(w *fakeWorkspace)
		want  map[string]*time.Time
	}{
		{
			name:  "the items listing is refused",
			setup: func(w *fakeWorkspace) { w.itemsStatus = http.StatusForbidden },
			want:  map[string]*time.Time{"lake.dbo.orders": nil, "sales.sales.orders": nil},
		},
		{
			name:  "OneLake refuses one item",
			setup: func(w *fakeWorkspace) { w.refused = map[string]int{salesItem: http.StatusForbidden} },
			want:  map[string]*time.Time{"lake.dbo.orders": lo.ToPtr(t0.Add(time.Hour)), "sales.sales.orders": nil},
		},
		{
			name: "OneLake refuses one table",
			setup: func(w *fakeWorkspace) {
				w.refused = map[string]int{salesItem + "/Tables/sales/orders": http.StatusForbidden}
			},
			want: map[string]*time.Time{"lake.dbo.orders": lo.ToPtr(t0.Add(time.Hour)), "sales.sales.orders": nil},
		},
		{
			name: "OneLake fails one commit",
			setup: func(w *fakeWorkspace) {
				w.refused = map[string]int{salesItem + "/Tables/sales/orders/_delta_log/0": http.StatusInternalServerError}
			},
			want: map[string]*time.Time{"lake.dbo.orders": lo.ToPtr(t0.Add(time.Hour)), "sales.sales.orders": nil},
		},
		{
			name: "two storage items have the name of the database",
			setup: func(w *fakeWorkspace) {
				w.items = append(
					w.items,
					&dwhexecfabric.StorageItem{
						ID:          "4d4d4d4d-0000-0000-0000-000000000004",
						Type:        dwhexecfabric.ItemTypeMirroredDatabase,
						DisplayName: "sales",
					},
				)
			},
			want: map[string]*time.Time{"lake.dbo.orders": lo.ToPtr(t0.Add(time.Hour)), "sales.sales.orders": nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := seedWorkspace(t)
			tt.setup(w)
			rows, err := metricsScrapper(t, w, routes...).QueryTableMetrics(context.Background(), time.Time{})
			require.NoError(t, err)
			require.Equal(t, tt.want, updatedAtByTable(rows))
			for _, r := range rows {
				require.NotNil(t, r.RowCount)
			}
		})
	}
}

func TestQueryTableMetricsReadsNoDeltaLogWhenTheLastLoadTimeIsOff(t *testing.T) {
	w := seedWorkspace(t)
	sc := metricsScrapper(t, w, databasesRoute("sales"), metricsRoutes("sales", [2]string{"sales", "orders"}))
	sc.conf.FetchLastLoadTime = false
	rows, err := sc.QueryTableMetrics(context.Background(), time.Time{})
	require.NoError(t, err)
	require.Equal(t, map[string]*time.Time{"sales.sales.orders": nil}, updatedAtByTable(rows))
	require.NotNil(t, rows[0].RowCount)
	requests, _ := w.listingCost()
	require.Zero(t, requests, "no OneLake listing")
	require.Empty(t, w.reads, "no OneLake read")
	require.Zero(t, w.itemsHits, "no items listing")
}

func TestQueryTableMetricsReadsTheDeltaLogsOfAllDatabasesWithinOneCap(t *testing.T) {
	w := seedWorkspace(t)
	w.delay = 5 * time.Millisecond
	var lakeTables, salesTables [][2]string
	for i := range 3 * maxDeltaLogConcurrency {
		name := fmt.Sprintf("t%03d", i)
		w.commit(lakeItem, "Tables/"+name, 0, t0, fmt.Sprintf(appendCommit, name, 1))
		w.commit(salesItem, "Tables/sales/"+name, 0, t0, fmt.Sprintf(appendCommit, name, 1))
		lakeTables = append(lakeTables, [2]string{"dbo", name})
		salesTables = append(salesTables, [2]string{"sales", name})
	}
	sc := metricsScrapper(t, w, databasesRoute("lake", "sales"), metricsRoutes("lake", lakeTables...), metricsRoutes("sales", salesTables...))

	rows, err := sc.QueryTableMetrics(context.Background(), time.Time{})
	require.NoError(t, err)
	for _, r := range rows {
		require.NotNil(t, r.UpdatedAt, r.Table)
	}
	require.LessOrEqual(t, w.maxInFlight, maxDeltaLogConcurrency, "one cap for all databases")
	require.Greater(t, w.maxInFlight, maxDeltaLogConcurrency/2, "the reads run in parallel")
}

func TestQueryTableMetricsWithNoOneLakeAccess(t *testing.T) {
	// A connection with only a pre-acquired SQL token cannot reach OneLake.
	sc, _ := newFakeFabricScrapper(t, databasesRoute("sales"), metricsRoutes("sales", [2]string{"sales", "orders"}))
	sc.conf.FetchLastLoadTime = true
	rows, err := sc.QueryTableMetrics(context.Background(), time.Time{})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Nil(t, rows[0].UpdatedAt)
	require.Equal(t, lo.ToPtr[int64](10), rows[0].RowCount)
}

func TestLastChangeReadsAtMostTheNewestCommits(t *testing.T) {
	w := seedWorkspace(t)
	dir := "Tables/sales/busy"
	w.commit(salesItem, dir, 0, t0, createCommit)
	w.commit(salesItem, dir, 1, t0.Add(time.Minute), fmt.Sprintf(appendCommit, "b1", 1))
	for v := int64(2); v < 2+maxCommitsForLastChange; v++ {
		w.commit(salesItem, dir, v, t0.Add(time.Duration(v)*time.Minute), compaction)
	}
	reader := w.reader(t)

	at, err := reader.lastChange(context.Background(), salesItem, []string{dir})
	require.NoError(t, err)
	require.Nil(t, at, "no data change in the commits it reads, and older ones exist")
	require.Equal(t, maxCommitsForLastChange, w.readsOf(".json"))
}

func TestCommitListingFollowsTheContinuation(t *testing.T) {
	w := seedWorkspace(t)
	w.pageSize = 2
	dir := "Tables/sales/paged"
	for v := int64(0); v < 5; v++ {
		w.commit(salesItem, dir, v, t0.Add(time.Duration(v)*time.Minute), fmt.Sprintf(appendCommit, fmt.Sprint(v), 1))
	}
	// A checkpoint and a CRC file are in the log too, and are not commits.
	w.files[salesItem+"/"+dir+"/_delta_log/00000000000000000004.checkpoint.parquet"] = fakeFile{written: t0}
	w.files[salesItem+"/"+dir+"/_delta_log/00000000000000000004.crc"] = fakeFile{written: t0}

	_, commits, err := w.reader(t).commitsOf(context.Background(), salesItem, []string{dir})
	require.NoError(t, err)
	require.Equal(t, []int64{4, 3, 2, 1, 0}, lo.Map(commits, func(c *deltaCommitFile, _ int) int64 { return c.Version }))
	require.Equal(t, t0.Add(4*time.Minute), commits[0].Written)
}

// longLog writes a log of n commits, each with its CRC file, and a checkpoint
// every checkpointEvery commits with _last_checkpoint (0: none). A Spark writer
// checkpoints every 10 commits, the Warehouse about every 100 to 120.
func longLog(w *fakeWorkspace, dir string, n, checkpointEvery int64) {
	for v := int64(0); v < n; v++ {
		w.commit(salesItem, dir, v, t0.Add(time.Duration(v)*time.Minute), fmt.Sprintf(appendCommit, fmt.Sprint(v), 1))
		w.files[salesItem+"/"+dir+"/_delta_log/"+fmt.Sprintf("%020d.crc", v)] = fakeFile{written: t0}
		if checkpointEvery > 0 && v > 0 && v%checkpointEvery == 0 {
			w.files[salesItem+"/"+dir+"/_delta_log/"+fmt.Sprintf("%020d.checkpoint.parquet", v)] = fakeFile{written: t0}
			w.files[salesItem+"/"+dir+"/_delta_log/_last_checkpoint"] = fakeFile{
				body:    []byte(fmt.Sprintf(`{"version":%d,"size":4}`, v)),
				written: t0,
			}
		}
	}
}

func TestLastChangeOfALongLogListsOnlyItsEnd(t *testing.T) {
	const commits = 3000
	for _, every := range []int64{10, 117} {
		t.Run(fmt.Sprintf("checkpoint every %d", every), func(t *testing.T) {
			w := seedWorkspace(t)
			w.pageSize = 500
			dir := "Tables/sales/long"
			longLog(w, dir, commits, every)

			at, err := w.reader(t).lastChange(context.Background(), salesItem, []string{dir})
			require.NoError(t, err)
			require.Equal(t, lo.ToPtr(t0.Add((commits-1)*time.Minute)), at)
			requests, entries := w.listingCost()
			// The whole log is more than 6000 entries in 13 pages.
			require.Equal(t, 1, requests, "listing requests")
			require.LessOrEqual(t, entries, int(3*(every+maxCommitsForLastChange)), "listed entries")
			require.Equal(t, 1, w.readsOf("_last_checkpoint"))
			require.Equal(t, 1, w.readsOf(".json"), "one commit read")
		})
	}
}

func TestLastChangeOfALogWithNoCheckpoint(t *testing.T) {
	// A short log has no checkpoint yet, so the whole log is listed.
	w := seedWorkspace(t)
	dir := "Tables/sales/short"
	longLog(w, dir, 14, 0)
	at, err := w.reader(t).lastChange(context.Background(), salesItem, []string{dir})
	require.NoError(t, err)
	require.Equal(t, lo.ToPtr(t0.Add(13*time.Minute)), at)
}

func TestLastChangeOfALogWithItsOldVersionsCleanedUp(t *testing.T) {
	w := seedWorkspace(t)
	dir := "Tables/sales/cleaned"
	longLog(w, dir, 700, 0)
	for v := int64(0); v < 500; v++ {
		delete(w.files, salesItem+"/"+dir+"/_delta_log/"+fmt.Sprintf("%020d.json", v))
		delete(w.files, salesItem+"/"+dir+"/_delta_log/"+fmt.Sprintf("%020d.crc", v))
	}
	at, err := w.reader(t).lastChange(context.Background(), salesItem, []string{dir})
	require.NoError(t, err)
	require.Equal(t, lo.ToPtr(t0.Add(699*time.Minute)), at)
}

func TestLastChangeWithACheckpointPastTheLog(t *testing.T) {
	// _last_checkpoint names a version that the log does not have, so the search
	// starts again at 0.
	w := seedWorkspace(t)
	dir := "Tables/sales/stale"
	w.commit(salesItem, dir, 0, t0, createCommit)
	w.commit(salesItem, dir, 1, t0.Add(time.Hour), fmt.Sprintf(appendCommit, "a", 1))
	w.commit(salesItem, dir, 2, t0.Add(2*time.Hour), compaction)
	w.files[salesItem+"/"+dir+"/_delta_log/_last_checkpoint"] = fakeFile{body: []byte(`{"version":300}`), written: t0}

	at, err := w.reader(t).lastChange(context.Background(), salesItem, []string{dir})
	require.NoError(t, err)
	require.Equal(t, lo.ToPtr(t0.Add(time.Hour)), at)
}

func TestLastChangeOfAnEmptyLog(t *testing.T) {
	w := seedWorkspace(t)
	dir := "Tables/sales/empty_log"
	// Only a file that is no version.
	w.files[salesItem+"/"+dir+"/_delta_log/_last_checkpoint"] = fakeFile{body: []byte(`{"version":40}`), written: t0}
	at, err := w.reader(t).lastChange(context.Background(), salesItem, []string{dir})
	require.NoError(t, err)
	require.Nil(t, at)
}

func historyWorkspace(t *testing.T) *fakeWorkspace {
	w := seedWorkspace(t)
	dir := "Tables/sales/history"
	w.commit(salesItem, dir, 0, t0, createCommit)
	w.commit(salesItem, dir, 1, t0.Add(1*time.Hour), fmt.Sprintf(appendCommit, "h1", 5))
	w.commit(salesItem, dir, 2, t0.Add(2*time.Hour), compaction)
	w.commit(salesItem, dir, 3, t0.Add(3*time.Hour), string(readCommitFixture(t, "warehouse_delete")))
	w.commit(salesItem, dir, 4, t0.Add(4*time.Hour), fmt.Sprintf(appendCommit, "h4", 2))
	return w
}

func TestHistoryFiltersByTheCommitTime(t *testing.T) {
	w := seedWorkspace(t)
	dir := "Tables/sales/ict"
	ict := func(at time.Time, file string) string {
		return fmt.Sprintf(`{"commitInfo":{"inCommitTimestamp":%d,"operation":"WRITE"}}
`, at.UnixMilli()) + fmt.Sprintf(appendCommit, file, 1)
	}
	committed := t0.Add(900 * time.Millisecond)
	// The file is written after the commit time, and after the end of the window.
	w.commit(salesItem, dir, 0, t0.Add(time.Second), ict(committed, "a"))
	// The file is in the window, but the commit time is before it.
	w.commit(salesItem, dir, 1, t0.Add(2*time.Second), ict(t0.Add(-time.Second), "b"))

	commits, err := w.reader(t).history(context.Background(), salesItem, []string{dir}, t0, t0.Add(950*time.Millisecond), 10)
	require.NoError(t, err)
	require.Equal(t, []int64{0}, lo.Map(commits, func(c *deltaCommit, _ int) int64 { return c.Version }))
	require.Equal(t, committed, commits[0].Time)
}

func TestFetchTableChangeHistory(t *testing.T) {
	w := historyWorkspace(t)
	sc := metricsScrapper(t, w)
	fqn := scrapper.DwhFqn{DatabaseName: "sales", SchemaName: "sales", ObjectName: "history"}

	events, err := sc.FetchTableChangeHistory(context.Background(), fqn, t0, t0.Add(24*time.Hour), 10)
	require.NoError(t, err)
	require.Equal(t, []*scrapper.TableChangeEvent{
		{Timestamp: t0.Add(4 * time.Hour), Version: "4", Operation: "INSERT", RowsInserted: lo.ToPtr[int64](2)},
		{Timestamp: t0.Add(3 * time.Hour), Version: "3", Operation: "DELETE", RowsDeleted: lo.ToPtr[int64](2)},
		{Timestamp: t0.Add(1 * time.Hour), Version: "1", Operation: "INSERT", RowsInserted: lo.ToPtr[int64](5)},
	}, events, "newest first, with no compaction and no creation")

	events, err = sc.FetchTableChangeHistory(context.Background(), fqn, t0.Add(90*time.Minute), t0.Add(3*time.Hour), 10)
	require.NoError(t, err)
	require.Equal(t, []string{"3"}, lo.Map(events, func(e *scrapper.TableChangeEvent, _ int) string { return e.Version }), "the range is inclusive")

	events, err = sc.FetchTableChangeHistory(context.Background(), fqn, t0, t0.Add(24*time.Hour), 2)
	require.NoError(t, err)
	require.Equal(t, []string{"4", "3"}, lo.Map(events, func(e *scrapper.TableChangeEvent, _ int) string { return e.Version }))

	events, err = sc.FetchTableChangeHistory(context.Background(), fqn, t0, t0.Add(24*time.Hour), 0)
	require.NoError(t, err)
	require.Empty(t, events)
}

func TestFetchTableChangeHistoryOfADboTableInALakehouseWithNoSchemas(t *testing.T) {
	sc := metricsScrapper(t, seedWorkspace(t))
	events, err := sc.FetchTableChangeHistory(context.Background(),
		scrapper.DwhFqn{DatabaseName: "lake", SchemaName: "dbo", ObjectName: "orders"}, t0, t0.Add(24*time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "1", events[0].Version)
}

func TestFetchTableChangeHistoryFailures(t *testing.T) {
	ctx := context.Background()
	window := func(sc *FabricScrapper, fqn scrapper.DwhFqn) error {
		_, err := sc.FetchTableChangeHistory(ctx, fqn, t0, t0.Add(24*time.Hour), 10)
		return err
	}

	err := window(metricsScrapper(t, seedWorkspace(t)), scrapper.DwhFqn{DatabaseName: "sales", SchemaName: "sales", ObjectName: "gone"})
	require.ErrorIs(t, err, errNoDeltaLog)

	err = window(metricsScrapper(t, seedWorkspace(t)), scrapper.DwhFqn{DatabaseName: "nowhere", SchemaName: "dbo", ObjectName: "t"})
	require.ErrorContains(t, err, `no single storage item of the workspace is named "nowhere"`)

	refused := seedWorkspace(t)
	refused.refused = map[string]int{salesItem: http.StatusForbidden}
	err = window(metricsScrapper(t, refused), scrapper.DwhFqn{DatabaseName: "sales", SchemaName: "sales", ObjectName: "orders"})
	require.True(t, dwhexecfabric.IsPermissionError(err), "a OneLake refusal is a permission error: %v", err)

	// The fake connection holds only a SQL token, so it has no REST client.
	sc, _ := newFakeFabricScrapper(t)
	err = window(sc, scrapper.DwhFqn{DatabaseName: "sales", SchemaName: "sales", ObjectName: "orders"})
	require.ErrorIs(t, err, dwhexecfabric.ErrNoAPICredential)

	// The fake host names no workspace.
	_, err = newDeltaLogReader(&FabricScrapperConf{FabricConf: dwhexecfabric.FabricConf{Host: "fake"}}, nil)
	require.ErrorContains(t, err, "the host names no Fabric workspace")

	// A pre-acquired SQL token cannot be used for OneLake.
	_, err = newDeltaLogReader(&FabricScrapperConf{FabricConf: dwhexecfabric.FabricConf{
		Host:        "lvmghcalzphu5hhdgghdffivni-6apfgq3vf4eexhtn4fl5xrwg7i.datawarehouse.fabric.microsoft.com",
		AccessToken: "sql-token",
	}}, nil)
	require.ErrorIs(t, err, dwhexecfabric.ErrNoAPICredential)
}

func TestItemsAreListedOneTimeForEachScrapper(t *testing.T) {
	w := seedWorkspace(t)
	reader := w.reader(t)
	ctx := context.Background()

	_, _, err := reader.itemOf(ctx, "sales")
	require.NoError(t, err)
	item, ok, err := reader.itemOf(ctx, "lake")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, lakeItem, item.ID)
	require.Equal(t, 1, w.itemsHits)
}

func TestFailedItemsListingIsNotCached(t *testing.T) {
	w := seedWorkspace(t)
	w.itemsStatus = http.StatusServiceUnavailable
	reader := w.reader(t)
	ctx := context.Background()

	_, _, err := reader.itemOf(ctx, "sales")
	require.Error(t, err)

	w.itemsStatus = 0
	item, ok, err := reader.itemOf(ctx, "sales")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, salesItem, item.ID)
	require.Equal(t, 2, w.itemsHits)
}

func TestOneLakeReadEscapesThePath(t *testing.T) {
	w := seedWorkspace(t)
	dir := "Tables/sales/order lines"
	w.commit(salesItem, dir, 0, t0, fmt.Sprintf(appendCommit, "l0", 1))
	at, err := w.reader(t).lastChange(context.Background(), salesItem, []string{dir})
	require.NoError(t, err)
	require.Equal(t, lo.ToPtr(t0), at)
	require.Equal(t, 1, w.readsOf(path.Join("order lines", "_delta_log", "00000000000000000000.json")))
}
