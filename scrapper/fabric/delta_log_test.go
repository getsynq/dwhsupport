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
	require.Equal(t, []string{"Tables/sales/orders"}, tableDirs(tableLayout{"sales": true}, "sales", "orders"))
	require.Equal(t, []string{"Tables/dbo/orders", "Tables/orders"}, tableDirs(tableLayout{"dbo": true}, "dbo", "orders"))
	// A Lakehouse with no schemas: its tables are at the top, and SQL shows them in dbo.
	require.Equal(t, []string{"Tables/orders", "Tables/dbo/orders"}, tableDirs(tableLayout{"orders": true}, "dbo", "orders"))
	// No layout: the schema directory first.
	require.Equal(t, []string{"Tables/dbo/orders", "Tables/orders"}, tableDirs(nil, "dbo", "orders"))
}

func TestStorageItemsByName(t *testing.T) {
	got := storageItemsByName([]*dwhexecfabric.WorkspaceItem{
		{ID: "AAAA", Type: "Lakehouse", DisplayName: "lake"},
		{ID: "bbbb", Type: "SQLEndpoint", DisplayName: "lake"},
		{ID: "cccc", Type: "Warehouse", DisplayName: "sales"},
		{ID: "dddd", Type: "DataPipeline", DisplayName: "pipe"},
		{ID: "eeee", Type: "Warehouse", DisplayName: "twice"},
		{ID: "ffff", Type: "MirroredDatabase", DisplayName: "twice"},
		{ID: "0000", Type: "SQLDatabase", DisplayName: "app"},
		{ID: "1111", Type: "MirroredDatabase", DisplayName: "mirror"},
	})
	require.Equal(t, map[string]string{"lake": "aaaa", "sales": "cccc", "twice": "", "app": "0000", "mirror": "1111"}, got)
}

// fakeWorkspace is the Fabric REST API and OneLake of one workspace.
type fakeWorkspace struct {
	t           *testing.T
	workspaceID string
	items       []*dwhexecfabric.WorkspaceItem
	itemsStatus int
	// files maps "<item>/<path>" to a file.
	files map[string]fakeFile
	// refused maps a "<item>/<directory>" prefix to the status OneLake answers for it.
	refused  map[string]int
	pageSize int

	mu        sync.Mutex
	itemsHits int
	reads     []string
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

func (w *fakeWorkspace) serveAPI(rw http.ResponseWriter, r *http.Request) {
	require.Equal(w.t, "/v1/workspaces/"+w.workspaceID+"/items", r.URL.Path)
	w.mu.Lock()
	w.itemsHits++
	w.mu.Unlock()
	if w.itemsStatus != 0 {
		rw.WriteHeader(w.itemsStatus)
		_, _ = rw.Write([]byte(`{"errorCode":"InsufficientWorkspaceRole","message":"no"}`))
		return
	}
	_ = json.NewEncoder(rw).Encode(map[string]any{"value": w.items})
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
	start := 0
	if c := q.Get("continuation"); c != "" {
		_, _ = fmt.Sscan(c, &start)
	}
	end := len(names)
	if w.pageSize > 0 && start+w.pageSize < end {
		end = start + w.pageSize
		rw.Header().Set("x-ms-continuation", fmt.Sprint(end))
	}
	var paths []map[string]any
	for _, n := range names[start:end] {
		paths = append(paths, children[n])
	}
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
		now:         time.Now,
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
		items: []*dwhexecfabric.WorkspaceItem{
			{ID: lakeItem, Type: "Lakehouse", DisplayName: "lake"},
			{ID: "3c3c3c3c-0000-0000-0000-000000000003", Type: "SQLEndpoint", DisplayName: "lake"},
			{ID: salesItem, Type: "Warehouse", DisplayName: "sales"},
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
					&dwhexecfabric.WorkspaceItem{ID: "4d4d4d4d-0000-0000-0000-000000000004", Type: "MirroredDatabase", DisplayName: "sales"},
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

func TestQueryTableMetricsWithNoOneLakeAccess(t *testing.T) {
	// A connection with only a pre-acquired SQL token cannot reach OneLake.
	sc, _ := newFakeFabricScrapper(t, databasesRoute("sales"), metricsRoutes("sales", [2]string{"sales", "orders"}))
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

	// The fake host names no workspace.
	sc, _ := newFakeFabricScrapper(t)
	err = window(sc, scrapper.DwhFqn{DatabaseName: "sales", SchemaName: "sales", ObjectName: "orders"})
	require.ErrorContains(t, err, "the host names no Fabric workspace")

	// A pre-acquired SQL token cannot be used for OneLake.
	_, err = newDeltaLogReader(&FabricScrapperConf{FabricConf: dwhexecfabric.FabricConf{
		Host:        "lvmghcalzphu5hhdgghdffivni-6apfgq3vf4eexhtn4fl5xrwg7i.datawarehouse.fabric.microsoft.com",
		AccessToken: "sql-token",
	}})
	require.ErrorIs(t, err, dwhexecfabric.ErrNoAPICredential)
}

func TestItemsListingIsCached(t *testing.T) {
	w := seedWorkspace(t)
	reader := w.reader(t)
	now := t0
	reader.now = func() time.Time { return now }
	ctx := context.Background()

	_, _, err := reader.itemOf(ctx, "sales")
	require.NoError(t, err)
	_, _, err = reader.itemOf(ctx, "lake")
	require.NoError(t, err)
	require.Equal(t, 1, w.itemsHits)

	now = now.Add(itemsTTL + time.Second)
	id, ok, err := reader.itemOf(ctx, "sales")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, salesItem, id)
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
