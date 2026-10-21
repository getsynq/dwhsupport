package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

const fakeProject = "test-project"

// fakeTable is a table of fakeMetadataServer: either the JSON tables.get
// answers with, or the HTTP status it fails with.
type fakeTable struct {
	id     string
	json   string
	status int
}

// fakeDataset is a dataset of fakeMetadataServer. A non-zero metadataStatus
// fails datasets.get, a non-zero listStatus fails tables.list, a non-zero
// tablesQueryStatus fails a query of the dataset's INFORMATION_SCHEMA.TABLES.
// labels is the JSON object of its labels, {"team":"data"} when empty.
type fakeDataset struct {
	id                string
	metadataStatus    int
	listStatus        int
	tablesQueryStatus int
	labels            string
	tables            []fakeTable
}

// fakeMetadataServer answers the BigQuery REST calls the metadata scrape makes
// (datasets.list, datasets.get, tables.list, tables.get) for one project, failing
// the ones a dataset or table is set up to fail.
//
// Query jobs are refused with a 403 unless answersQueries is set, so
// QuerySqlDefinitions falls back to reading each table. With it set, a query of
// a dataset's INFORMATION_SCHEMA.TABLES answers with a row per table that
// tables.get answers for, the way BigQuery leaves out a table the caller may
// not read, and a query of the region's is refused, the way it is for a role
// granted only on the datasets.
type fakeMetadataServer struct {
	datasets       []fakeDataset
	answersQueries bool

	mu      sync.Mutex
	queries []string
}

// queried returns the SQL of every query job it was asked to run.
func (f *fakeMetadataServer) queried() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.queries...)
}

func (f *fakeMetadataServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, rest, found := strings.Cut(r.URL.Path, "/projects/"+fakeProject+"/")
	if !found {
		fakeError(w, http.StatusNotFound, "unexpected request "+r.URL.Path)
		return
	}
	parts := strings.Split(rest, "/")
	switch {
	case len(parts) == 1 && parts[0] == "jobs":
		if !f.answersQueries {
			fakeError(w, http.StatusForbidden, "Access Denied: Project "+fakeProject+": User does not have bigquery.jobs.create permission")
			return
		}
		f.insertQuery(w, r)
	case len(parts) == 2 && (parts[0] == "queries" || parts[0] == "jobs"):
		f.queryResults(w, parts[1], parts[0] == "queries")
	case len(parts) == 1 && parts[0] == "datasets":
		var items []string
		for _, ds := range f.datasets {
			items = append(items, fmt.Sprintf(`{"datasetReference":{"projectId":%q,"datasetId":%q}}`, fakeProject, ds.id))
		}
		_, _ = fmt.Fprintf(w, `{"kind":"bigquery#datasetList","datasets":[%s]}`, strings.Join(items, ","))
	case len(parts) >= 2 && parts[0] == "datasets":
		ds, ok := f.dataset(parts[1])
		if !ok {
			fakeError(w, http.StatusNotFound, "Not found: Dataset "+fakeProject+":"+parts[1])
			return
		}
		switch {
		case len(parts) == 2:
			if ds.metadataStatus != 0 {
				fakeError(w, ds.metadataStatus, "dataset "+ds.id+" refused")
				return
			}
			labels := ds.labels
			if labels == "" {
				labels = `{"team":"data"}`
			}
			_, _ = fmt.Fprintf(w, `{"datasetReference":{"projectId":%q,"datasetId":%q},"labels":%s}`, fakeProject, ds.id, labels)
		case len(parts) == 3 && parts[2] == "tables":
			if ds.listStatus != 0 {
				fakeError(w, ds.listStatus, "Access Denied: Dataset "+fakeProject+":"+ds.id+": Permission bigquery.tables.list denied")
				return
			}
			var items []string
			for _, t := range ds.tables {
				items = append(items, fmt.Sprintf(`{"tableReference":{"projectId":%q,"datasetId":%q,"tableId":%q}}`, fakeProject, ds.id, t.id))
			}
			_, _ = fmt.Fprintf(w, `{"kind":"bigquery#tableList","tables":[%s]}`, strings.Join(items, ","))
		case len(parts) == 4 && parts[2] == "tables":
			for _, t := range ds.tables {
				if t.id != parts[3] {
					continue
				}
				if t.status != 0 {
					fakeError(w, t.status, "table "+t.id+" refused")
					return
				}
				_, _ = fmt.Fprintf(w, `{"tableReference":{"projectId":%q,"datasetId":%q,"tableId":%q},%s}`, fakeProject, ds.id, t.id, t.json)
				return
			}
			fakeError(w, http.StatusNotFound, "Not found: Table "+parts[3])
		default:
			fakeError(w, http.StatusNotFound, "unexpected request "+r.URL.Path)
		}
	default:
		fakeError(w, http.StatusNotFound, "unexpected request "+r.URL.Path)
	}
}

// insertQuery answers a jobs.insert of a query, under a job id naming the
// dataset whose INFORMATION_SCHEMA.TABLES it reads.
func (f *fakeMetadataServer) insertQuery(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var req struct {
		Configuration struct {
			Query struct {
				Query string `json:"query"`
			} `json:"query"`
		} `json:"configuration"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		fakeError(w, http.StatusBadRequest, err.Error())
		return
	}
	sql := req.Configuration.Query.Query
	f.mu.Lock()
	f.queries = append(f.queries, sql)
	f.mu.Unlock()

	if strings.Contains(sql, "region-") {
		fakeError(w, http.StatusForbidden, "Access Denied: Table "+fakeProject+":region-us.INFORMATION_SCHEMA.TABLES: User does not have permission")
		return
	}
	for _, ds := range f.datasets {
		if !strings.Contains(sql, fmt.Sprintf("`%s`.`%s`.INFORMATION_SCHEMA.TABLES", fakeProject, ds.id)) {
			continue
		}
		if ds.tablesQueryStatus != 0 {
			fakeError(
				w,
				ds.tablesQueryStatus,
				"Access Denied: Table "+fakeProject+":"+ds.id+".INFORMATION_SCHEMA.TABLES: User does not have permission",
			)
			return
		}
		_, _ = fmt.Fprintf(
			w, `{"jobReference":{"projectId":%q,"jobId":%q,"location":"US"},"configuration":{"query":{"query":"x"}},"status":{"state":"DONE"}}`,
			fakeProject, "tables-"+ds.id,
		)
		return
	}
	fakeError(w, http.StatusBadRequest, "unexpected query "+sql)
}

// queryResults answers jobs.get, and jobs.getQueryResults with the rows of the
// dataset the job id names.
func (f *fakeMetadataServer) queryResults(w http.ResponseWriter, jobId string, withRows bool) {
	datasetId, ok := strings.CutPrefix(jobId, "tables-")
	ds, found := f.dataset(datasetId)
	if !ok || !found {
		fakeError(w, http.StatusNotFound, "Not found: Job "+jobId)
		return
	}
	jobRef := fmt.Sprintf(`"jobReference":{"projectId":%q,"jobId":%q,"location":"US"}`, fakeProject, jobId)
	if !withRows {
		_, _ = fmt.Fprintf(w, `{%s,"configuration":{"query":{"query":"x"}},"status":{"state":"DONE"}}`, jobRef)
		return
	}
	var rows []string
	for _, t := range ds.tables {
		if t.status != 0 {
			continue
		}
		isView := strings.Contains(t.json, `"type":"VIEW"`)
		ddl := fmt.Sprintf("CREATE TABLE `%s.%s.%s` (id INT64);", fakeProject, ds.id, t.id)
		if isView {
			ddl = fmt.Sprintf("CREATE VIEW `%s.%s.%s` AS select 1 as id;", fakeProject, ds.id, t.id)
		}
		rows = append(rows, fmt.Sprintf(`{"f":[{"v":%q},{"v":%q},{"v":%q},{"v":%q},{"v":"%t"}]}`, fakeProject, ds.id, t.id, ddl, isView))
	}
	_, _ = fmt.Fprintf(w, `{%s,"jobComplete":true,"totalRows":"%d","rows":[%s],"schema":{"fields":[`+
		`{"name":"database","type":"STRING"},{"name":"schema","type":"STRING"},{"name":"table","type":"STRING"},`+
		`{"name":"sql","type":"STRING"},{"name":"is_view","type":"BOOLEAN"}]}}`, jobRef, len(rows), strings.Join(rows, ","))
}

func (f *fakeMetadataServer) dataset(id string) (fakeDataset, bool) {
	for _, ds := range f.datasets {
		if ds.id == id {
			return ds, true
		}
	}
	return fakeDataset{}, false
}

func fakeError(w http.ResponseWriter, status int, message string) {
	reason := map[int]string{http.StatusBadRequest: "invalidQuery", http.StatusForbidden: "accessDenied", http.StatusNotFound: "notFound"}[status]
	if reason == "" {
		reason = "backendError"
	}
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `{"error":{"code":%d,"message":%q,"errors":[{"reason":%q,"message":%q}]}}`, status, message, reason, message)
}

func newFakeMetadataScrapper(t *testing.T, datasets ...fakeDataset) *BigQueryScrapper {
	t.Helper()
	return newFakeMetadataScrapperOn(t, &fakeMetadataServer{datasets: datasets})
}

func newFakeMetadataScrapperOn(t *testing.T, fake *fakeMetadataServer) *BigQueryScrapper {
	t.Helper()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	client, err := bigquery.NewClient(
		context.Background(),
		fakeProject,
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{Timeout: 5 * time.Second}),
	)
	require.NoError(t, err)

	conf := &BigQueryScrapperConf{BigQueryConf: dwhexecbigquery.BigQueryConf{ProjectId: fakeProject, Region: "us"}}
	rateLimitCfg := DefaultRateLimitConfig
	// The client retries a 5xx on its own until the call's deadline, so a table
	// set up to fail with one fails after this long.
	rateLimitCfg.CallTimeout = 500 * time.Millisecond
	s := &BigQueryScrapper{
		conf:         conf,
		scope:        ScopeFromConf(conf),
		executor:     dwhexecbigquery.NewBigqueryExecutorFromClient(client, &conf.BigQueryConf),
		rateLimitCfg: rateLimitCfg,
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func fakeBaseTable(id string) fakeTable {
	return fakeTable{
		id: id,
		json: `"type":"TABLE","description":"a table","schema":{"fields":[{"name":"id","type":"INTEGER"}]},` +
			`"numRows":"10","numBytes":"100","lastModifiedTime":"1700000000000"`,
	}
}

func fakeView(id string) fakeTable {
	return fakeTable{
		id:   id,
		json: `"type":"VIEW","schema":{"fields":[{"name":"id","type":"INTEGER"}]},"view":{"query":"select 1 as id"}`,
	}
}

func fakeRefusedTable(id string, status int) fakeTable {
	return fakeTable{id: id, status: status}
}

func sortedStrings(values []string) []string {
	sort.Strings(values)
	return values
}
