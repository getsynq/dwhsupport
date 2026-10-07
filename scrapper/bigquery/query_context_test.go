package bigquery

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

// recordedJob is the part of a jobs.insert request that carries our marker.
type recordedJob struct {
	SQL        string
	Labels     map[string]string
	DryRun     bool
	Parameters int
}

// fakeJobsServer answers the BigQuery REST calls a query job makes (jobs.insert,
// jobs.getQueryResults, jobs.get) with an empty, finished result, and records
// every job it was asked to insert.
type fakeJobsServer struct {
	mu   sync.Mutex
	jobs []recordedJob
}

func (f *fakeJobsServer) recorded() []recordedJob {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedJob(nil), f.jobs...)
}

func (f *fakeJobsServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	const jobRef = `"jobReference":{"projectId":"test-project","jobId":"job1","location":"US"}`
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/jobs"):
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Configuration struct {
				DryRun bool              `json:"dryRun"`
				Labels map[string]string `json:"labels"`
				Query  struct {
					Query           string            `json:"query"`
					QueryParameters []json.RawMessage `json:"queryParameters"`
				} `json:"query"`
			} `json:"configuration"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.jobs = append(f.jobs, recordedJob{
			SQL:        req.Configuration.Query.Query,
			Labels:     req.Configuration.Labels,
			DryRun:     req.Configuration.DryRun,
			Parameters: len(req.Configuration.Query.QueryParameters),
		})
		f.mu.Unlock()
		_, _ = w.Write([]byte(`{` + jobRef + `,"configuration":{"query":{"query":"x"}},"status":{"state":"DONE"},` +
			`"statistics":{"totalBytesProcessed":"42","query":{"totalBytesProcessed":"42","totalBytesProcessedAccuracy":"PRECISE"}}}`))
	case strings.Contains(r.URL.Path, "/queries/"):
		_, _ = w.Write([]byte(`{` + jobRef + `,"jobComplete":true,"totalRows":"0","rows":[],` +
			`"schema":{"fields":[{"name":"value","type":"INTEGER"}]}}`))
	case strings.Contains(r.URL.Path, "/jobs/"):
		_, _ = w.Write([]byte(`{` + jobRef + `,"configuration":{"query":{"query":"x"}},"status":{"state":"DONE"}}`))
	default:
		http.Error(w, "unexpected request "+r.Method+" "+r.URL.Path, http.StatusNotFound)
	}
}

func newFakeJobsScrapper(t *testing.T) (*BigQueryScrapper, *fakeJobsServer) {
	t.Helper()
	fake := &fakeJobsServer{}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	client, err := bigquery.NewClient(
		context.Background(),
		"test-project",
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{Timeout: 5 * time.Second}),
	)
	require.NoError(t, err)

	conf := &BigQueryScrapperConf{BigQueryConf: dwhexecbigquery.BigQueryConf{ProjectId: "test-project"}}
	s := &BigQueryScrapper{
		conf:         conf,
		executor:     dwhexecbigquery.NewBigqueryExecutorFromClient(client, &conf.BigQueryConf),
		rateLimitCfg: DefaultRateLimitConfig,
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, fake
}

// queryPaths runs one query through every path of the scrapper that starts a
// BigQuery job.
var queryPaths = map[string]func(ctx context.Context, s *BigQueryScrapper) error{
	"queryRows": func(ctx context.Context, s *BigQueryScrapper) error {
		_, err := s.queryRows(ctx, "SELECT 1 AS value")
		return err
	},
	"QueryCustomMetrics": func(ctx context.Context, s *BigQueryScrapper) error {
		_, err := s.QueryCustomMetrics(ctx, "SELECT 1 AS value")
		return err
	},
	"RunRawQuery": func(ctx context.Context, s *BigQueryScrapper) error {
		it, err := s.RunRawQuery(ctx, "SELECT 1 AS value")
		if err != nil {
			return err
		}
		return it.Close()
	},
	"QueryShape": func(ctx context.Context, s *BigQueryScrapper) error {
		_, err := s.QueryShape(ctx, "SELECT 1 AS value")
		return err
	},
	"EstimateQuery": func(ctx context.Context, s *BigQueryScrapper) error {
		_, err := s.EstimateQuery(ctx, "SELECT 1 AS value")
		return err
	},
	"QuerySegments": func(ctx context.Context, s *BigQueryScrapper) error {
		_, err := s.QuerySegments(ctx, "SELECT 'a' AS segment")
		return err
	},
	"Executor.Exec": func(ctx context.Context, s *BigQueryScrapper) error {
		return s.Executor().Exec(ctx, "SELECT 1 AS value")
	},
}

// Every BigQuery job we start carries the caller's query context twice: as a
// SQL comment, which INFORMATION_SCHEMA.JOBS keeps in the query text, and as
// job labels. Query-history readers drop our own traffic by either.
func TestEveryQueryPathCarriesTheQueryContext(t *testing.T) {
	qc := querycontext.QueryContext{"source": "synq", "monitor": "m-1"}
	ctx := querycontext.WithQueryContext(context.Background(), qc)

	for name, run := range queryPaths {
		t.Run(name, func(t *testing.T) {
			s, fake := newFakeJobsScrapper(t)
			require.NoError(t, run(ctx, s))

			jobs := fake.recorded()
			require.Len(t, jobs, 1)
			assert.True(t, strings.HasSuffix(jobs[0].SQL, qc.FormatAsSQLComment()), "query text %q has no query-context comment", jobs[0].SQL)
			assert.Equal(t, map[string]string{"source": "synq", "monitor": "m-1"}, jobs[0].Labels)
		})
	}
}

// Without a query context, a job goes out as written and without labels.
func TestQueryPathsWithoutQueryContextAreUnchanged(t *testing.T) {
	for name, run := range queryPaths {
		t.Run(name, func(t *testing.T) {
			s, fake := newFakeJobsScrapper(t)
			require.NoError(t, run(context.Background(), s))

			jobs := fake.recorded()
			require.Len(t, jobs, 1)
			assert.NotContains(t, jobs[0].SQL, "/*")
			assert.Empty(t, jobs[0].Labels)
		})
	}
}

// The estimate stays a dry run with the marker added.
func TestEstimateQueryIsStillADryRun(t *testing.T) {
	s, fake := newFakeJobsScrapper(t)
	ctx := querycontext.WithQueryContext(context.Background(), querycontext.QueryContext{"source": "synq"})

	estimate, err := s.EstimateQuery(ctx, "SELECT 1")
	require.NoError(t, err)
	require.NotNil(t, estimate.BytesScanned)
	assert.Equal(t, int64(42), *estimate.BytesScanned)

	jobs := fake.recorded()
	require.Len(t, jobs, 1)
	assert.True(t, jobs[0].DryRun)
}

// queryRows used to drop its arguments; they are now the job's positional
// parameters, as on the executor.
func TestQueryRowsSendsItsArguments(t *testing.T) {
	s, fake := newFakeJobsScrapper(t)

	_, err := s.queryRows(context.Background(), "SELECT ? AS value, ? AS other", 1, "a")
	require.NoError(t, err)

	jobs := fake.recorded()
	require.Len(t, jobs, 1)
	assert.Equal(t, 2, jobs[0].Parameters)
}
