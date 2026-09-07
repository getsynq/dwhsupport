package databricks

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
)

// The scrapes in this file run against a workspace that refuses requests for exceeding
// its control-plane quota, end to end: the fake answers the endpoints the real API
// answers, and the scrapper is the one NewDatabricksScrapper builds, so what is under
// test is the client a metrics run actually gets.
//
// A metrics scrape is one request per catalog, one per schema, one per schema's tables
// and then one per table, so a workspace of any size runs into that quota. Before the
// pacing, every one of those requests re-sent a refusal on the SDK's own schedule,
// which kept an exhausted quota exhausted, and a per-table read that never succeeded
// was collected as a warning string nobody read — so the run reported the numbers it
// had managed to fetch as though they were the workspace's.

// fastPacing is the shipped pacing with its waits scaled down, so a scrape converges
// in milliseconds rather than in production-sized ones.
var fastPacing = dwhexecdatabricks.Pacing{
	FirstInterval: 2 * time.Millisecond,
	MaxInterval:   80 * time.Millisecond,
	MaxPause:      200 * time.Millisecond,
	DecayAfter:    5,
	MaxAttempts:   8,
	WaitBudget:    5 * time.Second,
}

type RateLimitedScrapeSuite struct {
	suite.Suite
}

func TestRateLimitedScrapeSuite(t *testing.T) {
	suite.Run(t, new(RateLimitedScrapeSuite))
}

// TestScrapesCompleteUnderSustainedRateLimiting is the reproducer: a quota that meters
// the requests it refuses as well as those it admits, which is what makes a client that
// retries without slowing down starve itself. Every listing and every per-table read
// has to land, at a pace the workspace accepts.
func (s *RateLimitedScrapeSuite) TestScrapesCompleteUnderSustainedRateLimiting() {
	quota := &meteredQuota{window: 25 * time.Millisecond}

	fake := workspaceOfTables(2, 2)
	fake.pacing = &fastPacing
	fake.readProperties = map[string]string{
		"spark.sql.statistics.numRows":   "1000",
		"spark.sql.statistics.totalSize": "2048",
	}
	fake.refuse = func(*http.Request) *refusedRequest {
		if quota.admit() {
			return nil
		}
		return rateLimited()
	}

	scrapper := fake.startWith(s.T(), &DatabricksScrapperConf{RefreshTableMetrics: true})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rows, err := scrapper.QueryTableMetrics(ctx, time.Now().Add(-time.Hour))
	s.Require().NoError(err)
	s.Require().Len(rows, 8, "the scrape finished without covering every table")
	for _, row := range rows {
		s.Require().NotNil(row.RowCount, "%s.%s.%s was reported without a row count", row.Database, row.Schema, row.Table)
		s.Equal(int64(1000), *row.RowCount)
		s.Require().NotNil(row.SizeBytes)
		s.Equal(int64(2048), *row.SizeBytes)
	}

	s.Positive(quota.refusedCount(), "the quota never bit, so this test proves nothing")
	stats := scrapper.ThrottleStats()
	s.True(stats.Paced())
	s.Positive(stats.Waited)
}

// TestEveryScrapeCompletesUnderSustainedRateLimiting is the same quota against the rest
// of the interface, because each of these walks Unity Catalog the same way and any of
// them would otherwise be the one that fails a fetch.
func (s *RateLimitedScrapeSuite) TestEveryScrapeCompletesUnderSustainedRateLimiting() {
	quota := &meteredQuota{window: 25 * time.Millisecond}

	fake := workspaceOfTables(2, 2)
	fake.pacing = &fastPacing
	fake.refuse = func(*http.Request) *refusedRequest {
		if quota.admit() {
			return nil
		}
		return rateLimited()
	}

	scrapper := fake.startWith(s.T(), &DatabricksScrapperConf{})
	ctx := context.Background()

	s.Run("QuerySchemas", func() {
		rows, err := scrapper.QuerySchemas(ctx)
		s.Require().NoError(err)
		s.Len(rows, 4)
	})
	s.Run("QueryTableMetrics", func() {
		rows, err := scrapper.QueryTableMetrics(ctx, time.Now())
		s.Require().NoError(err)
		s.Len(rows, 8)
	})
	s.Run("QueryTables", func() {
		rows, err := scrapper.QueryTables(ctx)
		s.Require().NoError(err)
		s.Len(rows, 8)
	})
	s.Run("QueryCatalog", func() {
		rows, err := scrapper.QueryCatalog(ctx)
		s.Require().NoError(err)
		s.Len(rows, 8)
	})
	s.Run("QuerySqlDefinitions", func() {
		rows, err := scrapper.QuerySqlDefinitions(ctx)
		s.Require().NoError(err)
		s.Len(rows, 8)
	})
	s.Run("QueryTableConstraints", func() {
		rows, err := scrapper.QueryTableConstraints(ctx)
		s.Require().NoError(err)
		s.Len(rows, 8)
	})

	s.Positive(quota.refusedCount(), "the quota never bit, so this test proves nothing")
}

// TestARefusalHoldsBackEveryConcurrentTableRead — the per-table reads of a metrics
// scrape run sixteen at a time, and before the pacing each of them discovered the
// workspace's limit for itself and re-sent on a schedule of its own. One refusal has to
// hold all of them back.
func (s *RateLimitedScrapeSuite) TestARefusalHoldsBackEveryConcurrentTableRead() {
	const blockWindow = 600 * time.Millisecond
	const inFlightGrace = 200 * time.Millisecond

	fake := workspaceOfTables(1, 5)
	fake.pacing = &dwhexecdatabricks.Pacing{
		FirstInterval: 20 * time.Millisecond,
		MaxInterval:   100 * time.Millisecond,
		MaxPause:      2 * time.Second,
		DecayAfter:    3,
		MaxAttempts:   8,
		WaitBudget:    10 * time.Second,
	}

	var mu sync.Mutex
	var startedAt, blockUntil time.Time
	var offsets []time.Duration
	var refusals int
	// Only the per-table reads are refused: the listings have to get through for there
	// to be concurrent reads to hold back at all.
	fake.refuse = func(r *http.Request) *refusedRequest {
		if !strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/") {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		if blockUntil.IsZero() {
			startedAt = time.Now()
			blockUntil = startedAt.Add(blockWindow)
		}
		offsets = append(offsets, time.Since(startedAt))
		if time.Now().Before(blockUntil) {
			refusals++
			refused := rateLimited()
			refused.retryAfter = "1"
			return refused
		}
		return nil
	}

	scrapper := fake.startWith(s.T(), &DatabricksScrapperConf{RefreshTableMetrics: true})

	rows, err := scrapper.QueryTableMetrics(context.Background(), time.Now().Add(-time.Hour))
	s.Require().NoError(err)
	s.Len(rows, 10)

	mu.Lock()
	defer mu.Unlock()
	s.Positive(refusals, "nothing was refused, so this test proves nothing")
	duringWindow := lo.Filter(offsets, func(offset time.Duration, _ int) bool {
		return offset > inFlightGrace && offset < blockWindow
	})
	s.Empty(duringWindow, "reads kept going out while the workspace was refusing every one of them")
}

// TestAMetricsRunFailsRatherThanReportPartialNumbers is the silent degradation this
// issue is half about. A per-table read refused for the whole run used to be collected
// as a warning string that nothing ever looked at, so the run returned the tables it had
// managed to read as though they were the workspace's metrics — which downstream is
// indistinguishable from tables that stopped growing.
func (s *RateLimitedScrapeSuite) TestAMetricsRunFailsRatherThanReportPartialNumbers() {
	fake := workspaceOfTables(1, 2)
	fake.pacing = &fastPacing
	fake.refuse = func(r *http.Request) *refusedRequest {
		if strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/") {
			return rateLimited()
		}
		return nil
	}

	scrapper := fake.startWith(s.T(), &DatabricksScrapperConf{RefreshTableMetrics: true})

	startedAt := time.Now()
	rows, err := scrapper.QueryTableMetrics(context.Background(), time.Now().Add(-time.Hour))
	elapsed := time.Since(startedAt)

	s.Require().Error(err)
	s.Nil(rows, "a partially read set of metrics must never be returned")
	s.True(dwhexecdatabricks.IsRateLimitError(err), "the run has to fail as rate limited: %v", err)
	s.Less(elapsed, 30*time.Second, "the refusals were absorbed by the SDK's five-minute retry instead of surfacing")
}

// TestAFailedTableReadThatIsNotARateLimitStillDegrades pins the other half of that
// behaviour: a table that cannot be read for any other reason — dropped between the
// listing and the read is the common one — leaves the numbers the listing carried and
// does not fail the run.
func (s *RateLimitedScrapeSuite) TestAFailedTableReadThatIsNotARateLimitStillDegrades() {
	fake := workspaceOfTables(1, 2)
	fake.refuse = func(r *http.Request) *refusedRequest {
		if strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/") {
			return &refusedRequest{
				status:    http.StatusNotFound,
				errorCode: "TABLE_DOES_NOT_EXIST",
				message:   "Table does not exist.",
			}
		}
		return nil
	}

	scrapper := fake.startWith(s.T(), &DatabricksScrapperConf{RefreshTableMetrics: true})

	rows, err := scrapper.QueryTableMetrics(context.Background(), time.Now().Add(-time.Hour))
	s.Require().NoError(err)
	s.Len(rows, 4)
	s.Zero(scrapper.ThrottleStats().Refusals, "a failure that is not a rate limit widened the spacing")
}

// TestAListingRefusedForeverFailsTheScrapePromptly — waiting cannot be unbounded, and
// what ends it has to be the pacing rather than the SDK re-sending each refusal for
// five minutes.
func (s *RateLimitedScrapeSuite) TestAListingRefusedForeverFailsTheScrapePromptly() {
	fake := workspaceOfTables(2, 2)
	fake.pacing = &fastPacing
	fake.refuse = func(r *http.Request) *refusedRequest {
		if strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/") {
			return rateLimited()
		}
		return nil
	}

	scrapper := fake.startWith(s.T(), &DatabricksScrapperConf{})

	startedAt := time.Now()
	rows, err := scrapper.QueryTables(context.Background())
	elapsed := time.Since(startedAt)

	s.Require().Error(err)
	s.Nil(rows)
	s.True(dwhexecdatabricks.IsRateLimitError(err), "the scrape has to fail as rate limited: %v", err)
	s.Less(elapsed, 30*time.Second, "the refusals were absorbed by the SDK's five-minute retry instead of surfacing")
	// One catalog listing, sent as many times as the pacing allows and no more.
	s.Equal(fastPacing.MaxAttempts, fake.servedRequests()-scrapeStartupRequests)
}

// TestARefusedWorkspaceIsNotReportedAsAnAuthFailure — the ping a scrapper comes up with
// is a request like any other, and a workspace over its quota refusing it is not a
// workspace whose credentials are wrong.
func (s *RateLimitedScrapeSuite) TestARefusedWorkspaceIsNotReportedAsAnAuthFailure() {
	fake := workspaceOfTables(1, 1)
	fake.pacing = &fastPacing
	fake.refuse = func(*http.Request) *refusedRequest { return rateLimited() }

	scrapper, err := fake.tryStart(s.T(), &DatabricksScrapperConf{})
	s.Require().Error(err)
	s.Nil(scrapper)
	s.True(dwhexecdatabricks.IsRateLimitError(err), "%v", err)
	s.NotContains(err.Error(), "authentication")
}

// scrapeStartupRequests is what a scrapper sends on its way up before any scrape runs:
// the ping, once.
const scrapeStartupRequests = 1

// workspaceOfTables is a workspace of catalogs each holding schemas each holding two
// tables, which is the shape every walk of Unity Catalog reads.
func workspaceOfTables(catalogs, schemasPerCatalog int) *fakeWorkspace {
	fake := newFakeWorkspace()
	for c := 0; c < catalogs; c++ {
		catalog := fmt.Sprintf("catalog_%d", c)
		fake.addCatalog(catalog)
		for s := 0; s < schemasPerCatalog; s++ {
			schema := fmt.Sprintf("schema_%d", s)
			fake.addSchema(catalog, schema)
			fake.addTable(catalog, schema, "orders")
			fake.addTable(catalog, schema, "customers")
		}
	}
	return fake
}

// meteredQuota is a rate limit that counts the requests it refuses, the way a real one
// does. It is what separates a client that paces itself from one that retries harder:
// the second never gets a request through, while a fake that meters only the requests
// it admits lets an unpaced client pass.
type meteredQuota struct {
	window time.Duration

	mu       sync.Mutex
	closedAt time.Time
	refused  int
}

func (q *meteredQuota) admit() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	admitted := !now.Before(q.closedAt)
	if !admitted {
		q.refused++
	}
	q.closedAt = now.Add(q.window)
	return admitted
}

func (q *meteredQuota) refusedCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.refused
}
