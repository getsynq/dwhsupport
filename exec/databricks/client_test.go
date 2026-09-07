package databricks

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/httpclient"
	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sync/errgroup"
)

const rateLimitedResponse = `{"error_code": "REQUEST_LIMIT_EXCEEDED", "message": "Rate limit exceeded. Please try again later."}`

type PacedClientSuite struct {
	suite.Suite
}

func TestPacedClientSuite(t *testing.T) {
	suite.Run(t, new(PacedClientSuite))
}

// TestPacesItselfOntoAQuotaThatMetersRefusals is the reproducer that matters: a quota
// which meters the requests it refuses as well as those it admits, the way a real
// limiter does. A client that retries without slowing down never gets a request
// through it, and a fake that only meters admitted requests would let such a client
// pass. Every request has to land, and the client has to converge on a pace the quota
// allows to make that happen.
func (s *PacedClientSuite) TestPacesItselfOntoAQuotaThatMetersRefusals() {
	quota := &meteredQuota{window: 60 * time.Millisecond}

	fake := newFakeControlPlane(s.T())
	fake.respond = func(int) response {
		if quota.admit() {
			return response{status: http.StatusOK, body: currentUserResponse}
		}
		return response{status: http.StatusTooManyRequests, body: rateLimitedResponse}
	}

	client := fake.workspaceClient(s.T(), Pacing{
		FirstInterval: 5 * time.Millisecond,
		MaxInterval:   250 * time.Millisecond,
		MaxPause:      250 * time.Millisecond,
		DecayAfter:    5,
		MaxAttempts:   8,
		WaitBudget:    10 * time.Second,
	})

	// Generous next to the couple of seconds a paced client needs, and far below the
	// deadline an unpaced one used to burn.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s.Require().NoError(callConcurrently(ctx, client, 20, 8))

	s.Positive(quota.refusedCount(), "the quota never bit, so this test proves nothing")
	stats := fake.throttle().Stats()
	s.Positive(stats.Refusals)
	s.Positive(stats.Waited)
	s.Positive(stats.Interval, "the client finished without ever slowing down")
}

// TestAnUnpacedClientStarvesOnTheSameQuota is the other half of the reproducer: with
// the SDK's own blind retry and nothing pacing it, the same quota refuses far more
// than it admits and the caller's deadline is what ends the run. It is here so that
// the test above cannot quietly stop proving anything.
func (s *PacedClientSuite) TestAnUnpacedClientStarvesOnTheSameQuota() {
	quota := &meteredQuota{window: 60 * time.Millisecond}

	fake := newFakeControlPlane(s.T())
	fake.respond = func(int) response {
		if quota.admit() {
			return response{status: http.StatusOK, body: currentUserResponse}
		}
		return response{status: http.StatusTooManyRequests, body: rateLimitedResponse}
	}

	client, err := databricks.NewWorkspaceClient(fake.config())
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s.Error(callConcurrently(ctx, client, 20, 8), "an unpaced client got through a quota that meters its refusals")
	s.Greater(quota.refusedCount(), quota.admittedCount(),
		"the quota admitted more than it refused, so it is not metering refusals")
}

// TestWaitsOutTheRetryAfterTheWorkspaceAsksFor — the refusal carries how long to wait,
// and the SDK ignores it entirely: it re-sends on its own one-second-and-growing
// schedule, which is how a caller turns a quota it has already exceeded into a
// sustained stream of refusals. One request makes the timing unambiguous.
func (s *PacedClientSuite) TestWaitsOutTheRetryAfterTheWorkspaceAsksFor() {
	const retryAfter = time.Second

	fake := newFakeControlPlane(s.T())
	fake.respond = func(attempt int) response {
		if attempt == 1 {
			return response{
				status: http.StatusTooManyRequests,
				header: http.Header{"Retry-After": []string{"1"}},
				body:   rateLimitedResponse,
			}
		}
		return response{status: http.StatusOK, body: currentUserResponse}
	}

	client := fake.workspaceClient(s.T(), Pacing{
		FirstInterval: 10 * time.Millisecond,
		MaxInterval:   100 * time.Millisecond,
		// Longer than the second the fake asks for: what MaxPause clamps is covered on
		// the throttle itself, and clamping here would be measuring the cap instead of
		// the header.
		MaxPause:    5 * time.Second,
		DecayAfter:  5,
		MaxAttempts: 8,
		WaitBudget:  30 * time.Second,
	})

	_, err := client.CurrentUser.Me(context.Background())
	s.Require().NoError(err)

	offsets := fake.offsets()
	s.Require().Len(offsets, 2, "the refused request was sent exactly once more")
	// Timer granularity means the wait can land a hair under a second; what matters is
	// that it is the header's second and not the SDK's own backoff.
	s.GreaterOrEqual(offsets[1], retryAfter-50*time.Millisecond, "the retry went out before the API said it could")
}

// TestOneRefusalHoldsBackEveryConcurrentRequest is the shape the wedge came down to.
// Concurrent callers each used to discover the rate limit for themselves and re-send
// on their own schedule, so a workspace over its quota stayed over its quota. A
// refusal has to hold all of them back, not only the request that saw it.
func (s *PacedClientSuite) TestOneRefusalHoldsBackEveryConcurrentRequest() {
	const blockWindow = time.Second
	const inFlightGrace = 200 * time.Millisecond

	fake := newFakeControlPlane(s.T())
	var mu sync.Mutex
	var blockUntil time.Time
	var refusals int
	fake.respond = func(int) response {
		mu.Lock()
		defer mu.Unlock()
		if blockUntil.IsZero() {
			blockUntil = time.Now().Add(blockWindow)
		}
		if time.Now().Before(blockUntil) {
			refusals++
			return response{
				status: http.StatusTooManyRequests,
				header: http.Header{"Retry-After": []string{"1"}},
				body:   rateLimitedResponse,
			}
		}
		return response{status: http.StatusOK, body: currentUserResponse}
	}

	client := fake.workspaceClient(s.T(), Pacing{
		FirstInterval: 50 * time.Millisecond,
		MaxInterval:   200 * time.Millisecond,
		MaxPause:      5 * time.Second,
		DecayAfter:    3,
		MaxAttempts:   8,
		WaitBudget:    30 * time.Second,
	})

	s.Require().NoError(callConcurrently(context.Background(), client, 24, 16))

	// Requests already in flight when the first refusal was answered still land, and
	// nothing may follow them until the window the API named has passed.
	duringWindow := lo.Filter(fake.offsets(), func(offset time.Duration, _ int) bool {
		return offset > inFlightGrace && offset < blockWindow
	})
	s.Empty(duringWindow, "requests kept going out while the workspace was refusing every one of them")

	mu.Lock()
	defer mu.Unlock()
	s.Positive(refusals, "the workspace never refused anything, so this test proves nothing")
}

// TestFailsPromptlyWhenTheQuotaNeverLifts — waiting cannot be unbounded, and it must
// end in the caller's hands rather than in the SDK's, which re-sends a refusal blindly
// for five minutes per request.
func (s *PacedClientSuite) TestFailsPromptlyWhenTheQuotaNeverLifts() {
	fake := newFakeControlPlane(s.T())
	fake.respond = func(int) response {
		return response{status: http.StatusTooManyRequests, body: rateLimitedResponse}
	}

	client := fake.workspaceClient(s.T(), fastPacing)

	startedAt := time.Now()
	_, err := client.CurrentUser.Me(context.Background())
	elapsed := time.Since(startedAt)

	s.Require().Error(err)
	s.True(IsRateLimitError(err), "a refusal has to stay recognisable as one: %v", err)
	s.Contains(err.Error(), "rate limited by the workspace")
	s.Less(elapsed, 10*time.Second, "the refusal was absorbed by the SDK's five-minute retry instead of surfacing")
	s.Len(fake.offsets(), fastPacing.MaxAttempts, "the request was sent a number of times the pacing does not decide")

	var rateLimited *RateLimitedError
	s.Require().ErrorAs(err, &rateLimited)
	s.Equal(http.StatusTooManyRequests, rateLimited.StatusCode)
	s.Equal("REQUEST_LIMIT_EXCEEDED", rateLimited.ErrorCode)
	s.Contains(rateLimited.Message, "Rate limit exceeded")
}

// TestSpendsNoAttemptsOnFailuresThatAreNotRateLimits — the waiting is for the quota
// alone. Spending attempts on a request refused for any other reason only delays the
// failure the caller has to see.
func (s *PacedClientSuite) TestSpendsNoAttemptsOnFailuresThatAreNotRateLimits() {
	fake := newFakeControlPlane(s.T())
	fake.respond = func(int) response {
		return response{
			status: http.StatusBadRequest,
			body:   `{"error_code": "INVALID_PARAMETER_VALUE", "message": "boom"}`,
		}
	}

	client := fake.workspaceClient(s.T(), fastPacing)

	_, err := client.CurrentUser.Me(context.Background())
	s.Require().Error(err)
	s.False(IsRateLimitError(err))
	s.Contains(err.Error(), "boom", "the API's own error stopped reaching the caller")
	s.Len(fake.offsets(), 1, "a failure that is not a rate limit was retried")
	s.Zero(fake.throttle().Stats().Refusals)
}

// TestRefusalWithoutA429StatusIsStillPaced — the control plane does not always answer
// a status the SDK maps onto its rate-limit error; a 500 carrying the code is the shape
// the SDK works around by re-sending anything whose message mentions
// REQUEST_LIMIT_EXCEEDED. That workaround has to be off, and the pacing has to see the
// refusal for what it is.
func (s *PacedClientSuite) TestRefusalWithoutA429StatusIsStillPaced() {
	fake := newFakeControlPlane(s.T())
	fake.respond = func(attempt int) response {
		if attempt <= 2 {
			return response{status: http.StatusInternalServerError, body: rateLimitedResponse}
		}
		return response{status: http.StatusOK, body: currentUserResponse}
	}

	client := fake.workspaceClient(s.T(), fastPacing)

	_, err := client.CurrentUser.Me(context.Background())
	s.Require().NoError(err)
	s.Len(fake.offsets(), 3)
	s.Equal(2, fake.throttle().Stats().Refusals)
	s.Positive(fake.throttle().Stats().Interval)
}

// TestPacingIsSharedByEveryClientOfTheWorkspace — the throttle is per workspace, not
// per client, which is what makes a scrape and a lineage walk enrolled from the same
// integration pace against each other rather than each discovering the limit
// separately.
func (s *PacedClientSuite) TestPacingIsSharedByEveryClientOfTheWorkspace() {
	fake := newFakeControlPlane(s.T())
	var once sync.Once
	fake.respond = func(int) response {
		refused := response{status: http.StatusOK, body: currentUserResponse}
		once.Do(func() {
			refused = response{
				status: http.StatusTooManyRequests,
				header: http.Header{"Retry-After": []string{"1"}},
				body:   rateLimitedResponse,
			}
		})
		return refused
	}

	pacing := Pacing{
		FirstInterval: 20 * time.Millisecond,
		MaxInterval:   100 * time.Millisecond,
		MaxPause:      5 * time.Second,
		DecayAfter:    50,
		MaxAttempts:   8,
		WaitBudget:    30 * time.Second,
	}
	refusedClient := fake.workspaceClient(s.T(), pacing)
	// A second client of the same workspace, built from a config of its own — the
	// lineage walk's client, in the process this is written for.
	otherClient := fake.workspaceClient(s.T(), pacing)

	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		_, err := refusedClient.CurrentUser.Me(ctx)
		return err
	})
	g.Go(func() error {
		// Started behind the refused one so the refusal is what it runs into.
		time.Sleep(50 * time.Millisecond)
		_, err := otherClient.CurrentUser.Me(ctx)
		return err
	})
	s.Require().NoError(g.Wait())

	offsets := fake.offsets()
	s.Require().Len(offsets, 3, "one refused request, its retry, and the other client's request")
	for _, offset := range offsets[1:] {
		s.GreaterOrEqual(offset, 900*time.Millisecond,
			"a request went out during the pause a refusal opened, so the clients are not sharing a throttle")
	}
	s.Equal(1, fake.throttle().Stats().Refusals)
}

// TestApiClientGivesUpRatherThanLettingTheSdkRetry covers the raw client the lineage
// endpoint is called through. Its refusals are the caller's to wait out, and the SDK's
// own retry of them — including the message match on REQUEST_LIMIT_EXCEEDED that
// outlives ErrorRetriable — has to be off.
func (s *PacedClientSuite) TestApiClientGivesUpRatherThanLettingTheSdkRetry() {
	fake := newFakeControlPlane(s.T())
	fake.respond = func(int) response {
		return response{status: http.StatusInternalServerError, body: rateLimitedResponse}
	}

	UsePacing(fake.server.URL, fastPacing)
	apiClient, err := NewApiClient(fake.config())
	s.Require().NoError(err)

	startedAt := time.Now()
	var out struct{}
	err = apiClient.Do(
		context.Background(),
		"GET",
		"/api/2.0/lineage-tracking/table-lineage",
		httpclient.WithRequestData(map[string]any{"table_name": "main.sales.orders"}),
		httpclient.WithResponseUnmarshal(&out),
	)
	elapsed := time.Since(startedAt)

	s.Require().Error(err)
	s.True(IsRateLimitError(err), "the caller cannot tell this was a rate limit: %v", err)
	s.Len(fake.offsets(), fastPacing.MaxAttempts, "the SDK re-sent the refusal on top of the pacing")
	s.Less(elapsed, 10*time.Second)
}

// TestARefusedPingIsNotReportedAsAnAuthFailure — a workspace over its quota is not a
// workspace we cannot authenticate against, and reporting it as one sends the customer
// to check credentials that are fine.
func (s *PacedClientSuite) TestARefusedPingIsNotReportedAsAnAuthFailure() {
	fake := newFakeControlPlane(s.T())
	fake.respond = func(int) response {
		return response{status: http.StatusTooManyRequests, body: rateLimitedResponse}
	}
	UsePacing(fake.server.URL, fastPacing)

	_, err := NewDatabricksExecutor(context.Background(), &DatabricksConf{
		WorkspaceUrl: fake.server.URL,
		Auth:         NewTokenAuth("test-token"),
	})
	s.Require().Error(err)
	s.True(IsRateLimitError(err))

	var authErr *dwhexec.AuthError
	s.False(errors.As(err, &authErr), "a rate limit was reported as an authentication failure: %v", err)
}

// TestPacingIsInstalledOnlyOnce — a config that has already been paced can be handed
// back to a constructor, which is what the cloud-side callers do with the config they
// take off a client to build the lineage client from.
func (s *PacedClientSuite) TestPacingIsInstalledOnlyOnce() {
	cfg := WithPacing(&databricks.Config{Host: "https://example.cloud.databricks.com"})
	paced, ok := cfg.HTTPTransport.(*pacedTransport)
	s.Require().True(ok)
	s.Equal(pacedHttpTimeoutSeconds, cfg.HTTPTimeoutSeconds)

	again := WithPacing(cfg)
	s.Same(cfg, again)
	s.Same(paced, again.HTTPTransport, "the pacing was wrapped around itself")
}

// TestPacingLeavesAConfiguredTimeoutAlone — the longer per-attempt timeout is there to
// cover the waiting, not to override a caller that has chosen one.
func (s *PacedClientSuite) TestPacingLeavesAConfiguredTimeoutAlone() {
	cfg := WithPacing(&databricks.Config{Host: "https://example.cloud.databricks.com", HTTPTimeoutSeconds: 30})
	s.Equal(30, cfg.HTTPTimeoutSeconds)
}

// callConcurrently sends count requests through the client with at most concurrency of
// them in flight, the way a scrape reads a workspace's tables.
func callConcurrently(ctx context.Context, client *databricks.WorkspaceClient, count, concurrency int) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	for i := 0; i < count; i++ {
		g.Go(func() error {
			_, err := client.CurrentUser.Me(gctx)
			return err
		})
	}
	return g.Wait()
}

const currentUserResponse = `{"id": "1", "userName": "tester"}`

// response is one answer the fake control plane gives.
type response struct {
	status int
	header http.Header
	body   string
}

// fakeControlPlane is a Databricks workspace's control plane, answering whatever the
// test tells it to and recording when each request arrived.
type fakeControlPlane struct {
	server    *httptest.Server
	startedAt time.Time

	mu             sync.Mutex
	requestOffsets []time.Duration
	// respond decides the answer to the nth request, counted from one.
	respond func(attempt int) response
}

func newFakeControlPlane(t *testing.T) *fakeControlPlane {
	t.Helper()
	fake := &fakeControlPlane{startedAt: time.Now()}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serve))
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeControlPlane) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requestOffsets = append(f.requestOffsets, time.Since(f.startedAt))
	attempt := len(f.requestOffsets)
	respond := f.respond
	f.mu.Unlock()

	answer := response{status: http.StatusOK, body: currentUserResponse}
	if respond != nil {
		answer = respond(attempt)
	}
	for key, values := range answer.header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(answer.status)
	_, _ = fmt.Fprint(w, answer.body)
}

// config is a client config pointed at the fake. The SDK's own client-side limiter is
// turned up out of the way, so that what a test measures is the pacing rather than a
// fifteen-per-second cap that has nothing to say about the workspace's quota.
func (f *fakeControlPlane) config() *databricks.Config {
	return &databricks.Config{Host: f.server.URL, Token: "test-token", RateLimitPerSecond: 1000}
}

func (f *fakeControlPlane) workspaceClient(t *testing.T, pacing Pacing) *databricks.WorkspaceClient {
	t.Helper()
	UsePacing(f.server.URL, pacing)
	client, err := NewWorkspaceClient(f.config())
	require.NoError(t, err)
	return client
}

func (f *fakeControlPlane) throttle() *Throttle {
	return ThrottleFor(f.server.URL)
}

// offsets reports when each request arrived, measured from the fake starting up.
func (f *fakeControlPlane) offsets() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Duration(nil), f.requestOffsets...)
}

// meteredQuota is a rate limit that counts the requests it refuses, the way a real one
// does. It is what separates a client that paces itself from one that retries harder:
// the second never gets a request through.
type meteredQuota struct {
	window time.Duration

	mu       sync.Mutex
	closedAt time.Time
	refused  int
	admitted int
}

func (q *meteredQuota) admit() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	admitted := !now.Before(q.closedAt)
	if admitted {
		q.admitted++
	} else {
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

func (q *meteredQuota) admittedCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.admitted
}
