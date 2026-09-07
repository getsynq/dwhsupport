package databricks

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/databricks/databricks-sdk-go/common"
	"github.com/stretchr/testify/suite"
)

// fastPacing is the shipped behaviour with its waits scaled down, so a test exercises
// the real widen-on-refusal and decay-on-success convergence without sitting through
// production-sized intervals.
var fastPacing = Pacing{
	FirstInterval: 2 * time.Millisecond,
	MaxInterval:   40 * time.Millisecond,
	MaxPause:      200 * time.Millisecond,
	DecayAfter:    5,
	MaxAttempts:   8,
	WaitBudget:    2 * time.Second,
}

type ThrottleSuite struct {
	suite.Suite
}

func TestThrottleSuite(t *testing.T) {
	suite.Run(t, new(ThrottleSuite))
}

// TestUnpacedUntilRefused — a workspace that never refuses a request must pay nothing
// for any of this, so the throttle hands out slots with no spacing at all until the
// first refusal.
func (s *ThrottleSuite) TestUnpacedUntilRefused() {
	throttle := NewThrottle(fastPacing)
	for i := 0; i < 100; i++ {
		s.Zero(throttle.reserve())
	}
	s.Equal(ThrottleStats{}, throttle.Stats())
	s.False(throttle.Stats().Paced())
}

// TestRefusalWidensTheSpacing — each refusal doubles the spacing handed out, up to the
// cap, so a process walks its offered rate down until the workspace accepts it.
func (s *ThrottleSuite) TestRefusalWidensTheSpacing() {
	throttle := NewThrottle(fastPacing)

	throttle.Refused(0)
	s.Equal(fastPacing.FirstInterval, throttle.Stats().Interval)

	throttle.Refused(0)
	s.Equal(2*fastPacing.FirstInterval, throttle.Stats().Interval)

	for i := 0; i < 20; i++ {
		throttle.Refused(0)
	}
	s.Equal(fastPacing.MaxInterval, throttle.Stats().Interval, "spacing grew past the cap")
	s.True(throttle.Stats().Paced())
}

// TestRefusalHoldsEveryWaiterBack — the spacing is handed out from one place, so the
// pause a refusal opens applies to every request in flight rather than only the one
// that was refused.
func (s *ThrottleSuite) TestRefusalHoldsEveryWaiterBack() {
	throttle := NewThrottle(Pacing{
		FirstInterval: 50 * time.Millisecond,
		MaxInterval:   time.Second,
		MaxPause:      5 * time.Second,
		DecayAfter:    5,
		MaxAttempts:   8,
	})

	throttle.Refused(300 * time.Millisecond)

	// The first slot waits out what the API asked for; the ones behind it are spaced by
	// the interval the refusal adopted.
	s.InDelta(300*time.Millisecond, throttle.reserve(), float64(50*time.Millisecond))
	s.InDelta(350*time.Millisecond, throttle.reserve(), float64(50*time.Millisecond))
}

// TestRetryAfterBelowTheSpacingDoesNotSpeedThingsUp — the header is a floor, not a
// target: a workspace asking for a shorter wait than the pace already converged on
// must not undo that convergence.
func (s *ThrottleSuite) TestRetryAfterBelowTheSpacingDoesNotSpeedThingsUp() {
	throttle := NewThrottle(Pacing{
		FirstInterval: 200 * time.Millisecond,
		MaxInterval:   time.Second,
		MaxPause:      5 * time.Second,
		DecayAfter:    5,
		MaxAttempts:   8,
	})

	throttle.Refused(time.Millisecond)
	s.InDelta(200*time.Millisecond, throttle.reserve(), float64(50*time.Millisecond))
}

// TestRetryAfterIsCappedByMaxPause — the waiting happens inside the per-attempt
// timeout the SDK puts on a request, so a Retry-After longer than the cap would turn a
// refusal that should have been waited out into a request timing out mid-flight.
func (s *ThrottleSuite) TestRetryAfterIsCappedByMaxPause() {
	throttle := NewThrottle(Pacing{
		FirstInterval: 10 * time.Millisecond,
		MaxInterval:   time.Second,
		MaxPause:      100 * time.Millisecond,
		DecayAfter:    5,
		MaxAttempts:   8,
	})

	throttle.Refused(time.Hour)
	s.InDelta(100*time.Millisecond, throttle.reserve(), float64(50*time.Millisecond))
}

// TestSpacingDecaysBackToUnpaced — the quota is shared with everything else running
// against the workspace, so a process that has been refused once has to keep probing
// back towards full speed instead of staying slow for the rest of its life.
func (s *ThrottleSuite) TestSpacingDecaysBackToUnpaced() {
	throttle := NewThrottle(fastPacing)

	throttle.Refused(0)
	throttle.Refused(0)
	throttle.Refused(0)
	s.Equal(4*fastPacing.FirstInterval, throttle.Stats().Interval)

	// A decay step needs DecayAfter accepted requests, and the interval reaching less
	// than one step is what returns the process to unpaced.
	for step := 0; step < 3; step++ {
		for i := 0; i < fastPacing.DecayAfter; i++ {
			throttle.Accepted()
		}
	}
	s.Zero(throttle.Stats().Interval)
	s.True(throttle.Stats().Paced(), "the refusals still happened, and the caller still wants to hear about them")
}

// TestStatsSinceAnEarlierSnapshot — the throttle outlives any one scrape, so a caller
// reporting on its own run has to difference the counters. A run that was not paced
// must not inherit the log line of one that was, hours earlier.
func (s *ThrottleSuite) TestStatsSinceAnEarlierSnapshot() {
	throttle := NewThrottle(fastPacing)
	throttle.Refused(0)
	throttle.reserve()

	before := throttle.Stats()
	s.True(before.Paced())

	s.False(throttle.Stats().Since(before).Paced(), "a run during which nothing was refused reads as paced")

	throttle.Refused(0)
	throttle.reserve()
	during := throttle.Stats().Since(before)
	s.Equal(1, during.Refusals)
	s.Positive(during.Waited)
	s.Less(during.Waited, throttle.Stats().Waited, "the run's wait is the throttle's running total")
	s.Equal(throttle.Stats().Interval, during.Interval, "the spacing is a rate, not a count")
}

func (s *ThrottleSuite) TestAcquireStopsWhenTheContextIsDone() {
	throttle := NewThrottle(Pacing{
		FirstInterval: time.Hour,
		MaxInterval:   time.Hour,
		MaxPause:      time.Hour,
		DecayAfter:    5,
		MaxAttempts:   8,
	})
	throttle.Refused(0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := throttle.Acquire(ctx)
	s.ErrorIs(err, context.Canceled)
}

// TestOneThrottlePerWorkspace — the throttle has to be found by the workspace, however
// the caller happens to spell it, or the catalog listings and the lineage walk would
// each pace against a throttle of their own and neither would see the other's refusals.
func (s *ThrottleSuite) TestOneThrottlePerWorkspace() {
	const workspace = "https://dbc-12345678-9abc.cloud.databricks.com"
	throttle := ThrottleFor(workspace)

	s.Same(throttle, ThrottleFor(workspace+"/"))
	s.Same(throttle, ThrottleFor("dbc-12345678-9abc.cloud.databricks.com"))
	s.Same(throttle, ThrottleFor("HTTPS://DBC-12345678-9ABC.cloud.databricks.com"))
	s.Same(throttle, throttleForHost("dbc-12345678-9abc.cloud.databricks.com", DefaultPacing))
	s.NotSame(throttle, ThrottleFor("https://dbc-87654321-cba9.cloud.databricks.com"))
}

// TestUsePacingKeepsTheThrottleAWorkspaceAlreadyHas — pacing cannot be swapped under
// a request already in flight, so the first caller to ask for a workspace's throttle
// is the one that decides its pacing.
func (s *ThrottleSuite) TestUsePacingKeepsTheThrottleAWorkspaceAlreadyHas() {
	const workspace = "https://use-pacing-once.cloud.databricks.com"
	throttle := UsePacing(workspace, fastPacing)
	s.Equal(fastPacing, throttle.Pacing())

	s.Same(throttle, UsePacing(workspace, DefaultPacing))
	s.Equal(fastPacing, ThrottleFor(workspace).Pacing())
}

// TestRateLimitDetection covers the errors that mean "you are over the quota". The
// control plane does not answer only 429s, and a refusal read as an ordinary failure
// fails a whole scrape.
func (s *ThrottleSuite) TestRateLimitDetection() {
	for name, tc := range map[string]struct {
		err        error
		isLimit    bool
		retryAfter time.Duration
	}{
		"429 without a header": {
			err:     apiError(http.StatusTooManyRequests, "REQUEST_LIMIT_EXCEEDED", "Rate limit exceeded. Please try again later.", nil),
			isLimit: true,
		},
		"429 with Retry-After seconds": {
			err: apiError(http.StatusTooManyRequests, "RESOURCE_EXHAUSTED", "Rate limit exceeded.", http.Header{
				"Retry-After": []string{"30"},
			}),
			isLimit:    true,
			retryAfter: 30 * time.Second,
		},
		"429 with a nonsense Retry-After": {
			err: apiError(http.StatusTooManyRequests, "RESOURCE_EXHAUSTED", "Rate limit exceeded.", http.Header{
				"Retry-After": []string{"soon"},
			}),
			isLimit: true,
		},
		"400 carrying the rate-limit message": {
			err:     apiError(http.StatusBadRequest, "", "Rate limit exceeded. Please try again later.", nil),
			isLimit: true,
		},
		"500 carrying the rate-limit code": {
			err:     apiError(http.StatusInternalServerError, "REQUEST_LIMIT_EXCEEDED", "internal error", nil),
			isLimit: true,
		},
		"the error the pacing itself gives up with": {
			err:        &RateLimitedError{StatusCode: 429, RetryAfter: 5 * time.Second, Attempts: 8},
			isLimit:    true,
			retryAfter: 5 * time.Second,
		},
		"a table that does not exist": {
			err: apiError(http.StatusNotFound, "TABLE_DOES_NOT_EXIST", "not found", nil),
		},
		"a bad request": {
			err: apiError(http.StatusBadRequest, "INVALID_PARAMETER_VALUE", "boom", nil),
		},
		"a denied permission": {
			err: apiError(http.StatusForbidden, "PERMISSION_DENIED", "User does not have USE SCHEMA on Schema", nil),
		},
		"an error that is not from the API": {
			err: context.DeadlineExceeded,
		},
	} {
		s.Run(name, func() {
			retryAfter, isLimit := RateLimitRetryAfter(tc.err)
			s.Equal(tc.isLimit, isLimit)
			s.Equal(tc.retryAfter, retryAfter)
			s.Equal(tc.isLimit, IsRateLimitError(tc.err))
		})
	}
}

// TestRetryAfterAsHttpDate — Retry-After is specified as either a number of seconds or
// an HTTP date, and which one arrives depends on the layer that refused the request.
func (s *ThrottleSuite) TestRetryAfterAsHttpDate() {
	err := apiError(http.StatusTooManyRequests, "RESOURCE_EXHAUSTED", "Rate limit exceeded.", http.Header{
		"Retry-After": []string{time.Now().Add(45 * time.Second).UTC().Format(http.TimeFormat)},
	})

	retryAfter, isLimit := RateLimitRetryAfter(err)
	s.True(isLimit)
	s.InDelta(45*time.Second, retryAfter, float64(2*time.Second))

	past := apiError(http.StatusTooManyRequests, "RESOURCE_EXHAUSTED", "Rate limit exceeded.", http.Header{
		"Retry-After": []string{time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat)},
	})
	retryAfter, isLimit = RateLimitRetryAfter(past)
	s.True(isLimit)
	s.Zero(retryAfter, "a date already in the past asks for no wait")
}

// TestRefusalIsReadOffTheResponse — the pacing sits below the SDK's error mapping and
// so has to recognise a refusal in the response itself, including the shapes that do
// not come back as a 429.
func (s *ThrottleSuite) TestRefusalIsReadOffTheResponse() {
	for name, tc := range map[string]struct {
		status     int
		header     http.Header
		body       string
		isRefusal  bool
		errorCode  string
		retryAfter time.Duration
	}{
		"429": {
			status:    http.StatusTooManyRequests,
			body:      `{"error_code": "REQUEST_LIMIT_EXCEEDED", "message": "Rate limit exceeded. Please try again later."}`,
			isRefusal: true,
			errorCode: "REQUEST_LIMIT_EXCEEDED",
		},
		"429 with Retry-After": {
			status:     http.StatusTooManyRequests,
			header:     http.Header{"Retry-After": []string{"7"}},
			body:       `{"error_code": "RESOURCE_EXHAUSTED", "message": "Rate limit exceeded."}`,
			isRefusal:  true,
			errorCode:  "RESOURCE_EXHAUSTED",
			retryAfter: 7 * time.Second,
		},
		"400 carrying the rate-limit code": {
			status:    http.StatusBadRequest,
			body:      `{"error_code": "REQUEST_LIMIT_EXCEEDED", "message": "Too many requests"}`,
			isRefusal: true,
			errorCode: "REQUEST_LIMIT_EXCEEDED",
		},
		"500 carrying only the rate-limit message": {
			status:    http.StatusInternalServerError,
			body:      `{"message": "Rate limit exceeded. Please try again later."}`,
			isRefusal: true,
		},
		"a gateway refusing in plain text": {
			status:    http.StatusServiceUnavailable,
			body:      "Too many requests, please slow down",
			isRefusal: true,
		},
		"the paginated-listing refusal": {
			status: http.StatusBadRequest,
			body: `{"error_code": "INVALID_PARAMETER_VALUE", "message": "The ListTables result set is too ` +
				`large to return in a single response. Error code #UC-PGRQD"}`,
		},
		"a schema that does not exist": {
			status: http.StatusNotFound,
			body:   `{"error_code": "SCHEMA_DOES_NOT_EXIST", "message": "Schema does not exist."}`,
		},
		"a listing that worked": {
			status: http.StatusOK,
			body:   `{"tables": []}`,
		},
	} {
		s.Run(name, func() {
			resp := &http.Response{
				StatusCode: tc.status,
				Header:     tc.header,
				Body:       io.NopCloser(strings.NewReader(tc.body)),
			}
			if resp.Header == nil {
				resp.Header = http.Header{}
			}

			refused, isRefusal := refusalOf(resp)
			s.Equal(tc.isRefusal, isRefusal)
			if tc.isRefusal {
				s.Equal(tc.status, refused.statusCode)
				s.Equal(tc.errorCode, refused.errorCode)
				s.Equal(tc.retryAfter, refused.retryAfter)
			}

			// Whatever was read to decide has to be put back: the SDK maps the same body
			// into the error the caller sees.
			body, err := io.ReadAll(resp.Body)
			s.Require().NoError(err)
			s.Equal(tc.body, string(body), "the response body was consumed by reading it")
		})
	}
}

// TestGivingUpDoesNotAskTheSdkToTryAgain pins the one thing the text of
// RateLimitedError may not do. The SDK re-sends any error whose message contains
// REQUEST_LIMIT_EXCEEDED, and matches it on the error the transport returns — so an
// error quoting what the workspace answered would land the refusal back under the
// blind five-minute retry the type exists to prevent.
func (s *ThrottleSuite) TestGivingUpDoesNotAskTheSdkToTryAgain() {
	err := &RateLimitedError{
		StatusCode: http.StatusTooManyRequests,
		ErrorCode:  "REQUEST_LIMIT_EXCEEDED",
		Message:    "Rate limit exceeded. REQUEST_LIMIT_EXCEEDED, i/o timeout, connection refused",
		RetryAfter: time.Second,
		Attempts:   8,
		Waited:     1500 * time.Millisecond,
	}

	for _, sdkRetriesOn := range []string{
		"REQUEST_LIMIT_EXCEEDED",
		"connection reset by peer",
		"TLS handshake timeout",
		"connection refused",
		"Unexpected error",
		"i/o timeout",
	} {
		s.NotContains(err.Error(), sdkRetriesOn)
	}
	s.Contains(err.Error(), "rate limited by the workspace")
	s.Contains(err.Error(), "8 attempts")
	s.Contains(err.Error(), "429")
}

func apiError(statusCode int, errorCode, message string, header http.Header) error {
	err := &apierr.APIError{ErrorCode: errorCode, Message: message, StatusCode: statusCode}
	if header != nil {
		err.ResponseWrapper = &common.ResponseWrapper{Response: &http.Response{Header: header}}
	}
	return err
}
