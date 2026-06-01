package bigquery

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/getsynq/dwhsupport/logging"
)

// RateLimitConfig controls retry behavior for BigQuery API rate limits.
type RateLimitConfig struct {
	// MaxRetries is the maximum number of retry attempts for rate-limited requests.
	MaxRetries int
	// BaseDelay is the initial backoff delay before the first retry.
	BaseDelay time.Duration
	// MaxDelay caps the exponential backoff.
	MaxDelay time.Duration
	// MetadataConcurrency limits the number of concurrent table.Metadata() calls.
	MetadataConcurrency int
	// CallTimeout bounds each individual BigQuery API call — including the
	// client's own internal retries. It is the real guard against a stalled
	// metadata call hanging a scrape: the BigQuery client retries a fast-failing
	// request forever, so only a context deadline stops it. Without this a single
	// blackholed call blocks until the caller's context (e.g. a 2h job timeout)
	// fires, wedging the whole concurrent fan-out behind it. Zero disables the
	// per-call bound.
	//
	// The default is generous on purpose: in practice these calls run ~100ms
	// (p99.9 ~0.6s; the slowest observed across ~10M calls/day was ~29s), so 60s
	// is ~2x the real-world worst case — it never trips a healthy call but still
	// catches a hang within a minute instead of hours.
	CallTimeout time.Duration
}

var DefaultRateLimitConfig = RateLimitConfig{
	MaxRetries:          5,
	BaseDelay:           1 * time.Second,
	MaxDelay:            30 * time.Second,
	MetadataConcurrency: 20,
	CallTimeout:         60 * time.Second,
}

// withRateLimitRetry runs fn, retrying when it returns a rate-limit error (HTTP
// 429 / gRPC ResourceExhausted) using exponential backoff with jitter.
// Non-rate-limit errors are returned immediately. Each attempt is given its own
// context bounded by cfg.CallTimeout so a stalled call (which the BigQuery client
// would otherwise retry internally forever) fails promptly instead of hanging.
func withRateLimitRetry[T any](ctx context.Context, cfg RateLimitConfig, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	delay := cfg.BaseDelay

	for attempt := range cfg.MaxRetries {
		result, err := callWithTimeout(ctx, cfg.CallTimeout, fn)
		if err == nil {
			return result, nil
		}
		if !errIsRateLimited(err) {
			return zero, err
		}
		if attempt == cfg.MaxRetries-1 {
			return zero, err
		}

		jitter := time.Duration(rand.Int64N(int64(delay / 2)))
		sleep := delay + jitter

		logging.GetLogger(ctx).
			WithField("attempt", attempt+1).
			WithField("delay", sleep.String()).
			Warn("BigQuery rate limited, retrying")

		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(sleep):
		}

		delay = min(delay*2, cfg.MaxDelay)
	}

	return zero, nil
}

// callWithTimeout invokes fn with a child context bounded by timeout (when > 0),
// so the BigQuery client's internal retry loop is cut off rather than running
// until the parent context's far-off deadline.
func callWithTimeout[T any](ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) (T, error) {
	if timeout <= 0 {
		return fn(ctx)
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fn(callCtx)
}
