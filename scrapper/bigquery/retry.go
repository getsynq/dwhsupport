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
}

var DefaultRateLimitConfig = RateLimitConfig{
	MaxRetries:          5,
	BaseDelay:           1 * time.Second,
	MaxDelay:            30 * time.Second,
	MetadataConcurrency: 20,
}

// withRateLimitRetry retries fn when it returns a rate-limit error (HTTP 429 /
// gRPC ResourceExhausted), using exponential backoff with jitter. Non-rate-limit
// errors are returned immediately.
func withRateLimitRetry[T any](ctx context.Context, cfg RateLimitConfig, fn func() (T, error)) (T, error) {
	var zero T
	delay := cfg.BaseDelay

	for attempt := range cfg.MaxRetries {
		result, err := fn()
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
