package bigquery

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWithRateLimitRetryCallTimeout proves a hung call is bounded by CallTimeout
// rather than blocking until the parent context's (far-off) deadline. This is the
// core guard for the catalog-scrape hang: the BigQuery client would otherwise
// retry a stalled metadata request forever.
func TestWithRateLimitRetryCallTimeout(t *testing.T) {
	cfg := RateLimitConfig{
		MaxRetries:  3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    time.Millisecond,
		CallTimeout: 100 * time.Millisecond,
	}

	start := time.Now()
	_, err := withRateLimitRetry(context.Background(), cfg, func(ctx context.Context) (int, error) {
		// Simulate a call that hangs until its context is cancelled.
		<-ctx.Done()
		return 0, ctx.Err()
	})
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Less(t, elapsed, 2*time.Second, "CallTimeout must bound a hung call")
}

// TestWithRateLimitRetryNoTimeout verifies CallTimeout=0 leaves the call
// unbounded (the parent context governs), preserving the opt-out.
func TestWithRateLimitRetryNoTimeout(t *testing.T) {
	cfg := RateLimitConfig{MaxRetries: 1, CallTimeout: 0}

	called := false
	_, err := withRateLimitRetry(context.Background(), cfg, func(ctx context.Context) (int, error) {
		called = true
		require.NoError(t, ctx.Err())
		_, hasDeadline := ctx.Deadline()
		require.False(t, hasDeadline, "CallTimeout=0 must not impose a deadline")
		return 42, nil
	})
	require.NoError(t, err)
	require.True(t, called)
}
