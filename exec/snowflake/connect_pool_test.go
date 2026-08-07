package snowflake

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A failed connect must not leave its pool behind. sql.OpenDB starts a
// connection-opener goroutine that survives a failed ping, and a misconfigured
// integration retries on a schedule indefinitely — so one leaked goroutine per
// attempt accumulates for as long as the process lives.
func TestNewSnowflakeExecutorClosesPoolOnFailedConnect(t *testing.T) {
	const attempts = 20

	baseline := runtime.NumGoroutine()

	for i := 0; i < attempts; i++ {
		// An empty password is rejected inside PingContext, which runs after sql.OpenDB
		// has started the opener, so this drives the path where the pool is abandoned
		// and must be closed. It needs no network, which keeps this a unit test.
		_, err := NewSnowflakeExecutor(context.Background(), &SnowflakeConf{
			Account: "no-such-account",
			User:    "no-such-user",
		})
		require.Error(t, err)
	}

	// Close signals the opener goroutine rather than joining it, so allow it to drain
	// instead of sampling immediately.
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() < baseline+attempts/2
	}, 5*time.Second, 50*time.Millisecond,
		"goroutines from %d failed connects were not released (baseline %d, still %d)",
		attempts, baseline, runtime.NumGoroutine())
}
