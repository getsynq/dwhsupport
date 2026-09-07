package lazy

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// TestGeneratesOnceUnderConcurrentGets is the reproducer for the race this used to
// carry: Get peeked at the generator to decide whether to run it, while the Get that
// was running it cleared the same field. Every concurrent user hits it — the per-table
// reads of a Databricks metrics scrape build their SQL executor from inside an
// errgroup — and it only ever showed up under the race detector.
func TestGeneratesOnceUnderConcurrentGets(t *testing.T) {
	var generated atomic.Int64
	value := New(func() (int, error) {
		generated.Add(1)
		return 42, nil
	})

	require.False(t, value.Has(), "nothing has been generated yet")

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := value.Get()
			require.NoError(t, err)
			require.Equal(t, 42, got)
			require.True(t, value.Has())
		}()
	}
	wg.Wait()

	require.Equal(t, int64(1), generated.Load(), "the generator ran more than once")
	require.True(t, value.Has())
}

// TestFailureIsGeneratedOnceToo — a generator that fails is not retried, which is what
// keeps a scrape that cannot build its SQL executor from trying to build one per table.
func TestFailureIsGeneratedOnceToo(t *testing.T) {
	var generated atomic.Int64
	boom := errors.New("boom")
	value := New(func() (int, error) {
		generated.Add(1)
		return 0, boom
	})

	for i := 0; i < 3; i++ {
		got, err := value.Get()
		require.ErrorIs(t, err, boom)
		require.Zero(t, got)
	}
	require.Equal(t, int64(1), generated.Load())
	require.True(t, value.Has(), "a failed generation has still happened")
}
