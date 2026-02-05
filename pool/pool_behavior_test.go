package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// =============================================================================
// Test Helpers
// =============================================================================

type testConn struct {
	id        string
	closed    atomic.Bool
	closedAt  time.Time
	createdAt time.Time
}

func (c *testConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.closedAt = time.Now()
	}
	return nil
}

func (c *testConn) IsClosed() bool {
	return c.closed.Load()
}

type testConnector struct {
	mu           sync.Mutex
	connectCount int
	connections  []*testConn
	delay        time.Duration
	failKeys     map[string]error
	onConnect    func(key string)
}

func newTestConnector() *testConnector {
	return &testConnector{
		failKeys: make(map[string]error),
	}
}

func (c *testConnector) Connect(ctx context.Context, key string) (*testConn, error) {
	if c.onConnect != nil {
		c.onConnect(key)
	}

	if c.delay > 0 {
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err, ok := c.failKeys[key]; ok {
		return nil, err
	}

	c.connectCount++
	conn := &testConn{
		id:        fmt.Sprintf("%s-%d", key, c.connectCount),
		createdAt: time.Now(),
	}
	c.connections = append(c.connections, conn)
	return conn, nil
}

func (c *testConnector) getConnectCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connectCount
}

func (c *testConnector) setFailKey(key string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failKeys[key] = err
}

func (c *testConnector) clearFailKey(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.failKeys, key)
}

// =============================================================================
// Pool Behavior Test Suite
// =============================================================================

type PoolBehaviorSuite struct {
	suite.Suite
	connector *testConnector
	pool      *Pool[string, *testConn]
}

func TestPoolBehaviorSuite(t *testing.T) {
	suite.Run(t, new(PoolBehaviorSuite))
}

func (s *PoolBehaviorSuite) SetupTest() {
	s.connector = newTestConnector()
	s.pool = New[string, *testConn](s.connector)
}

func (s *PoolBehaviorSuite) TearDownTest() {
	if s.pool != nil {
		s.pool.Close()
	}
}

// =============================================================================
// Basic Functionality Tests
// =============================================================================

func (s *PoolBehaviorSuite) TestAcquireCreatesNewConnection() {
	lease, err := s.pool.Acquire(context.Background(), "key1")
	require.NoError(s.T(), err)
	defer lease.Release()

	assert.NotNil(s.T(), lease.Value())
	assert.Equal(s.T(), "key1-1", lease.Value().id)
	assert.Equal(s.T(), 1, s.connector.getConnectCount())
}

func (s *PoolBehaviorSuite) TestAcquireReusesExistingConnection() {
	// First acquire
	lease1, err := s.pool.Acquire(context.Background(), "key1")
	require.NoError(s.T(), err)
	conn1 := lease1.Value()

	// Second acquire for same key (while first is still held)
	lease2, err := s.pool.Acquire(context.Background(), "key1")
	require.NoError(s.T(), err)
	conn2 := lease2.Value()

	// Should be the exact same connection
	assert.Same(s.T(), conn1, conn2, "Expected same connection to be reused")
	assert.Equal(s.T(), 1, s.connector.getConnectCount())

	lease1.Release()
	lease2.Release()
}

func (s *PoolBehaviorSuite) TestAcquireReusesAfterRelease() {
	// Acquire and release
	lease1, err := s.pool.Acquire(context.Background(), "key1")
	require.NoError(s.T(), err)
	conn1 := lease1.Value()
	lease1.Release()

	// Acquire again - should reuse
	lease2, err := s.pool.Acquire(context.Background(), "key1")
	require.NoError(s.T(), err)
	conn2 := lease2.Value()

	assert.Same(s.T(), conn1, conn2, "Expected same connection to be reused after release")
	assert.Equal(s.T(), 1, s.connector.getConnectCount())

	lease2.Release()
}

func (s *PoolBehaviorSuite) TestDifferentKeysGetDifferentConnections() {
	lease1, err := s.pool.Acquire(context.Background(), "key1")
	require.NoError(s.T(), err)
	defer lease1.Release()

	lease2, err := s.pool.Acquire(context.Background(), "key2")
	require.NoError(s.T(), err)
	defer lease2.Release()

	lease3, err := s.pool.Acquire(context.Background(), "key3")
	require.NoError(s.T(), err)
	defer lease3.Release()

	assert.NotSame(s.T(), lease1.Value(), lease2.Value())
	assert.NotSame(s.T(), lease2.Value(), lease3.Value())
	assert.Equal(s.T(), 3, s.connector.getConnectCount())
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

func (s *PoolBehaviorSuite) TestConcurrentAcquireSameKey() {
	s.connector.delay = 50 * time.Millisecond // Slow connection to ensure overlap

	const numGoroutines = 10
	var wg sync.WaitGroup
	leases := make([]*Lease[string, *testConn], numGoroutines)
	errs := make([]error, numGoroutines)

	// Start all goroutines simultaneously
	start := make(chan struct{})
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			lease, err := s.pool.Acquire(context.Background(), "shared-key")
			leases[idx] = lease
			errs[idx] = err
		}(i)
	}

	close(start) // Release all goroutines
	wg.Wait()

	// Verify all succeeded
	for i, err := range errs {
		assert.NoError(s.T(), err, "Goroutine %d failed", i)
	}

	// Verify only one connection was created
	assert.Equal(s.T(), 1, s.connector.getConnectCount())

	// Verify all got the same connection
	var firstConn *testConn
	for i, lease := range leases {
		if lease == nil {
			continue
		}
		if firstConn == nil {
			firstConn = lease.Value()
		} else {
			assert.Same(s.T(), firstConn, lease.Value(), "Goroutine %d got different connection", i)
		}
		lease.Release()
	}

	// Verify usage tracking
	usage := s.pool.Usage()
	assert.Equal(s.T(), 0, usage["shared-key"])
}

func (s *PoolBehaviorSuite) TestConcurrentAcquireDifferentKeys() {
	s.connector.delay = 10 * time.Millisecond

	const numKeys = 20
	var wg sync.WaitGroup
	leases := make([]*Lease[string, *testConn], numKeys)
	errs := make([]error, numKeys)

	start := make(chan struct{})
	for i := 0; i < numKeys; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			key := fmt.Sprintf("key-%d", idx)
			lease, err := s.pool.Acquire(context.Background(), key)
			leases[idx] = lease
			errs[idx] = err
		}(i)
	}

	close(start)
	wg.Wait()

	// Verify all succeeded
	for i, err := range errs {
		assert.NoError(s.T(), err, "Key %d failed", i)
	}

	// Should have created numKeys connections
	assert.Equal(s.T(), numKeys, s.connector.getConnectCount())

	// Cleanup
	for _, lease := range leases {
		if lease != nil {
			lease.Release()
		}
	}
}

func (s *PoolBehaviorSuite) TestConcurrentAcquireAndRelease() {
	const iterations = 100
	const numGoroutines = 10

	var wg sync.WaitGroup
	errCount := atomic.Int32{}

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				lease, err := s.pool.Acquire(context.Background(), "contended-key")
				if err != nil {
					errCount.Add(1)
					continue
				}
				time.Sleep(time.Microsecond)
				lease.Release()
			}
		}()
	}

	wg.Wait()

	assert.Equal(s.T(), int32(0), errCount.Load())
	assert.Equal(s.T(), 1, s.connector.getConnectCount())
}

// =============================================================================
// Error Handling Tests
// =============================================================================

func (s *PoolBehaviorSuite) TestConnectErrorPropagates() {
	expectedErr := errors.New("connection refused")
	s.connector.setFailKey("bad-key", expectedErr)

	_, err := s.pool.Acquire(context.Background(), "bad-key")

	assert.Error(s.T(), err)
	assert.ErrorIs(s.T(), err, expectedErr)
}

func (s *PoolBehaviorSuite) TestConnectErrorPropagesToAllWaiters() {
	expectedErr := errors.New("connection refused")
	s.connector.delay = 50 * time.Millisecond
	s.connector.setFailKey("failing-key", expectedErr)

	const numWaiters = 5
	var wg sync.WaitGroup
	errs := make([]error, numWaiters)

	start := make(chan struct{})
	for i := 0; i < numWaiters; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, err := s.pool.Acquire(context.Background(), "failing-key")
			errs[idx] = err
		}(i)
	}

	close(start)
	wg.Wait()

	// All should have received the same error
	for i, err := range errs {
		assert.Error(s.T(), err, "Waiter %d should have error", i)
		assert.ErrorIs(s.T(), err, expectedErr, "Waiter %d has wrong error", i)
	}

	// No connection should have been created
	assert.Equal(s.T(), 0, s.connector.getConnectCount())
}

func (s *PoolBehaviorSuite) TestRetryAfterError() {
	s.connector.setFailKey("retry-key", errors.New("temporary error"))

	// First attempt fails
	_, err := s.pool.Acquire(context.Background(), "retry-key")
	assert.Error(s.T(), err)

	// Clear the error
	s.connector.clearFailKey("retry-key")

	// Second attempt should succeed
	lease, err := s.pool.Acquire(context.Background(), "retry-key")
	require.NoError(s.T(), err)
	defer lease.Release()

	assert.NotNil(s.T(), lease.Value())
}

func (s *PoolBehaviorSuite) TestContextCancellation() {
	s.connector.delay = 1 * time.Second // Long delay

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := s.pool.Acquire(ctx, "timeout-key")

	assert.Error(s.T(), err)
	assert.ErrorIs(s.T(), err, context.DeadlineExceeded)
}

// =============================================================================
// Pool Lifecycle Tests
// =============================================================================

func (s *PoolBehaviorSuite) TestCloseClosesAllConnections() {
	// Create multiple connections
	lease1, _ := s.pool.Acquire(context.Background(), "key1")
	lease2, _ := s.pool.Acquire(context.Background(), "key2")
	lease3, _ := s.pool.Acquire(context.Background(), "key3")

	conn1 := lease1.Value()
	conn2 := lease2.Value()
	conn3 := lease3.Value()

	// Release back to pool
	lease1.Release()
	lease2.Release()
	lease3.Release()

	// Close pool
	err := s.pool.Close()
	require.NoError(s.T(), err)

	// All connections should be closed
	assert.True(s.T(), conn1.IsClosed(), "conn1 should be closed")
	assert.True(s.T(), conn2.IsClosed(), "conn2 should be closed")
	assert.True(s.T(), conn3.IsClosed(), "conn3 should be closed")

	s.pool = nil // Prevent double close in TearDownTest
}

func (s *PoolBehaviorSuite) TestAcquireAfterCloseReturnsError() {
	s.pool.Close()

	_, err := s.pool.Acquire(context.Background(), "key1")

	assert.Error(s.T(), err)
	s.pool = nil // Prevent double close in TearDownTest
}

func (s *PoolBehaviorSuite) TestDoubleCloseIsSafe() {
	lease, _ := s.pool.Acquire(context.Background(), "key1")
	lease.Release()

	// Multiple closes should not panic
	assert.NotPanics(s.T(), func() {
		s.pool.Close()
		s.pool.Close()
		s.pool.Close()
	})

	s.pool = nil
}

func (s *PoolBehaviorSuite) TestCloseWhileConnecting() {
	s.connector.delay = 100 * time.Millisecond

	// Start acquiring in background
	errCh := make(chan error, 1)
	go func() {
		_, err := s.pool.Acquire(context.Background(), "key1")
		errCh <- err
	}()

	// Wait a bit for connect to start
	time.Sleep(20 * time.Millisecond)

	// Close the pool
	s.pool.Close()

	// The acquire should fail
	err := <-errCh
	assert.Error(s.T(), err)

	s.pool = nil
}

// =============================================================================
// Usage Tracking Tests
// =============================================================================

func (s *PoolBehaviorSuite) TestUsageTrackingAccuracy() {
	// Initial: no usage
	usage := s.pool.Usage()
	assert.Empty(s.T(), usage)

	// Acquire 3 leases for same key
	lease1, _ := s.pool.Acquire(context.Background(), "key1")
	assert.Equal(s.T(), 1, s.pool.Usage()["key1"])

	lease2, _ := s.pool.Acquire(context.Background(), "key1")
	lease3, _ := s.pool.Acquire(context.Background(), "key1")
	assert.Equal(s.T(), 3, s.pool.Usage()["key1"])

	// Release one
	lease1.Release()
	assert.Equal(s.T(), 2, s.pool.Usage()["key1"])

	// Release remaining
	lease2.Release()
	lease3.Release()
	assert.Equal(s.T(), 0, s.pool.Usage()["key1"])
}

func (s *PoolBehaviorSuite) TestUsageWithMultipleKeys() {
	lease1a, _ := s.pool.Acquire(context.Background(), "key1")
	lease1b, _ := s.pool.Acquire(context.Background(), "key1")
	lease2, _ := s.pool.Acquire(context.Background(), "key2")
	lease3a, _ := s.pool.Acquire(context.Background(), "key3")
	lease3b, _ := s.pool.Acquire(context.Background(), "key3")
	lease3c, _ := s.pool.Acquire(context.Background(), "key3")

	usage := s.pool.Usage()
	assert.Equal(s.T(), 2, usage["key1"])
	assert.Equal(s.T(), 1, usage["key2"])
	assert.Equal(s.T(), 3, usage["key3"])

	lease1a.Release()
	lease1b.Release()
	lease2.Release()
	lease3a.Release()
	lease3b.Release()
	lease3c.Release()
}

// =============================================================================
// Lease Behavior Tests
// =============================================================================

func (s *PoolBehaviorSuite) TestLeaseMultipleReleaseSafe() {
	lease, _ := s.pool.Acquire(context.Background(), "key1")

	// Multiple releases should be safe
	assert.NotPanics(s.T(), func() {
		lease.Release()
		lease.Release()
		lease.Release()
	})

	// Usage should be 0, not negative
	assert.Equal(s.T(), 0, s.pool.Usage()["key1"])
}

func (s *PoolBehaviorSuite) TestLeaseValueAfterRelease() {
	lease, _ := s.pool.Acquire(context.Background(), "key1")
	lease.Release()

	// Value should be nil after release
	assert.Nil(s.T(), lease.Value())
}

func (s *PoolBehaviorSuite) TestLeaseReleasedFlag() {
	lease, _ := s.pool.Acquire(context.Background(), "key1")

	assert.False(s.T(), lease.Released())

	lease.Release()

	assert.True(s.T(), lease.Released())
}

func (s *PoolBehaviorSuite) TestLeaseCloseEqualsRelease() {
	lease, _ := s.pool.Acquire(context.Background(), "key1")

	err := lease.Close()

	assert.NoError(s.T(), err)
	assert.True(s.T(), lease.Released())
}

// =============================================================================
// Garbage Collection Test Suite
// =============================================================================

type PoolGCSuite struct {
	suite.Suite
	connector *testConnector
	pool      *Pool[string, *testConn]
}

func TestPoolGCSuite(t *testing.T) {
	suite.Run(t, new(PoolGCSuite))
}

func (s *PoolGCSuite) SetupTest() {
	s.connector = newTestConnector()
	s.pool = New[string, *testConn](s.connector,
		WithCleanerInterval[string, *testConn](10*time.Millisecond),
		WithMaxIdleDuration[string, *testConn](30*time.Millisecond),
	)
}

func (s *PoolGCSuite) TearDownTest() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *PoolGCSuite) TestIdleConnectionsAreEvicted() {
	// Create and release connection
	lease, err := s.pool.Acquire(context.Background(), "idle-key")
	require.NoError(s.T(), err)
	conn := lease.Value()
	lease.Release()

	// Connection should still be in pool
	_, ok := s.pool.Usage()["idle-key"]
	assert.True(s.T(), ok, "Connection should be in pool after release")

	// Wait for eviction
	time.Sleep(100 * time.Millisecond)

	// Connection should be evicted
	_, ok = s.pool.Usage()["idle-key"]
	assert.False(s.T(), ok, "Connection should have been evicted")

	// Connection should be closed
	assert.True(s.T(), conn.IsClosed())
}

func (s *PoolGCSuite) TestActiveConnectionsAreNotEvicted() {
	// Acquire and hold connection
	lease, err := s.pool.Acquire(context.Background(), "active-key")
	require.NoError(s.T(), err)
	conn := lease.Value()

	// Wait longer than eviction time
	time.Sleep(100 * time.Millisecond)

	// Connection should NOT be evicted (still in use)
	assert.False(s.T(), conn.IsClosed(), "Active connection should not be evicted")
	assert.Equal(s.T(), 1, s.pool.Usage()["active-key"])

	lease.Release()
}

func (s *PoolGCSuite) TestManualGCTriggersCleanup() {
	// Use a pool with very long cleaner interval
	s.pool.Close()
	s.pool = New[string, *testConn](s.connector,
		WithCleanerInterval[string, *testConn](1*time.Hour),
		WithMaxIdleDuration[string, *testConn](10*time.Millisecond),
	)

	// Create and release connection
	lease, err := s.pool.Acquire(context.Background(), "gc-key")
	require.NoError(s.T(), err)
	conn := lease.Value()
	lease.Release()

	// Wait for idle duration to pass
	time.Sleep(50 * time.Millisecond)

	// Manually trigger GC
	s.pool.GC()

	// Give cleanup goroutine time to run
	time.Sleep(20 * time.Millisecond)

	// Connection should be evicted
	assert.True(s.T(), conn.IsClosed(), "Connection should be closed after manual GC")
}

// =============================================================================
// Hooks Test Suite
// =============================================================================

type PoolHooksSuite struct {
	suite.Suite
	connector *testConnector
	pool      *Pool[string, *testConn]

	connectingCalled atomic.Int32
	connectedCalled  atomic.Int32
	acquireCalled    atomic.Int32
	waitingCalled    atomic.Int32
	releaseCalled    atomic.Int32
	evictCalled      atomic.Int32
	errorCalled      atomic.Int32
	lastError        error
	mu               sync.Mutex
}

func TestPoolHooksSuite(t *testing.T) {
	suite.Run(t, new(PoolHooksSuite))
}

func (s *PoolHooksSuite) SetupTest() {
	s.connector = newTestConnector()
	s.connectingCalled.Store(0)
	s.connectedCalled.Store(0)
	s.acquireCalled.Store(0)
	s.waitingCalled.Store(0)
	s.releaseCalled.Store(0)
	s.evictCalled.Store(0)
	s.errorCalled.Store(0)
	s.lastError = nil

	hooks := Hooks[string, *testConn]{
		OnConnecting: func(ctx context.Context, key string) {
			s.connectingCalled.Add(1)
		},
		OnConnected: func(ctx context.Context, key string, value *testConn) {
			s.connectedCalled.Add(1)
		},
		OnAcquire: func(ctx context.Context, key string, value *testConn) {
			s.acquireCalled.Add(1)
		},
		OnWaiting: func(ctx context.Context, key string) {
			s.waitingCalled.Add(1)
		},
		OnRelease: func(ctx context.Context, key string, value *testConn) {
			s.releaseCalled.Add(1)
		},
		OnEvict: func(ctx context.Context, key string, value *testConn) {
			s.evictCalled.Add(1)
		},
		OnConnectError: func(ctx context.Context, key string, err error) {
			s.errorCalled.Add(1)
			s.mu.Lock()
			s.lastError = err
			s.mu.Unlock()
		},
	}

	s.pool = New[string, *testConn](s.connector,
		WithHooks[string, *testConn](hooks),
		WithCleanerInterval[string, *testConn](10*time.Millisecond),
		WithMaxIdleDuration[string, *testConn](30*time.Millisecond),
	)
}

func (s *PoolHooksSuite) TearDownTest() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *PoolHooksSuite) TestOnConnectingCalledOnce() {
	s.connector.delay = 30 * time.Millisecond

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, _ := s.pool.Acquire(context.Background(), "hook-key")
			if lease != nil {
				lease.Release()
			}
		}()
	}
	wg.Wait()

	assert.Equal(s.T(), int32(1), s.connectingCalled.Load())
}

func (s *PoolHooksSuite) TestOnConnectedCalledOnce() {
	s.connector.delay = 30 * time.Millisecond

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, _ := s.pool.Acquire(context.Background(), "hook-key")
			if lease != nil {
				lease.Release()
			}
		}()
	}
	wg.Wait()

	assert.Equal(s.T(), int32(1), s.connectedCalled.Load())
}

func (s *PoolHooksSuite) TestOnWaitingCalledForWaiters() {
	s.connector.delay = 30 * time.Millisecond

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, _ := s.pool.Acquire(context.Background(), "hook-key")
			if lease != nil {
				lease.Release()
			}
		}()
	}
	wg.Wait()

	// 2 waiters (first one doesn't wait)
	assert.Equal(s.T(), int32(2), s.waitingCalled.Load())
}

func (s *PoolHooksSuite) TestOnAcquireCalledOnReuse() {
	// First acquire - creates new connection
	lease1, _ := s.pool.Acquire(context.Background(), "hook-key")
	lease1.Release()

	assert.Equal(s.T(), int32(0), s.acquireCalled.Load())

	// Second acquire - reuses from pool
	lease2, _ := s.pool.Acquire(context.Background(), "hook-key")
	lease2.Release()

	assert.Equal(s.T(), int32(1), s.acquireCalled.Load())
}

func (s *PoolHooksSuite) TestOnEvictCalledOnCleanup() {
	lease, _ := s.pool.Acquire(context.Background(), "hook-key")
	lease.Release()

	// Wait for eviction
	time.Sleep(100 * time.Millisecond)

	assert.Equal(s.T(), int32(1), s.evictCalled.Load())
}

func (s *PoolHooksSuite) TestOnConnectErrorCalled() {
	expectedErr := errors.New("connect failed")
	s.connector.setFailKey("error-key", expectedErr)

	s.pool.Acquire(context.Background(), "error-key")

	assert.Equal(s.T(), int32(1), s.errorCalled.Load())
	s.mu.Lock()
	assert.ErrorIs(s.T(), s.lastError, expectedErr)
	s.mu.Unlock()
}

// =============================================================================
// Stress Test Suite
// =============================================================================

type PoolStressSuite struct {
	suite.Suite
	connector *testConnector
	pool      *Pool[string, *testConn]
}

func TestPoolStressSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress tests in short mode")
	}
	suite.Run(t, new(PoolStressSuite))
}

func (s *PoolStressSuite) SetupTest() {
	s.connector = newTestConnector()
	s.pool = New[string, *testConn](s.connector)
}

func (s *PoolStressSuite) TearDownTest() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *PoolStressSuite) TestHighConcurrencyStress() {
	const numGoroutines = 100
	const numIterations = 100
	const numKeys = 10

	var wg sync.WaitGroup
	errorCount := atomic.Int32{}

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				key := fmt.Sprintf("key-%d", j%numKeys)
				lease, err := s.pool.Acquire(context.Background(), key)
				if err != nil {
					errorCount.Add(1)
					continue
				}
				time.Sleep(time.Microsecond * time.Duration(id%10))
				lease.Release()
			}
		}(i)
	}

	wg.Wait()

	assert.Equal(s.T(), int32(0), errorCount.Load())
	assert.Equal(s.T(), numKeys, s.connector.getConnectCount())
}

func (s *PoolStressSuite) TestRapidAcquireReleaseCycles() {
	const cycles = 1000

	for i := 0; i < cycles; i++ {
		lease, err := s.pool.Acquire(context.Background(), "rapid-key")
		require.NoError(s.T(), err, "Cycle %d failed", i)
		lease.Release()
	}

	assert.Equal(s.T(), 1, s.connector.getConnectCount())
}

func (s *PoolStressSuite) TestMixedKeyAccessPattern() {
	const numGoroutines = 50
	const numIterations = 50

	var wg sync.WaitGroup
	errorCount := atomic.Int32{}

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				// Mix of shared and unique keys
				var key string
				if j%3 == 0 {
					key = "shared-key"
				} else {
					key = fmt.Sprintf("goroutine-%d-iter-%d", id, j)
				}

				lease, err := s.pool.Acquire(context.Background(), key)
				if err != nil {
					errorCount.Add(1)
					continue
				}
				time.Sleep(time.Microsecond)
				lease.Release()
			}
		}(i)
	}

	wg.Wait()

	assert.Equal(s.T(), int32(0), errorCount.Load())
}
