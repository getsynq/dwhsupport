package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockCloser is a simple test implementation of Closer
type mockCloser struct {
	id     string
	closed atomic.Bool
}

func (m *mockCloser) Close() error {
	m.closed.Store(true)
	return nil
}

func (m *mockCloser) IsClosed() bool {
	return m.closed.Load()
}

func TestPool_Acquire_NewConnection(t *testing.T) {
	connectCalls := atomic.Int32{}
	connector := ConnectorFunc[string, *mockCloser](func(ctx context.Context, key string) (*mockCloser, error) {
		connectCalls.Add(1)
		return &mockCloser{id: key}, nil
	})

	p := New(connector)
	defer p.Close()

	lease, err := p.Acquire(context.Background(), "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer lease.Release()

	if lease.Value().id != "key1" {
		t.Errorf("expected id 'key1', got %q", lease.Value().id)
	}

	if connectCalls.Load() != 1 {
		t.Errorf("expected 1 connect call, got %d", connectCalls.Load())
	}
}

func TestPool_Acquire_ReuseConnection(t *testing.T) {
	connectCalls := atomic.Int32{}
	connector := ConnectorFunc[string, *mockCloser](func(ctx context.Context, key string) (*mockCloser, error) {
		connectCalls.Add(1)
		return &mockCloser{id: key}, nil
	})

	p := New(connector)
	defer p.Close()

	// First acquire
	lease1, err := p.Acquire(context.Background(), "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Second acquire for same key
	lease2, err := p.Acquire(context.Background(), "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be same underlying connection
	if lease1.Value() != lease2.Value() {
		t.Error("expected same connection to be reused")
	}

	// Should only have connected once
	if connectCalls.Load() != 1 {
		t.Errorf("expected 1 connect call, got %d", connectCalls.Load())
	}

	lease1.Release()
	lease2.Release()
}

func TestPool_Acquire_ConcurrentSameKey(t *testing.T) {
	connectCalls := atomic.Int32{}
	startCh := make(chan struct{})

	connector := ConnectorFunc[string, *mockCloser](func(ctx context.Context, key string) (*mockCloser, error) {
		connectCalls.Add(1)
		<-startCh // Wait for signal
		return &mockCloser{id: key}, nil
	})

	p := New(connector)
	defer p.Close()

	var wg sync.WaitGroup
	leases := make([]*Lease[string, *mockCloser], 3)
	errs := make([]error, 3)

	// Start 3 concurrent acquires
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			lease, err := p.Acquire(context.Background(), "key1")
			leases[idx] = lease
			errs[idx] = err
		}(i)
	}

	// Give goroutines time to start
	time.Sleep(50 * time.Millisecond)

	// Release the connection
	close(startCh)
	wg.Wait()

	// Check all succeeded
	for i, err := range errs {
		if err != nil {
			t.Errorf("lease %d: unexpected error: %v", i, err)
		}
	}

	// Should only connect once
	if connectCalls.Load() != 1 {
		t.Errorf("expected 1 connect call, got %d", connectCalls.Load())
	}

	// All should have same underlying connection
	for i := 1; i < len(leases); i++ {
		if leases[i].Value() != leases[0].Value() {
			t.Error("expected all leases to share same connection")
		}
	}

	// Clean up
	for _, l := range leases {
		if l != nil {
			l.Release()
		}
	}
}

func TestPool_Acquire_DifferentKeys(t *testing.T) {
	connectCalls := atomic.Int32{}
	connector := ConnectorFunc[string, *mockCloser](func(ctx context.Context, key string) (*mockCloser, error) {
		connectCalls.Add(1)
		return &mockCloser{id: key}, nil
	})

	p := New(connector)
	defer p.Close()

	lease1, err := p.Acquire(context.Background(), "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer lease1.Release()

	lease2, err := p.Acquire(context.Background(), "key2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer lease2.Release()

	if connectCalls.Load() != 2 {
		t.Errorf("expected 2 connect calls, got %d", connectCalls.Load())
	}

	if lease1.Value() == lease2.Value() {
		t.Error("expected different connections for different keys")
	}
}

func TestPool_Acquire_ConnectError(t *testing.T) {
	expectedErr := errors.New("connection failed")
	connector := ConnectorFunc[string, *mockCloser](func(ctx context.Context, key string) (*mockCloser, error) {
		return nil, expectedErr
	})

	p := New(connector)
	defer p.Close()

	lease, err := p.Acquire(context.Background(), "key1")
	if err == nil {
		lease.Release()
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestPool_Usage(t *testing.T) {
	connector := ConnectorFunc[string, *mockCloser](func(ctx context.Context, key string) (*mockCloser, error) {
		return &mockCloser{id: key}, nil
	})

	p := New(connector)
	defer p.Close()

	// Initial usage should be empty
	usage := p.Usage()
	if len(usage) != 0 {
		t.Errorf("expected empty usage, got %v", usage)
	}

	// Acquire one
	lease1, _ := p.Acquire(context.Background(), "key1")
	usage = p.Usage()
	if usage["key1"] != 1 {
		t.Errorf("expected usage 1, got %d", usage["key1"])
	}

	// Acquire second for same key
	lease2, _ := p.Acquire(context.Background(), "key1")
	usage = p.Usage()
	if usage["key1"] != 2 {
		t.Errorf("expected usage 2, got %d", usage["key1"])
	}

	// Release one
	lease1.Release()
	usage = p.Usage()
	if usage["key1"] != 1 {
		t.Errorf("expected usage 1 after release, got %d", usage["key1"])
	}

	// Release second
	lease2.Release()
	usage = p.Usage()
	if usage["key1"] != 0 {
		t.Errorf("expected usage 0, got %d", usage["key1"])
	}
}

func TestPool_Close(t *testing.T) {
	connector := ConnectorFunc[string, *mockCloser](func(ctx context.Context, key string) (*mockCloser, error) {
		return &mockCloser{id: key}, nil
	})

	p := New(connector)

	lease, _ := p.Acquire(context.Background(), "key1")
	conn := lease.Value()
	lease.Release()

	// Close pool
	err := p.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Connection should be closed
	if !conn.IsClosed() {
		t.Error("expected connection to be closed")
	}

	// Subsequent acquire should fail
	_, err = p.Acquire(context.Background(), "key1")
	if err == nil {
		t.Error("expected error after pool close")
	}
}

func TestPool_GC(t *testing.T) {
	connector := ConnectorFunc[string, *mockCloser](func(ctx context.Context, key string) (*mockCloser, error) {
		return &mockCloser{id: key}, nil
	})

	p := New(connector,
		WithCleanerInterval[string, *mockCloser](10*time.Millisecond),
		WithMaxIdleDuration[string, *mockCloser](50*time.Millisecond),
	)
	defer p.Close()

	lease, _ := p.Acquire(context.Background(), "key1")
	conn := lease.Value()
	lease.Release()

	// Should still be in pool
	usage := p.Usage()
	if _, ok := usage["key1"]; !ok {
		t.Error("expected connection to still be in pool")
	}

	// Wait for cleanup
	time.Sleep(100 * time.Millisecond)

	// Should be cleaned up
	usage = p.Usage()
	if _, ok := usage["key1"]; ok {
		t.Error("expected connection to be cleaned up")
	}

	// Connection should be closed
	if !conn.IsClosed() {
		t.Error("expected connection to be closed after cleanup")
	}
}

func TestPool_Hooks(t *testing.T) {
	var (
		connectingCalled atomic.Bool
		connectedCalled  atomic.Bool
		acquireCalled    atomic.Bool
		evictCalled      atomic.Bool
	)

	hooks := Hooks[string, *mockCloser]{
		OnConnecting: func(ctx context.Context, key string) {
			connectingCalled.Store(true)
		},
		OnConnected: func(ctx context.Context, key string, value *mockCloser) {
			connectedCalled.Store(true)
		},
		OnAcquire: func(ctx context.Context, key string, value *mockCloser) {
			acquireCalled.Store(true)
		},
		OnEvict: func(ctx context.Context, key string, value *mockCloser) {
			evictCalled.Store(true)
		},
	}

	connector := ConnectorFunc[string, *mockCloser](func(ctx context.Context, key string) (*mockCloser, error) {
		return &mockCloser{id: key}, nil
	})

	p := New(connector,
		WithHooks[string, *mockCloser](hooks),
		WithCleanerInterval[string, *mockCloser](10*time.Millisecond),
		WithMaxIdleDuration[string, *mockCloser](20*time.Millisecond),
	)
	defer p.Close()

	// First acquire - should call OnConnecting and OnConnected
	lease1, _ := p.Acquire(context.Background(), "key1")
	if !connectingCalled.Load() {
		t.Error("expected OnConnecting to be called")
	}
	if !connectedCalled.Load() {
		t.Error("expected OnConnected to be called")
	}

	// Second acquire - should call OnAcquire
	lease2, _ := p.Acquire(context.Background(), "key1")
	if !acquireCalled.Load() {
		t.Error("expected OnAcquire to be called")
	}

	lease1.Release()
	lease2.Release()

	// Wait for eviction
	time.Sleep(100 * time.Millisecond)
	if !evictCalled.Load() {
		t.Error("expected OnEvict to be called")
	}
}

func TestLease_MultipleRelease(t *testing.T) {
	connector := ConnectorFunc[string, *mockCloser](func(ctx context.Context, key string) (*mockCloser, error) {
		return &mockCloser{id: key}, nil
	})

	p := New(connector)
	defer p.Close()

	lease, _ := p.Acquire(context.Background(), "key1")

	// Multiple releases should be safe
	lease.Release()
	lease.Release()
	lease.Release()

	// Usage should be 0
	usage := p.Usage()
	if usage["key1"] != 0 {
		t.Errorf("expected usage 0, got %d", usage["key1"])
	}
}

func TestLease_Close(t *testing.T) {
	connector := ConnectorFunc[string, *mockCloser](func(ctx context.Context, key string) (*mockCloser, error) {
		return &mockCloser{id: key}, nil
	})

	p := New(connector)
	defer p.Close()

	lease, _ := p.Acquire(context.Background(), "key1")

	// Close should work like Release
	err := lease.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !lease.Released() {
		t.Error("expected lease to be released")
	}
}

// Test with integer keys
func TestPool_IntegerKey(t *testing.T) {
	connector := ConnectorFunc[int, *mockCloser](func(ctx context.Context, key int) (*mockCloser, error) {
		return &mockCloser{id: string(rune(key))}, nil
	})

	p := New(connector)
	defer p.Close()

	lease, err := p.Acquire(context.Background(), 42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer lease.Release()

	usage := p.Usage()
	if usage[42] != 1 {
		t.Errorf("expected usage 1, got %d", usage[42])
	}
}

// Test with struct keys
type connectionKey struct {
	host string
	port int
}

func TestPool_StructKey(t *testing.T) {
	connector := ConnectorFunc[connectionKey, *mockCloser](func(ctx context.Context, key connectionKey) (*mockCloser, error) {
		return &mockCloser{id: key.host}, nil
	})

	p := New(connector)
	defer p.Close()

	key := connectionKey{host: "localhost", port: 5432}
	lease, err := p.Acquire(context.Background(), key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer lease.Release()

	usage := p.Usage()
	if usage[key] != 1 {
		t.Errorf("expected usage 1, got %d", usage[key])
	}
}
