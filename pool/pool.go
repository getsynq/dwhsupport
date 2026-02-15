// Package pool provides a generic connection pooling implementation.
//
// The pool manages reusable connections of any type, handling concurrent
// connection requests, usage tracking, and automatic cleanup of idle connections.
//
// Example usage:
//
//	connector := pool.ConnectorFunc[string, *sql.DB](func(ctx context.Context, dsn string) (*sql.DB, error) {
//		return sql.Open("postgres", dsn)
//	})
//	p := pool.New(connector)
//	defer p.Close()
//
//	lease, err := p.Acquire(ctx, "postgres://localhost/mydb")
//	if err != nil {
//		return err
//	}
//	defer lease.Release()
//
//	db := lease.Value()
//	// use db...
package pool

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Closer is the interface for resources that can be closed.
type Closer interface {
	Close() error
}

// Connector creates connections of type V keyed by K.
type Connector[K comparable, V Closer] interface {
	Connect(ctx context.Context, key K) (V, error)
}

// ConnectorFunc is a function adapter for Connector.
type ConnectorFunc[K comparable, V Closer] func(ctx context.Context, key K) (V, error)

func (f ConnectorFunc[K, V]) Connect(ctx context.Context, key K) (V, error) {
	return f(ctx, key)
}

// Hooks provides callbacks for pool lifecycle events.
// All callbacks are optional.
type Hooks[K comparable, V Closer] struct {
	// OnConnecting is called when a new connection attempt starts.
	OnConnecting func(ctx context.Context, key K)
	// OnConnected is called when a new connection is established.
	OnConnected func(ctx context.Context, key K, value V)
	// OnConnectError is called when a connection attempt fails.
	OnConnectError func(ctx context.Context, key K, err error)
	// OnAcquire is called when an existing connection is reused.
	OnAcquire func(ctx context.Context, key K, value V)
	// OnWaiting is called when waiting for an in-flight connection.
	OnWaiting func(ctx context.Context, key K)
	// OnRelease is called when a connection is released back to the pool.
	OnRelease func(ctx context.Context, key K, value V)
	// OnEvict is called when a connection is evicted from the pool.
	OnEvict func(ctx context.Context, key K, value V)
	// FormatKey formats a key for logging. If nil, fmt.Sprintf("%v", key) is used.
	FormatKey func(key K) string
}

// Option configures a Pool.
type Option[K comparable, V Closer] func(*Pool[K, V])

// WithCleanerInterval sets the interval between cleanup cycles.
// Default: 30 seconds.
func WithCleanerInterval[K comparable, V Closer](d time.Duration) Option[K, V] {
	return func(p *Pool[K, V]) {
		p.cleanerInterval = d
	}
}

// WithMaxIdleDuration sets the maximum time a connection can be idle before eviction.
// Default: 5 minutes.
func WithMaxIdleDuration[K comparable, V Closer](d time.Duration) Option[K, V] {
	return func(p *Pool[K, V]) {
		p.maxIdleDuration = d
	}
}

// WithHooks sets lifecycle hooks for the pool.
func WithHooks[K comparable, V Closer](hooks Hooks[K, V]) Option[K, V] {
	return func(p *Pool[K, V]) {
		p.hooks = hooks
	}
}

// Pool manages a pool of reusable connections.
type Pool[K comparable, V Closer] struct {
	mu        sync.Mutex
	connector Connector[K, V]
	hooks     Hooks[K, V]

	connecting map[K]*connecting[V]
	connected  map[K]*connected[V]

	cleanerInterval  time.Duration
	maxIdleDuration  time.Duration
	closingCh        chan struct{}
	cleanerCh        chan struct{}
	startCleanerOnce sync.Once
	closed           bool
}

type connecting[V any] struct {
	res   V
	err   error
	done  chan struct{} // closed when connection is ready
	mu    sync.Mutex
	usage int
}

func (c *connecting[V]) addWaiter() {
	c.mu.Lock()
	c.usage++
	c.mu.Unlock()
}

func (c *connecting[V]) wait() {
	<-c.done
}

func (c *connecting[V]) signal() {
	close(c.done)
}

type connected[V any] struct {
	value     V
	usage     int
	idleSince *time.Time
	openedAt  time.Time
}

func (c *connected[V]) increaseUsage() {
	c.usage++
	if c.usage > 0 {
		c.idleSince = nil
	}
}

func (c *connected[V]) decreaseUsage() {
	c.usage--
	if c.usage == 0 {
		now := time.Now()
		c.idleSince = &now
	}
}

// New creates a new Pool with the given connector and options.
func New[K comparable, V Closer](connector Connector[K, V], opts ...Option[K, V]) *Pool[K, V] {
	p := &Pool[K, V]{
		connector:       connector,
		connecting:      make(map[K]*connecting[V]),
		connected:       make(map[K]*connected[V]),
		cleanerCh:       make(chan struct{}, 1),
		closingCh:       make(chan struct{}),
		cleanerInterval: 30 * time.Second,
		maxIdleDuration: 5 * time.Minute,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Acquire obtains a connection from the pool.
// If an existing connection is available, it is reused.
// If a connection is being established, this call waits for it.
// Otherwise, a new connection is created.
//
// The returned Lease must be released when done, either by calling Release()
// or Close(). Failing to release the lease will prevent the connection from
// being reused or cleaned up.
func (p *Pool[K, V]) Acquire(ctx context.Context, key K) (*Lease[K, V], error) {
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool is closed")
	}

	// Check for existing connected connection
	if c, ok := p.connected[key]; ok {
		if p.hooks.OnAcquire != nil {
			p.hooks.OnAcquire(ctx, key, c.value)
		}
		c.increaseUsage()
		lease := p.newLease(key, c.value)
		p.mu.Unlock()
		return lease, nil
	}

	// Check for in-flight connection
	if conn, ok := p.connecting[key]; ok {
		if p.hooks.OnWaiting != nil {
			p.hooks.OnWaiting(ctx, key)
		}
		conn.addWaiter()
		p.mu.Unlock()
		conn.wait()

		if conn.err != nil {
			return nil, conn.err
		}
		return p.newLease(key, conn.res), nil
	}

	// Start new connection
	if p.hooks.OnConnecting != nil {
		p.hooks.OnConnecting(ctx, key)
	}
	conn := &connecting[V]{
		done: make(chan struct{}),
	}
	conn.addWaiter()
	p.connecting[key] = conn

	p.mu.Unlock()

	go func() {
		p.mu.Lock()
		connector := p.connector
		p.mu.Unlock()

		value, err := connector.Connect(ctx, key)

		p.mu.Lock()
		defer p.mu.Unlock()

		delete(p.connecting, key)

		if err != nil {
			if p.hooks.OnConnectError != nil {
				p.hooks.OnConnectError(ctx, key, err)
			}
			conn.err = err
			conn.signal()
			return
		}

		// Pool was closed while connecting
		if p.closed {
			value.Close()
			conn.err = fmt.Errorf("pool is closing")
			conn.signal()
			return
		}

		if p.hooks.OnConnected != nil {
			p.hooks.OnConnected(ctx, key, value)
		}

		p.startCleanerOnce.Do(p.startCleaner)

		conn.mu.Lock()
		usage := conn.usage
		conn.mu.Unlock()

		p.connected[key] = &connected[V]{
			value:    value,
			usage:    usage,
			openedAt: time.Now(),
		}
		conn.res = value
		conn.signal()
	}()

	conn.wait()

	if conn.err != nil {
		return nil, conn.err
	}

	return p.newLease(key, conn.res), nil
}

// GC triggers a garbage collection cycle to clean up idle connections.
func (p *Pool[K, V]) GC() {
	select {
	case p.cleanerCh <- struct{}{}:
	default:
	}
}

// Usage returns the current usage count for each connection in the pool.
func (p *Pool[K, V]) Usage() map[K]int {
	p.mu.Lock()
	defer p.mu.Unlock()

	usage := make(map[K]int)
	for key, conn := range p.connected {
		usage[key] = conn.usage
	}
	return usage
}

// Close closes the pool and all connections.
// After Close is called, Acquire will return an error.
func (p *Pool[K, V]) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	close(p.closingCh)

	for key, conn := range p.connected {
		delete(p.connected, key)
		conn.usage = 0
		conn.value.Close()
	}

	return nil
}

func (p *Pool[K, V]) release(key K) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.connected[key]; ok {
		c.decreaseUsage()
	}
}

func (p *Pool[K, V]) newLease(key K, value V) *Lease[K, V] {
	return &Lease[K, V]{
		key:   key,
		value: value,
		pool:  p,
	}
}

func (p *Pool[K, V]) startCleaner() {
	go func() {
		timer := time.NewTimer(p.cleanerInterval)
		defer timer.Stop()

		for {
			select {
			case <-timer.C:
				select {
				case p.cleanerCh <- struct{}{}:
				default:
				}
			case <-p.cleanerCh:
				p.runCleanup()
				timer.Reset(p.cleanerInterval)
			case <-p.closingCh:
				timer.Stop()
				return
			}
		}
	}()
}

func (p *Pool[K, V]) runCleanup() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	for key, conn := range p.connected {
		if conn.idleSince == nil {
			continue
		}
		idleDuration := now.Sub(*conn.idleSince)
		if idleDuration >= p.maxIdleDuration {
			if p.hooks.OnEvict != nil {
				p.hooks.OnEvict(context.Background(), key, conn.value)
			}
			conn.value.Close()
			delete(p.connected, key)
		}
	}
}
