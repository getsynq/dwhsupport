// Package pool provides a generic connection pooling implementation.
//
// The pool manages reusable connections of any type, handling concurrent
// connection requests, usage tracking, and automatic cleanup of idle connections.
//
// # Basic Usage
//
// The pool is generic over two type parameters:
//   - K: the key type (must be comparable for use as map key)
//   - V: the value type (must implement Close() error)
//
// Example with a simple closer:
//
//	type MyConn struct { /* ... */ }
//	func (c *MyConn) Close() error { return nil }
//
//	connector := pool.ConnectorFunc[string, *MyConn](func(ctx context.Context, key string) (*MyConn, error) {
//	    return &MyConn{}, nil
//	})
//
//	p := pool.New(connector)
//	defer p.Close()
//
//	lease, err := p.Acquire(ctx, "connection-key")
//	if err != nil {
//	    return err
//	}
//	defer lease.Release()
//
//	conn := lease.Value()
//	// use conn...
//
// # Scrapper Pool
//
// For scrapper.Scrapper specifically, use ScrapperPool which returns a wrapper
// that implements scrapper.Scrapper directly:
//
//	connector := pool.ConnectorFunc[string, scrapper.Scrapper](func(ctx context.Context, connId string) (scrapper.Scrapper, error) {
//	    // Create your scrapper...
//	    return myScrapper, nil
//	})
//
//	p := pool.NewScrapperPool(connector)
//	defer p.Close()
//
//	scr, err := p.Acquire(ctx, "my-connection")
//	if err != nil {
//	    return err
//	}
//	defer scr.Close() // Returns to pool, doesn't actually close
//
//	rows, err := scr.QueryCatalog(ctx)
//
// # Custom Key Types
//
// Any comparable type can be used as a key:
//
//	type ConnectionKey struct {
//	    Host string
//	    Port int
//	    User string
//	}
//
//	connector := pool.ConnectorFunc[ConnectionKey, *sql.DB](...)
//	p := pool.New(connector)
//
//	lease, _ := p.Acquire(ctx, ConnectionKey{Host: "localhost", Port: 5432, User: "admin"})
//
// # Configuration
//
// The pool supports several options:
//
//	p := pool.New(connector,
//	    pool.WithCleanerInterval[K, V](30 * time.Second),  // How often to check for idle connections
//	    pool.WithMaxIdleDuration[K, V](5 * time.Minute),   // How long before idle connections are closed
//	    pool.WithHooks[K, V](pool.Hooks[K, V]{
//	        OnConnecting: func(ctx context.Context, key K) { /* ... */ },
//	        OnConnected:  func(ctx context.Context, key K, value V) { /* ... */ },
//	        OnRelease:    func(ctx context.Context, key K, value V) { /* ... */ },
//	        OnEvict:      func(ctx context.Context, key K, value V) { /* ... */ },
//	    }),
//	)
//
// # Connection Lifecycle
//
// 1. First Acquire(ctx, key): Creates new connection via Connector.Connect()
// 2. Subsequent Acquire(ctx, key): Reuses existing connection (increments usage)
// 3. Concurrent Acquire(ctx, key): Waits for in-flight connection
// 4. Release()/Close(): Decrements usage, marks connection as idle when usage=0
// 5. GC cycle: Closes connections idle longer than MaxIdleDuration
// 6. Pool.Close(): Closes all connections
package pool
