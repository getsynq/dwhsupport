package pool

import "sync"

// Lease represents a borrowed connection from the pool.
// It must be released when done by calling Release() or Close().
type Lease[K comparable, V Closer] struct {
	key      K
	value    V
	pool     *Pool[K, V]
	mu       sync.Mutex
	released bool
}

// Value returns the underlying connection.
// Returns the zero value if the lease has been released.
func (l *Lease[K, V]) Value() V {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.value
}

// Release returns the connection to the pool.
// It is safe to call Release multiple times.
func (l *Lease[K, V]) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.released {
		return
	}
	l.released = true

	if l.pool != nil {
		l.pool.release(l.key)
		l.pool = nil
	}

	var zero V
	l.value = zero
}

// Close is an alias for Release.
// It satisfies the Closer interface, allowing Lease to be used with defer.
func (l *Lease[K, V]) Close() error {
	l.Release()
	return nil
}

// Released returns true if the lease has been released.
func (l *Lease[K, V]) Released() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.released
}
