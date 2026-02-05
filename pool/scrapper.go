package pool

import (
	"context"
	"sync"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
)

// ScrapperPool is a specialized pool for scrapper.Scrapper connections.
type ScrapperPool[K comparable] struct {
	pool *Pool[K, scrapper.Scrapper]
}

// NewScrapperPool creates a new pool for scrapper.Scrapper connections.
func NewScrapperPool[K comparable](connector Connector[K, scrapper.Scrapper], opts ...Option[K, scrapper.Scrapper]) *ScrapperPool[K] {
	return &ScrapperPool[K]{
		pool: New(connector, opts...),
	}
}

// Acquire obtains a scrapper from the pool, returning a wrapper that implements
// scrapper.Scrapper. When Close() is called on the wrapper, the connection is
// released back to the pool rather than being closed.
func (p *ScrapperPool[K]) Acquire(ctx context.Context, key K) (scrapper.Scrapper, error) {
	lease, err := p.pool.Acquire(ctx, key)
	if err != nil {
		return nil, err
	}
	return &scrapperWrapper[K]{
		lease: lease,
	}, nil
}

// GC triggers a garbage collection cycle.
func (p *ScrapperPool[K]) GC() {
	p.pool.GC()
}

// Usage returns the current usage count for each connection.
func (p *ScrapperPool[K]) Usage() map[K]int {
	return p.pool.Usage()
}

// Close closes the pool and all connections.
func (p *ScrapperPool[K]) Close() error {
	return p.pool.Close()
}

// Pool returns the underlying generic pool.
func (p *ScrapperPool[K]) Pool() *Pool[K, scrapper.Scrapper] {
	return p.pool
}

var _ scrapper.Scrapper = &scrapperWrapper[string]{}

// scrapperWrapper wraps a lease to implement scrapper.Scrapper.
// When Close() is called, it releases the lease back to the pool.
type scrapperWrapper[K comparable] struct {
	lease *Lease[K, scrapper.Scrapper]
	mu    sync.Mutex
}

func (s *scrapperWrapper[K]) DialectType() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease == nil || s.lease.Released() {
		return ""
	}
	return s.lease.Value().DialectType()
}

func (s *scrapperWrapper[K]) SqlDialect() sqldialect.Dialect {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease == nil || s.lease.Released() {
		return nil
	}
	return s.lease.Value().SqlDialect()
}

func (s *scrapperWrapper[K]) IsPermissionError(err error) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease == nil || s.lease.Released() {
		return false
	}
	return s.lease.Value().IsPermissionError(err)
}

func (s *scrapperWrapper[K]) ValidateConfiguration(ctx context.Context) (warnings []string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease == nil || s.lease.Released() {
		return nil, nil
	}
	return s.lease.Value().ValidateConfiguration(ctx)
}

func (s *scrapperWrapper[K]) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease == nil || s.lease.Released() {
		return nil, nil
	}
	return s.lease.Value().QueryCatalog(ctx)
}

func (s *scrapperWrapper[K]) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease == nil || s.lease.Released() {
		return nil, nil
	}
	return s.lease.Value().QueryTableMetrics(ctx, lastMetricsFetchTime)
}

func (s *scrapperWrapper[K]) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease == nil || s.lease.Released() {
		return nil, nil
	}
	return s.lease.Value().QuerySqlDefinitions(ctx)
}

func (s *scrapperWrapper[K]) QueryTables(ctx context.Context) ([]*scrapper.TableRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease == nil || s.lease.Released() {
		return nil, nil
	}
	return s.lease.Value().QueryTables(ctx)
}

func (s *scrapperWrapper[K]) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease == nil || s.lease.Released() {
		return nil, nil
	}
	return s.lease.Value().QueryDatabases(ctx)
}

func (s *scrapperWrapper[K]) QuerySegments(ctx context.Context, sql string, args ...any) ([]*scrapper.SegmentRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease == nil || s.lease.Released() {
		return nil, nil
	}
	return s.lease.Value().QuerySegments(ctx, sql, args...)
}

func (s *scrapperWrapper[K]) QueryCustomMetrics(ctx context.Context, sql string, args ...any) ([]*scrapper.CustomMetricsRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease == nil || s.lease.Released() {
		return nil, nil
	}
	return s.lease.Value().QueryCustomMetrics(ctx, sql, args...)
}

func (s *scrapperWrapper[K]) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lease == nil {
		return nil
	}

	s.lease.Release()
	s.lease = nil
	return nil
}
