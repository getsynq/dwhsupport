package pool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
)

// mockScrapper implements scrapper.Scrapper for testing
type mockScrapper struct {
	dialectType string
	closed      atomic.Bool
}

func (m *mockScrapper) DialectType() string {
	return m.dialectType
}

func (m *mockScrapper) SqlDialect() sqldialect.Dialect {
	return nil
}

func (m *mockScrapper) IsPermissionError(err error) bool {
	return false
}

func (m *mockScrapper) ValidateConfiguration(ctx context.Context) (warnings []string, err error) {
	return nil, nil
}

func (m *mockScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	return nil, nil
}

func (m *mockScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	return nil, nil
}

func (m *mockScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	return nil, nil
}

func (m *mockScrapper) QueryTables(ctx context.Context) ([]*scrapper.TableRow, error) {
	return nil, nil
}

func (m *mockScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	return nil, nil
}

func (m *mockScrapper) QuerySegments(ctx context.Context, sql string, args ...any) ([]*scrapper.SegmentRow, error) {
	return nil, nil
}

func (m *mockScrapper) QueryCustomMetrics(ctx context.Context, sql string, args ...any) ([]*scrapper.CustomMetricsRow, error) {
	return nil, nil
}

func (m *mockScrapper) Close() error {
	m.closed.Store(true)
	return nil
}

func (m *mockScrapper) IsClosed() bool {
	return m.closed.Load()
}

var _ scrapper.Scrapper = &mockScrapper{}

func TestScrapperPool_Acquire(t *testing.T) {
	connectCalls := atomic.Int32{}
	connector := ConnectorFunc[string, scrapper.Scrapper](func(ctx context.Context, key string) (scrapper.Scrapper, error) {
		connectCalls.Add(1)
		return &mockScrapper{dialectType: key}, nil
	})

	pool := NewScrapperPool(connector)
	defer pool.Close()

	scr, err := pool.Acquire(context.Background(), "snowflake")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check that the wrapper implements scrapper.Scrapper
	if scr.DialectType() != "snowflake" {
		t.Errorf("expected dialect 'snowflake', got %q", scr.DialectType())
	}

	// Close the wrapper (should release back to pool)
	err = scr.Close()
	if err != nil {
		t.Fatalf("unexpected error on close: %v", err)
	}

	// Usage should be 0
	usage := pool.Usage()
	if usage["snowflake"] != 0 {
		t.Errorf("expected usage 0, got %d", usage["snowflake"])
	}

	// Should only have connected once
	if connectCalls.Load() != 1 {
		t.Errorf("expected 1 connect call, got %d", connectCalls.Load())
	}
}

func TestScrapperPool_Reuse(t *testing.T) {
	connectCalls := atomic.Int32{}
	connector := ConnectorFunc[string, scrapper.Scrapper](func(ctx context.Context, key string) (scrapper.Scrapper, error) {
		connectCalls.Add(1)
		return &mockScrapper{dialectType: key}, nil
	})

	pool := NewScrapperPool(connector)
	defer pool.Close()

	// First acquire
	scr1, err := pool.Acquire(context.Background(), "bigquery")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Second acquire
	scr2, err := pool.Acquire(context.Background(), "bigquery")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Both should work
	if scr1.DialectType() != "bigquery" {
		t.Errorf("scr1: expected dialect 'bigquery', got %q", scr1.DialectType())
	}
	if scr2.DialectType() != "bigquery" {
		t.Errorf("scr2: expected dialect 'bigquery', got %q", scr2.DialectType())
	}

	// Should only connect once
	if connectCalls.Load() != 1 {
		t.Errorf("expected 1 connect call, got %d", connectCalls.Load())
	}

	// Usage should be 2
	usage := pool.Usage()
	if usage["bigquery"] != 2 {
		t.Errorf("expected usage 2, got %d", usage["bigquery"])
	}

	scr1.Close()
	scr2.Close()
}

func TestScrapperPool_CloseReleasesToPool(t *testing.T) {
	var underlyingScrapper *mockScrapper
	connector := ConnectorFunc[string, scrapper.Scrapper](func(ctx context.Context, key string) (scrapper.Scrapper, error) {
		underlyingScrapper = &mockScrapper{dialectType: key}
		return underlyingScrapper, nil
	})

	pool := NewScrapperPool(connector)
	defer pool.Close()

	scr, _ := pool.Acquire(context.Background(), "postgres")
	scr.Close()

	// Underlying scrapper should NOT be closed (it's back in the pool)
	if underlyingScrapper.IsClosed() {
		t.Error("underlying scrapper should not be closed when wrapper is closed")
	}

	// Should still be in pool
	usage := pool.Usage()
	if _, ok := usage["postgres"]; !ok {
		t.Error("connection should still be in pool")
	}
}

func TestScrapperPool_PoolCloseClosesConnections(t *testing.T) {
	var underlyingScrapper *mockScrapper
	connector := ConnectorFunc[string, scrapper.Scrapper](func(ctx context.Context, key string) (scrapper.Scrapper, error) {
		underlyingScrapper = &mockScrapper{dialectType: key}
		return underlyingScrapper, nil
	})

	pool := NewScrapperPool(connector)

	scr, _ := pool.Acquire(context.Background(), "redshift")
	scr.Close() // Release back to pool

	// Close the pool
	pool.Close()

	// Underlying scrapper should now be closed
	if !underlyingScrapper.IsClosed() {
		t.Error("underlying scrapper should be closed when pool is closed")
	}
}

func TestScrapperPool_WrapperSafeAfterRelease(t *testing.T) {
	connector := ConnectorFunc[string, scrapper.Scrapper](func(ctx context.Context, key string) (scrapper.Scrapper, error) {
		return &mockScrapper{dialectType: key}, nil
	})

	pool := NewScrapperPool(connector)
	defer pool.Close()

	scr, _ := pool.Acquire(context.Background(), "clickhouse")
	scr.Close()

	// Calling methods after close should be safe (return zero values)
	if scr.DialectType() != "" {
		t.Errorf("expected empty dialect after release, got %q", scr.DialectType())
	}

	// Multiple closes should be safe
	scr.Close()
	scr.Close()
}

func TestScrapperPool_GC(t *testing.T) {
	var underlyingScrapper *mockScrapper
	connector := ConnectorFunc[string, scrapper.Scrapper](func(ctx context.Context, key string) (scrapper.Scrapper, error) {
		underlyingScrapper = &mockScrapper{dialectType: key}
		return underlyingScrapper, nil
	})

	pool := NewScrapperPool(connector,
		WithCleanerInterval[string, scrapper.Scrapper](10*time.Millisecond),
		WithMaxIdleDuration[string, scrapper.Scrapper](30*time.Millisecond),
	)
	defer pool.Close()

	scr, _ := pool.Acquire(context.Background(), "duckdb")
	scr.Close()

	// Wait for GC
	time.Sleep(100 * time.Millisecond)

	// Should be cleaned up
	if !underlyingScrapper.IsClosed() {
		t.Error("expected idle connection to be closed by GC")
	}
}

func TestScrapperPool_AccessUnderlyingPool(t *testing.T) {
	connector := ConnectorFunc[string, scrapper.Scrapper](func(ctx context.Context, key string) (scrapper.Scrapper, error) {
		return &mockScrapper{dialectType: key}, nil
	})

	pool := NewScrapperPool(connector)
	defer pool.Close()

	// Access underlying pool for advanced usage
	underlyingPool := pool.Pool()
	if underlyingPool == nil {
		t.Error("expected non-nil underlying pool")
	}
}
