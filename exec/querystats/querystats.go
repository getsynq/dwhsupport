package querystats

import (
	"context"
	"sync"
	"time"
)

// QueryStats holds execution statistics collected from the database driver.
// Fields are pointers — nil means the metric is not available for the driver.
type QueryStats struct {
	// RowsRead is the number of rows read/scanned by the query engine.
	RowsRead *int64
	// BytesRead is the number of bytes read/scanned by the query engine.
	BytesRead *int64
	// RowsProduced is the number of result rows returned to the caller.
	RowsProduced *int64
	// CacheHit indicates whether the query result was served from cache.
	CacheHit *bool
	// BytesBilled is the number of bytes billed (BigQuery).
	BytesBilled *int64
	// SlotMillis is the slot time consumed (BigQuery).
	SlotMillis *int64
	// Blocks is the number of data blocks read (ClickHouse).
	Blocks *int64
	// CompletedSplits is the number of completed splits (Trino).
	CompletedSplits *int64
	// CPUTimeMillis is the CPU time consumed (Trino).
	CPUTimeMillis *int64
	// WallTimeMillis is the wall time reported by the engine (Trino).
	WallTimeMillis *int64
	// Duration is the wall-clock time of the query as measured by the client.
	// This field is always set.
	Duration time.Duration
}

// Merge copies non-nil fields from other into s, overwriting existing values.
func (s *QueryStats) Merge(other QueryStats) {
	if other.RowsRead != nil {
		s.RowsRead = other.RowsRead
	}
	if other.BytesRead != nil {
		s.BytesRead = other.BytesRead
	}
	if other.RowsProduced != nil {
		s.RowsProduced = other.RowsProduced
	}
	if other.CacheHit != nil {
		s.CacheHit = other.CacheHit
	}
	if other.BytesBilled != nil {
		s.BytesBilled = other.BytesBilled
	}
	if other.SlotMillis != nil {
		s.SlotMillis = other.SlotMillis
	}
	if other.Blocks != nil {
		s.Blocks = other.Blocks
	}
	if other.CompletedSplits != nil {
		s.CompletedSplits = other.CompletedSplits
	}
	if other.CPUTimeMillis != nil {
		s.CPUTimeMillis = other.CPUTimeMillis
	}
	if other.WallTimeMillis != nil {
		s.WallTimeMillis = other.WallTimeMillis
	}
}

// Callback is the function type invoked with query execution statistics.
// It is called once, right before the query function returns, even on error.
type Callback func(QueryStats)

type callbackKey struct{}
type driverStatsKey struct{}

// WithCallback attaches a stats callback to the context.
// When a query is executed with this context, the callback will be invoked
// with whatever statistics the driver is able to collect.
func WithCallback(ctx context.Context, cb Callback) context.Context {
	return context.WithValue(ctx, callbackKey{}, cb)
}

// GetCallback retrieves the stats callback from the context, if any.
func GetCallback(ctx context.Context) (Callback, bool) {
	cb, ok := ctx.Value(callbackKey{}).(Callback)
	return cb, ok
}

// DriverStats is a thread-safe accumulator for driver-specific stats.
// Database executors populate this via ClickHouse callbacks, BigQuery job stats, etc.
// The Collector merges these into the final QueryStats on Finish().
type DriverStats struct {
	mu    sync.Mutex
	stats QueryStats
}

// Set replaces the accumulated driver stats with a merge.
func (d *DriverStats) Set(s QueryStats) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stats.Merge(s)
}

// Get returns a copy of the accumulated driver stats.
func (d *DriverStats) Get() QueryStats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stats
}

// WithDriverStats attaches a DriverStats accumulator to the context.
// Database executors should call GetDriverStats to retrieve and populate it.
func WithDriverStats(ctx context.Context, ds *DriverStats) context.Context {
	return context.WithValue(ctx, driverStatsKey{}, ds)
}

// GetDriverStats retrieves the DriverStats accumulator from the context, if any.
// Returns nil if no DriverStats is present.
func GetDriverStats(ctx context.Context) *DriverStats {
	ds, _ := ctx.Value(driverStatsKey{}).(*DriverStats)
	return ds
}

// GetOrCreateDriverStats retrieves an existing DriverStats from the context,
// or creates a new one if a stats callback is present but no DriverStats exists yet.
// Returns the DriverStats and a potentially updated context.
// Returns nil and the original context if no stats callback is registered.
func GetOrCreateDriverStats(ctx context.Context) (*DriverStats, context.Context) {
	if ds := GetDriverStats(ctx); ds != nil {
		return ds, ctx
	}
	// Only create if there's a callback registered
	if _, ok := GetCallback(ctx); !ok {
		return nil, ctx
	}
	ds := &DriverStats{}
	return ds, WithDriverStats(ctx, ds)
}

// Collector accumulates query statistics and invokes the callback on Finish.
// Use Start() to create a Collector; call Finish() (typically via defer) to invoke the callback.
type Collector struct {
	cb          Callback
	driverStats *DriverStats
	start       time.Time
	Stats       QueryStats
}

// Start creates a Collector if a callback is present in the context.
// Returns nil and the original context if no callback is registered.
// It reuses an existing DriverStats from context (e.g., set by a driver-specific
// enrichment layer), or creates a new one if needed.
func Start(ctx context.Context) (*Collector, context.Context) {
	cb, ok := GetCallback(ctx)
	if !ok {
		return nil, ctx
	}
	ds, ctx := GetOrCreateDriverStats(ctx)
	return &Collector{
		cb:          cb,
		driverStats: ds,
		start:       time.Now(),
	}, ctx
}

// Finish merges driver-specific stats, sets the duration, and invokes the callback.
// Safe to call on nil Collector.
func (c *Collector) Finish() {
	if c == nil {
		return
	}
	c.Stats.Duration = time.Since(c.start)
	if c.driverStats != nil {
		c.Stats.Merge(c.driverStats.Get())
	}
	c.cb(c.Stats)
}

// SetRowsProduced sets the number of rows produced.
// Safe to call on nil Collector.
func (c *Collector) SetRowsProduced(n int64) {
	if c == nil {
		return
	}
	c.Stats.RowsProduced = Int64Ptr(n)
}

// Int64Ptr returns a pointer to the given int64 value.
func Int64Ptr(v int64) *int64 {
	return &v
}

// BoolPtr returns a pointer to the given bool value.
func BoolPtr(v bool) *bool {
	return &v
}
