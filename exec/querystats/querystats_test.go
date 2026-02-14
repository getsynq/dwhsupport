package querystats

import (
	"context"
	"testing"
	"time"
)

func TestWithCallback_RoundTrip(t *testing.T) {
	var called bool
	cb := func(stats QueryStats) {
		called = true
	}

	ctx := WithCallback(context.Background(), cb)
	got, ok := GetCallback(ctx)
	if !ok {
		t.Fatal("expected callback to be present in context")
	}
	got(QueryStats{Duration: time.Second})
	if !called {
		t.Fatal("expected callback to be called")
	}
}

func TestGetCallback_Missing(t *testing.T) {
	_, ok := GetCallback(context.Background())
	if ok {
		t.Fatal("expected no callback in plain context")
	}
}

func TestStart_WithCallback(t *testing.T) {
	var received QueryStats
	ctx := WithCallback(context.Background(), func(stats QueryStats) {
		received = stats
	})

	collector, ctx := Start(ctx)
	if collector == nil {
		t.Fatal("expected non-nil collector")
	}

	// Verify DriverStats is attached to context
	ds := GetDriverStats(ctx)
	if ds == nil {
		t.Fatal("expected DriverStats in context")
	}

	// Simulate driver setting stats
	ds.Set(QueryStats{BytesRead: Int64Ptr(1024)})

	collector.SetRowsProduced(42)
	collector.Finish()

	if received.RowsProduced == nil || *received.RowsProduced != 42 {
		t.Fatalf("expected RowsProduced=42, got %v", received.RowsProduced)
	}
	if received.BytesRead == nil || *received.BytesRead != 1024 {
		t.Fatalf("expected BytesRead=1024, got %v", received.BytesRead)
	}
	if received.Duration == 0 {
		t.Fatal("expected non-zero duration")
	}
}

func TestStart_WithoutCallback(t *testing.T) {
	collector, ctx := Start(context.Background())
	if collector != nil {
		t.Fatal("expected nil collector without callback")
	}
	// Should not panic
	collector.Finish()
	collector.SetRowsProduced(10)

	// No DriverStats in context
	if GetDriverStats(ctx) != nil {
		t.Fatal("expected no DriverStats without callback")
	}
}

func TestMerge(t *testing.T) {
	s := QueryStats{RowsRead: Int64Ptr(100)}
	s.Merge(QueryStats{BytesRead: Int64Ptr(200), RowsRead: Int64Ptr(150)})
	if *s.RowsRead != 150 {
		t.Fatalf("expected RowsRead=150, got %d", *s.RowsRead)
	}
	if *s.BytesRead != 200 {
		t.Fatalf("expected BytesRead=200, got %d", *s.BytesRead)
	}
	if s.CacheHit != nil {
		t.Fatal("expected CacheHit to remain nil")
	}
}

func TestDriverStats_ThreadSafety(t *testing.T) {
	ds := &DriverStats{}
	done := make(chan struct{})
	go func() {
		ds.Set(QueryStats{RowsRead: Int64Ptr(100)})
		close(done)
	}()
	ds.Set(QueryStats{BytesRead: Int64Ptr(200)})
	<-done
	got := ds.Get()
	// Both should be set (order doesn't matter for non-overlapping fields)
	if got.RowsRead == nil || got.BytesRead == nil {
		t.Fatalf("expected both fields set, got RowsRead=%v BytesRead=%v", got.RowsRead, got.BytesRead)
	}
}

func TestHelpers(t *testing.T) {
	v := Int64Ptr(42)
	if *v != 42 {
		t.Fatalf("expected 42, got %d", *v)
	}

	b := BoolPtr(true)
	if !*b {
		t.Fatal("expected true")
	}
}
