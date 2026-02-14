package querystats

import (
	"context"
	"encoding/json"
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

func TestQueryStats_JSON(t *testing.T) {
	stats := QueryStats{
		RowsRead:     Int64Ptr(1000),
		BytesRead:    Int64Ptr(2048),
		RowsProduced: Int64Ptr(50),
		CacheHit:     BoolPtr(true),
		Duration:     500 * time.Millisecond,
	}

	data, err := json.Marshal(stats)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded QueryStats
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if *decoded.RowsRead != 1000 {
		t.Fatalf("expected RowsRead=1000, got %d", *decoded.RowsRead)
	}
	if *decoded.BytesRead != 2048 {
		t.Fatalf("expected BytesRead=2048, got %d", *decoded.BytesRead)
	}
	if *decoded.RowsProduced != 50 {
		t.Fatalf("expected RowsProduced=50, got %d", *decoded.RowsProduced)
	}
	if !*decoded.CacheHit {
		t.Fatal("expected CacheHit=true")
	}
	if decoded.Duration != 500*time.Millisecond {
		t.Fatalf("expected Duration=500ms, got %v", decoded.Duration)
	}

	// Nil fields should be omitted
	sparse := QueryStats{Duration: time.Second}
	data, _ = json.Marshal(sparse)
	var m map[string]interface{}
	json.Unmarshal(data, &m)
	if _, ok := m["rows_read"]; ok {
		t.Fatal("expected rows_read to be omitted from JSON")
	}
	if _, ok := m["duration"]; !ok {
		t.Fatal("expected duration to be present in JSON")
	}
}

func TestQueryID_MergedFromDriverStats(t *testing.T) {
	var received QueryStats
	ctx := WithCallback(context.Background(), func(stats QueryStats) {
		received = stats
	})

	collector, ctx := Start(ctx)
	ds := GetDriverStats(ctx)
	ds.Set(QueryStats{QueryID: "job-abc-123"})
	collector.SetRowsProduced(1)
	collector.Finish()

	if received.QueryID != "job-abc-123" {
		t.Fatalf("expected QueryID=job-abc-123, got %q", received.QueryID)
	}
}

func TestSetQueryID(t *testing.T) {
	var received QueryStats
	ctx := WithCallback(context.Background(), func(stats QueryStats) {
		received = stats
	})

	collector, _ := Start(ctx)
	collector.SetQueryID("direct-id")
	collector.Finish()

	if received.QueryID != "direct-id" {
		t.Fatalf("expected QueryID=direct-id, got %q", received.QueryID)
	}

	// Safe on nil
	var nilCollector *Collector
	nilCollector.SetQueryID("noop")
}

func TestQueryStatsFetch(t *testing.T) {
	ctx := context.Background()
	if IsQueryStatsFetch(ctx) {
		t.Fatal("expected query stats fetch to be false by default")
	}
	ctx = WithQueryStatsFetch(ctx)
	if !IsQueryStatsFetch(ctx) {
		t.Fatal("expected query stats fetch to be true after WithQueryStatsFetch")
	}
}

func TestMerge_QueryID(t *testing.T) {
	s := QueryStats{QueryID: "old"}
	s.Merge(QueryStats{QueryID: "new"})
	if s.QueryID != "new" {
		t.Fatalf("expected QueryID=new, got %q", s.QueryID)
	}

	// Empty QueryID should not overwrite
	s.Merge(QueryStats{})
	if s.QueryID != "new" {
		t.Fatalf("expected QueryID=new after empty merge, got %q", s.QueryID)
	}
}

func TestQueryStats_JSON_WithQueryID(t *testing.T) {
	stats := QueryStats{
		QueryID:  "bq-job-123",
		Duration: time.Second,
	}
	data, err := json.Marshal(stats)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var decoded QueryStats
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if decoded.QueryID != "bq-job-123" {
		t.Fatalf("expected QueryID=bq-job-123, got %q", decoded.QueryID)
	}

	// Empty QueryID should be omitted
	sparse := QueryStats{Duration: time.Second}
	data, _ = json.Marshal(sparse)
	var m map[string]interface{}
	json.Unmarshal(data, &m)
	if _, ok := m["query_id"]; ok {
		t.Fatal("expected query_id to be omitted from JSON when empty")
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
