package tablestats

import (
	"context"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/querybuilder"
	. "github.com/getsynq/dwhsupport/sqldialect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubQuerier is a test double for TableStatsQuerier.
type stubQuerier struct {
	rows []exec.QueryMapResult
	err  error
}

func (s *stubQuerier) QueryMaps(_ context.Context, _ string) ([]exec.QueryMapResult, error) {
	return s.rows, s.err
}

// captureQuerier records the SQL that was passed to QueryMaps.
type captureQuerier struct {
	capturedSQL string
	rows        []exec.QueryMapResult
}

func (c *captureQuerier) QueryMaps(_ context.Context, sql string) ([]exec.QueryMapResult, error) {
	c.capturedSQL = sql
	return c.rows, nil
}

func newTestFetcher(project, schema, table string) *TableStatsQuerier {
	fqn := TableFqn(project, schema, table)
	return NewTableStatsQuerier(querybuilder.NewMetaQueryBuilder(fqn))
}

// ---------------------------------------------------------------------------
// toInt64FromInterface
// ---------------------------------------------------------------------------

func TestToInt64FromInterface(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		n, err := toInt64FromInterface(int64(42))
		require.NoError(t, err)
		assert.Equal(t, int64(42), n)
	})
	t.Run("int32", func(t *testing.T) {
		n, err := toInt64FromInterface(int32(7))
		require.NoError(t, err)
		assert.Equal(t, int64(7), n)
	})
	t.Run("int", func(t *testing.T) {
		n, err := toInt64FromInterface(int(99))
		require.NoError(t, err)
		assert.Equal(t, int64(99), n)
	})
	t.Run("float64 rounds", func(t *testing.T) {
		n, err := toInt64FromInterface(float64(3.7))
		require.NoError(t, err)
		assert.Equal(t, int64(4), n)
	})
	t.Run("float32 rounds", func(t *testing.T) {
		n, err := toInt64FromInterface(float32(2.5))
		require.NoError(t, err)
		assert.Equal(t, int64(3), n)
	})
	t.Run("unsupported type", func(t *testing.T) {
		_, err := toInt64FromInterface("not a number")
		require.Error(t, err)
	})
}

// ---------------------------------------------------------------------------
// TableStatsFetcher.Fetch — Trino path
// ---------------------------------------------------------------------------

func TestFetchTrino_SummaryRowExtracted(t *testing.T) {
	n := float64(1000)
	ds := float64(512)
	querier := &stubQuerier{rows: []exec.QueryMapResult{
		{"column_name": "col_a", "data_size": ds},
		{"column_name": nil, "row_count": n, "data_size": nil},
	}}

	result, err := newTestFetcher("proj", "sch", "tbl").Fetch(context.Background(), NewTrinoDialect(), querier)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.NumRows)
	assert.Equal(t, int64(1000), *result.NumRows)
	require.NotNil(t, result.SizeBytes)
	assert.Equal(t, int64(512), *result.SizeBytes)
	assert.Nil(t, result.LastLoadedAt)
}

func TestFetchTrino_DataSizeSummedAcrossColumns(t *testing.T) {
	querier := &stubQuerier{rows: []exec.QueryMapResult{
		{"column_name": "a", "data_size": float64(100)},
		{"column_name": "b", "data_size": float64(200)},
		{"column_name": nil, "row_count": float64(50), "data_size": nil},
	}}

	result, err := newTestFetcher("p", "s", "t").Fetch(context.Background(), NewTrinoDialect(), querier)
	require.NoError(t, err)
	require.NotNil(t, result.SizeBytes)
	assert.Equal(t, int64(300), *result.SizeBytes)
}

func TestFetchTrino_EmptyResult(t *testing.T) {
	querier := &stubQuerier{rows: nil}
	result, err := newTestFetcher("p", "s", "t").Fetch(context.Background(), NewTrinoDialect(), querier)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestFetchTrino_NoDataSize(t *testing.T) {
	querier := &stubQuerier{rows: []exec.QueryMapResult{
		{"column_name": nil, "row_count": float64(42)},
	}}
	result, err := newTestFetcher("p", "s", "t").Fetch(context.Background(), NewTrinoDialect(), querier)
	require.NoError(t, err)
	require.NotNil(t, result.NumRows)
	assert.Equal(t, int64(42), *result.NumRows)
	assert.Nil(t, result.SizeBytes)
}

func TestFetchTrino_SQLUsesQuotedIdentifiers(t *testing.T) {
	q := &captureQuerier{rows: []exec.QueryMapResult{}}
	fetcher := newTestFetcher("my_project", "my_schema", "my_table")
	_, _ = fetcher.Fetch(context.Background(), NewTrinoDialect(), q)
	assert.Equal(t, `SHOW STATS FOR "my_project"."my_schema"."my_table"`, q.capturedSQL)
}

// ---------------------------------------------------------------------------
// TableStatsFetcher.Fetch — generic path (Postgres)
// ---------------------------------------------------------------------------

func TestFetchGeneric_ParsesNumRows(t *testing.T) {
	querier := &stubQuerier{rows: []exec.QueryMapResult{
		{"num_rows": int64(999)},
	}}
	result, err := newTestFetcher("", "my_schema", "my_table").Fetch(context.Background(), NewPostgresDialect(), querier)
	require.NoError(t, err)
	require.NotNil(t, result.NumRows)
	assert.Equal(t, int64(999), *result.NumRows)
}

func TestFetchGeneric_ParsesLastLoadedAt(t *testing.T) {
	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	querier := &stubQuerier{rows: []exec.QueryMapResult{
		{"num_rows": int64(5), "last_loaded_at": ts},
	}}
	result, err := newTestFetcher("", "sch", "tbl").Fetch(context.Background(), NewPostgresDialect(), querier)
	require.NoError(t, err)
	require.NotNil(t, result.LastLoadedAt)
	assert.Equal(t, ts, *result.LastLoadedAt)
}

func TestFetchGeneric_EmptyResult(t *testing.T) {
	querier := &stubQuerier{rows: nil}
	result, err := newTestFetcher("", "sch", "tbl").Fetch(context.Background(), NewPostgresDialect(), querier)
	require.NoError(t, err)
	assert.Nil(t, result)
}
