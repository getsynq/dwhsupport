package stdsql

import (
	"context"
	"fmt"
	"strconv"

	"github.com/getsynq/dwhsupport/exec/querystats"
)

// ExplainRows runs an EXPLAIN-style statement and returns the result column
// names plus every row as a slice of raw driver values. It is the shared
// plumbing behind the per-dialect EstimateQuery implementations that read an
// EXPLAIN result set.
//
// The caller is responsible for passing a statement that only plans the query
// (EXPLAIN / EXPLAIN ESTIMATE / EXPLAIN USING JSON) and never one that executes
// it (no EXPLAIN ANALYZE) — EstimateQuery must not run the user's query.
func ExplainRows(ctx context.Context, db RowQuerier, sql string) (columns []string, rows [][]any, err error) {
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()

	sqlRows, err := db.QueryRows(ctx, sql)
	if err != nil {
		return nil, nil, err
	}
	defer sqlRows.Close()

	columns, err = sqlRows.Columns()
	if err != nil {
		return nil, nil, err
	}

	for sqlRows.Next() {
		cells, err := sqlRows.SliceScan()
		if err != nil {
			return nil, nil, err
		}
		rows = append(rows, cells)
	}
	if err := sqlRows.Err(); err != nil {
		return nil, nil, err
	}
	return columns, rows, nil
}

// CellString renders a raw driver cell (string, []byte, fmt.Stringer, ...) as a
// string. Used to pull JSON text out of a single EXPLAIN result cell.
func CellString(v any) (string, bool) {
	switch val := v.(type) {
	case nil:
		return "", false
	case string:
		return val, true
	case []byte:
		return string(val), true
	case fmt.Stringer:
		return val.String(), true
	default:
		return fmt.Sprint(v), true
	}
}

// CellInt64 coerces a raw driver cell to int64, handling the signed/unsigned/
// float/text shapes different drivers use for EXPLAIN numeric columns.
func CellInt64(v any) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int32:
		return int64(val), true
	case int:
		return int64(val), true
	case uint64:
		return int64(val), true
	case uint32:
		return int64(val), true
	case uint:
		return int64(val), true
	case float64:
		return int64(val), true
	case float32:
		return int64(val), true
	case []byte:
		return parseInt64(string(val))
	case string:
		return parseInt64(val)
	default:
		return 0, false
	}
}

func parseInt64(s string) (int64, bool) {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, true
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(f), true
	}
	return 0, false
}
