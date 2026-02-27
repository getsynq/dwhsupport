package tablestats

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/querybuilder"
	"github.com/getsynq/dwhsupport/sqldialect"
)

// MapQuerier is the minimal interface required by TableStatsQuerier.
// It is satisfied by querier.Querier[T].
type MapQuerier interface {
	QueryMaps(ctx context.Context, sql string) ([]exec.QueryMapResult, error)
}

// TableStatsResult holds the result of a table stats fetch.
type TableStatsResult struct {
	NumRows      *int64
	SizeBytes    *int64
	LastLoadedAt *time.Time
}

// TableStatsQuerier executes table-stats queries built by a MetaQueryBuilder.
type TableStatsQuerier struct {
	builder *querybuilder.MetaQueryBuilder
}

// NewTableStatsQuerier returns a querier that uses builder to generate SQL.
func NewTableStatsQuerier(builder *querybuilder.MetaQueryBuilder) *TableStatsQuerier {
	return &TableStatsQuerier{builder: builder}
}

// Fetch executes the appropriate query for the given dialect and returns the
// aggregated table stats. Returns nil when the table is not found.
func (f *TableStatsQuerier) Fetch(
	ctx context.Context,
	dialect sqldialect.Dialect,
	querier MapQuerier,
) (*TableStatsResult, error) {
	sql, err := f.builder.ToSql(dialect)
	if err != nil {
		return nil, err
	}

	rows, err := querier.QueryMaps(ctx, sql)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}

	switch dialect.(type) {
	case *sqldialect.TrinoDialect:
		return aggregateTrinoShowStats(rows)
	default:
		return mapToTableStatsResult(rows[0])
	}
}

// aggregateTrinoShowStats reduces the multi-row SHOW STATS output into a single
// TableStatsResult. The summary row (column_name IS NULL) carries row_count;
// per-column rows carry data_size which is summed.
func aggregateTrinoShowStats(rows []exec.QueryMapResult) (*TableStatsResult, error) {
	var numRows *int64
	totalDataSize := int64(0)
	hasDataSize := false

	for _, row := range rows {
		// The summary row has NULL column_name and carries the table-level row_count.
		if row["column_name"] == nil {
			if v, ok := row["row_count"]; ok && v != nil {
				n, err := toInt64FromInterface(v)
				if err != nil {
					return nil, fmt.Errorf("row_count: %w", err)
				}
				numRows = &n
			}
		}

		// Sum data_size across all per-column rows.
		if v, ok := row["data_size"]; ok && v != nil {
			n, err := toInt64FromInterface(v)
			if err != nil {
				return nil, fmt.Errorf("data_size: %w", err)
			}
			totalDataSize += n
			hasDataSize = true
		}
	}

	result := &TableStatsResult{NumRows: numRows}
	if hasDataSize {
		result.SizeBytes = &totalDataSize
	}
	return result, nil
}

func mapToTableStatsResult(row exec.QueryMapResult) (*TableStatsResult, error) {
	result := &TableStatsResult{}

	if v, ok := row["num_rows"]; ok && v != nil {
		n, err := toInt64FromInterface(v)
		if err != nil {
			return nil, fmt.Errorf("num_rows: %w", err)
		}
		result.NumRows = &n
	}

	if v, ok := row["size_bytes"]; ok && v != nil {
		n, err := toInt64FromInterface(v)
		if err != nil {
			return nil, fmt.Errorf("size_bytes: %w", err)
		}
		result.SizeBytes = &n
	}

	if v, ok := row["last_loaded_at"]; ok && v != nil {
		t, err := toTimeFromInterface(v)
		if err != nil {
			return nil, fmt.Errorf("last_loaded_at: %w", err)
		}
		result.LastLoadedAt = &t
	}

	return result, nil
}

func toInt64FromInterface(v any) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case int32:
		return int64(t), nil
	case int:
		return int64(t), nil
	case float32:
		return int64(math.Round(float64(t))), nil
	case float64:
		return int64(math.Round(t)), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toTimeFromInterface(v any) (time.Time, error) {
	switch t := v.(type) {
	case time.Time:
		return t, nil
	case *time.Time:
		if t == nil {
			return time.Time{}, fmt.Errorf("nil time pointer")
		}
		return *t, nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", v)
	}
}
