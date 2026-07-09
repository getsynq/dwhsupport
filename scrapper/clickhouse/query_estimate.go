package clickhouse

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
	scrapperstdsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
	"github.com/pkg/errors"
)

// EstimateQuery returns a row estimate via `EXPLAIN ESTIMATE`, which reports the
// parts/rows/marks the planner expects to read per table without executing the
// query. Bytes are not available from this plan, so only Rows is populated. The
// estimate is planner-based and depends on up-to-date part metadata.
func (e *ClickhouseScrapper) EstimateQuery(ctx context.Context, sql string) (*scrapper.QueryEstimate, error) {
	columns, rows, err := scrapperstdsql.ExplainRows(ctx, e.executor, "EXPLAIN ESTIMATE "+sql)
	if err != nil {
		return nil, err
	}
	return parseClickhouseEstimate(columns, rows)
}

// parseClickhouseEstimate sums the per-table `rows` column of an
// `EXPLAIN ESTIMATE` result. Output columns are: database, table, parts, rows,
// marks.
func parseClickhouseEstimate(columns []string, rows [][]any) (*scrapper.QueryEstimate, error) {
	rowsIdx := -1
	for i, c := range columns {
		if c == "rows" {
			rowsIdx = i
			break
		}
	}
	if rowsIdx < 0 {
		return nil, errors.Errorf("EXPLAIN ESTIMATE has no 'rows' column, got %v", columns)
	}

	var total int64
	for _, r := range rows {
		if rowsIdx >= len(r) {
			continue
		}
		if n, ok := scrapperstdsql.CellInt64(r[rowsIdx]); ok {
			total += n
		}
	}

	return &scrapper.QueryEstimate{Rows: &total}, nil
}
