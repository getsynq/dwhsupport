package mysql

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

// EstimateQuery is unsupported for now. MySQL's `EXPLAIN FORMAT=JSON` exposes
// per-table planner estimates (rows_examined_per_scan / rows_produced_per_join)
// but no single authoritative row or byte total, and the JSON shape differs
// between MySQL and MariaDB. Returning ErrUnsupported until a validation spike
// settles a reliable aggregation across both.
func (e *MySQLScrapper) EstimateQuery(ctx context.Context, sql string) (*scrapper.QueryEstimate, error) {
	return nil, scrapper.ErrUnsupported
}
