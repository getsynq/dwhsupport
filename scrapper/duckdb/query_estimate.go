package duckdb

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

// EstimateQuery is unsupported: DuckDB's EXPLAIN emits only planner cost and
// cardinality estimates, with no bytes-scanned figure. (DuckDB is embedded and
// local, so a pre-run scan estimate has little value anyway.)
func (e *DuckDBScrapper) EstimateQuery(ctx context.Context, sql string) (*scrapper.QueryEstimate, error) {
	return nil, scrapper.ErrUnsupported
}
