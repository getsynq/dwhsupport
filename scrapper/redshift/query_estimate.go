package redshift

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

// EstimateQuery is unsupported: Redshift's EXPLAIN reports only abstract planner
// cost units and cardinality, with no bytes-scanned figure to translate into a
// scan/cost estimate.
func (e *RedshiftScrapper) EstimateQuery(ctx context.Context, sql string) (*scrapper.QueryEstimate, error) {
	return nil, scrapper.ErrUnsupported
}
