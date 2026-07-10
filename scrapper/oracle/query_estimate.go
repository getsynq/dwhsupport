package oracle

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

// EstimateQuery is unsupported for now. Oracle has no single-statement EXPLAIN
// that returns an estimate as a result set: `EXPLAIN PLAN FOR` writes to
// PLAN_TABLE and requires a follow-up read, and the Cardinality/Bytes columns
// depend on gathered optimizer statistics. Returning ErrUnsupported until a
// validation spike confirms a trustworthy, side-effect-free path.
func (e *OracleScrapper) EstimateQuery(ctx context.Context, sql string) (*scrapper.QueryEstimate, error) {
	return nil, scrapper.ErrUnsupported
}
