package databricks

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

// EstimateQuery is unsupported for now. Databricks/Spark EXPLAIN output is a
// cost-based-optimizer plan whose byte/row estimates require CBO statistics
// (ANALYZE TABLE ... COMPUTE STATISTICS) that are frequently absent, and the
// plan text format is not stable enough to parse reliably. Returning
// ErrUnsupported until a validation spike confirms a trustworthy estimate.
func (e *DatabricksScrapper) EstimateQuery(ctx context.Context, sql string) (*scrapper.QueryEstimate, error) {
	return nil, scrapper.ErrUnsupported
}
