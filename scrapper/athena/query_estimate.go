package athena

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

// EstimateQuery is unsupported for now. Athena is Presto-derived and its
// EXPLAIN surface mirrors Trino's; a pre-run bytes estimate depends on the Glue
// table statistics being populated, which is not guaranteed. Athena does bill
// on bytes scanned, so a reliable estimate is valuable — but it needs a
// validation spike before we advertise it. Returning ErrUnsupported until then.
func (e *AthenaScrapper) EstimateQuery(ctx context.Context, sql string) (*scrapper.QueryEstimate, error) {
	return nil, scrapper.ErrUnsupported
}
