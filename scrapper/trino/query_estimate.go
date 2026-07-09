package trino

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

// EstimateQuery is unsupported for now. Trino's `EXPLAIN (TYPE IO)` /
// `EXPLAIN (TYPE DISTRIBUTED)` can surface an estimate, but whether a reliable
// pre-run bytes figure is available depends heavily on the backing connector's
// statistics (Hive/Iceberg with fresh stats vs none), and it has not been
// validated against our pinned driver and typical connectors. Returning
// ErrUnsupported until a validation spike confirms a trustworthy estimate.
func (e *TrinoScrapper) EstimateQuery(ctx context.Context, sql string) (*scrapper.QueryEstimate, error) {
	return nil, scrapper.ErrUnsupported
}
