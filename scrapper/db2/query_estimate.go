package db2

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

// EstimateQuery is unsupported: Db2's EXPLAIN writes into explain tables the
// user has to create first, and reads back from them.
func (e *Db2Scrapper) EstimateQuery(ctx context.Context, sql string) (*scrapper.QueryEstimate, error) {
	return nil, scrapper.ErrUnsupported
}
