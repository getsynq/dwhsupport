package duckdb

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *DuckDBScrapper) QueryRoutines(ctx context.Context) ([]*scrapper.RoutineRow, error) {
	return nil, scrapper.ErrUnsupported
}
