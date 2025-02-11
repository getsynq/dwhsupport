package duckdb

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *DuckDBScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	return nil, scrapper.ErrUnsupported
}
