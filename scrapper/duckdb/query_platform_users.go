package duckdb

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *DuckDBScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return nil, scrapper.ErrUnsupported
}
