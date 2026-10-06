package duckdb

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

// QueryPlatformUsers is unsupported: DuckDB is an embedded database with no
// authentication and no users. MotherDuck accounts are not exposed through SQL.
func (e *DuckDBScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return nil, scrapper.ErrUnsupported
}
