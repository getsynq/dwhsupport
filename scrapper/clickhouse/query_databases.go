package clickhouse

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *ClickhouseScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	return nil, scrapper.ErrUnsupported
}
