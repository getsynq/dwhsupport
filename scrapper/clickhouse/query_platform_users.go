package clickhouse

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *ClickhouseScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return nil, scrapper.ErrUnsupported
}
