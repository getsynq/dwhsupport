package mysql

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *MySQLScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return nil, scrapper.ErrUnsupported
}
