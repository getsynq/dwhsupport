package db2

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *Db2Scrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return nil, scrapper.ErrUnsupported
}
