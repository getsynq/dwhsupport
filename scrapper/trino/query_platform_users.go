package trino

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *TrinoScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return nil, scrapper.ErrUnsupported
}
