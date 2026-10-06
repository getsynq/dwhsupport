package athena

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *AthenaScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return nil, scrapper.ErrUnsupported
}
