package oracle

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *OracleScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return nil, scrapper.ErrUnsupported
}
