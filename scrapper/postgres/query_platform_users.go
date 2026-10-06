package postgres

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *PostgresScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return nil, scrapper.ErrUnsupported
}
