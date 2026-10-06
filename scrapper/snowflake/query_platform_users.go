package snowflake

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *SnowflakeScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return nil, scrapper.ErrUnsupported
}
