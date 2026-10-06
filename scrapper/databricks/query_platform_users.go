package databricks

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *DatabricksScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return nil, scrapper.ErrUnsupported
}
