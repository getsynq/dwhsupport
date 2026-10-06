package fabric

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *FabricScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return nil, scrapper.ErrUnsupported
}
