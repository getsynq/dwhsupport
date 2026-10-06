package mssql

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *MSSQLScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return nil, scrapper.ErrUnsupported
}
