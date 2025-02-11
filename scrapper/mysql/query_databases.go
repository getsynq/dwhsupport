package mysql

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *MySQLScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	return nil, scrapper.ErrUnsupported
}
