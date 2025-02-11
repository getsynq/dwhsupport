package redshift

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *RedshiftScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	return nil, scrapper.ErrUnsupported
}
