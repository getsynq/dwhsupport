package bigquery

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *BigQueryScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	return nil, scrapper.ErrUnsupported
}
