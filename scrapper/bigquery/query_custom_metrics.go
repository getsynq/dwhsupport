package bigquery

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *BigQueryScrapper) QueryCustomMetrics(ctx context.Context, sql string, args ...any) ([]*scrapper.CustomMetricsRow, error) {
	return nil, scrapper.ErrUnsupported
}
