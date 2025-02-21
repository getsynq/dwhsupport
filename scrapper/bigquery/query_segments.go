package bigquery

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *BigQueryScrapper) QuerySegments(ctx context.Context, sql string, args ...any) ([]*scrapper.SegmentRow, error) {
	return nil, scrapper.ErrUnsupported
}
