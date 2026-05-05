package athena

import (
	"context"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
)

// V1 is metadata-only. The methods below either return empty results or
// ErrUnsupported; later phases will fill them in.

func (e *AthenaScrapper) QueryTableMetrics(ctx context.Context, lastFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	return nil, nil
}

func (e *AthenaScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	return nil, nil
}
