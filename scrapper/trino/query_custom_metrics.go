package trino

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
	scrappersqtsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
)

func (e *TrinoScrapper) QueryCustomMetrics(ctx context.Context, sql string, args ...any) ([]*scrapper.CustomMetricsRow, error) {
	return scrappersqtsql.QueryCustomMetrics(ctx, e.executor.GetDb(), sql, args...)
}
