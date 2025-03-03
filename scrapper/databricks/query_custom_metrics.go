package databricks

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
	scrappersqtsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
)

func (e *DatabricksScrapper) QueryCustomMetrics(ctx context.Context, sql string, args ...any) ([]*scrapper.CustomMetricsRow, error) {
	executor, err := e.lazyExecutor.Get()
	if err != nil {
		return nil, err
	}
	return scrappersqtsql.QueryCustomMetrics(ctx, executor.GetDb(), sql, args...)
}
