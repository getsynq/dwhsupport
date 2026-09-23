package db2

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
	scrapperstdsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
)

func (e *Db2Scrapper) QueryCustomMetrics(ctx context.Context, sql string, args ...any) ([]*scrapper.CustomMetricsRow, error) {
	return scrapperstdsql.QueryCustomMetrics(ctx, e.executor, e.DialectType(), sql, args...)
}
