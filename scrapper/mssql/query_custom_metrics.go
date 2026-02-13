package mssql

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
	scrapperstdsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
)

func (e *MSSQLScrapper) QueryCustomMetrics(ctx context.Context, sql string, args ...any) ([]*scrapper.CustomMetricsRow, error) {
	return scrapperstdsql.QueryCustomMetrics(ctx, e.executor.GetDb(), sql, args...)
}
