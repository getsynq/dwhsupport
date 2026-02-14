package clickhouse

import (
	"context"

	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/scrapper"
	scrappersqtsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
)

func (e *ClickhouseScrapper) QueryCustomMetrics(ctx context.Context, sql string, args ...any) ([]*scrapper.CustomMetricsRow, error) {
	return scrappersqtsql.QueryCustomMetrics(dwhexecclickhouse.EnrichClickhouseContext(ctx), e.executor.GetDb(), sql, args...)
}
