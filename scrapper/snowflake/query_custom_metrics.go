package snowflake

import (
	"context"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/scrapper"
	scrappersqtsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
)

func (e *SnowflakeScrapper) QueryCustomMetrics(ctx context.Context, sql string, args ...any) ([]*scrapper.CustomMetricsRow, error) {
	ctx = dwhexecsnowflake.EnrichSnowflakeContext(ctx, e.executor.GetDb().DB)
	return scrappersqtsql.QueryCustomMetrics(ctx, e.executor.GetDb(), sql, args...)
}
