package clickhouse

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
	scrapperstdsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
)

func (e *ClickhouseScrapper) RunRawQuery(ctx context.Context, sql string) (scrapper.RawQueryRowIterator, error) {
	return scrapperstdsql.RunRawQuery(ctx, e.executor, sql)
}
