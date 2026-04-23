package databricks

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
	scrapperstdsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
)

func (e *DatabricksScrapper) RunRawQuery(ctx context.Context, sql string) (scrapper.RawQueryRowIterator, error) {
	executor, err := e.lazyExecutor.Get()
	if err != nil {
		return nil, err
	}
	return scrapperstdsql.RunRawQuery(ctx, executor, sql)
}
