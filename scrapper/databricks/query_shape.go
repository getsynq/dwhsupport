package databricks

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
	scrappersqtsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
)

func (e *DatabricksScrapper) QueryShape(ctx context.Context, sql string) ([]*scrapper.QueryShapeColumn, error) {
	executor, err := e.lazyExecutor.Get()
	if err != nil {
		return nil, err
	}
	return scrappersqtsql.QueryShape(ctx, executor.GetDb(), sql)
}
