package postgres

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
	scrapperstdsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
)

func (e *PostgresScrapper) RunRawQuery(ctx context.Context, sql string) (scrapper.RawQueryRowIterator, error) {
	return scrapperstdsql.RunRawQuery(ctx, e.executor, sql)
}
