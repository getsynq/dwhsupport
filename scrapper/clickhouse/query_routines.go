package clickhouse

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *ClickhouseScrapper) QueryRoutines(ctx context.Context) ([]*scrapper.RoutineRow, error) {
	return nil, scrapper.ErrUnsupported
}
