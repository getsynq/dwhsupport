package mysql

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *MySQLScrapper) QueryRoutines(ctx context.Context) ([]*scrapper.RoutineRow, error) {
	return nil, scrapper.ErrUnsupported
}
