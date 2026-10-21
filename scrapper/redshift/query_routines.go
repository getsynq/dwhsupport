package redshift

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *RedshiftScrapper) QueryRoutines(ctx context.Context) ([]*scrapper.RoutineRow, error) {
	return nil, scrapper.ErrUnsupported
}
