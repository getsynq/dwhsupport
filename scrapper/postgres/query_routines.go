package postgres

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *PostgresScrapper) QueryRoutines(ctx context.Context) ([]*scrapper.RoutineRow, error) {
	return nil, scrapper.ErrUnsupported
}
