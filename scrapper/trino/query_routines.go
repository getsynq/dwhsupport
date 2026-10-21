package trino

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *TrinoScrapper) QueryRoutines(ctx context.Context) ([]*scrapper.RoutineRow, error) {
	return nil, scrapper.ErrUnsupported
}
