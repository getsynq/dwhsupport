package athena

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *AthenaScrapper) QueryRoutines(ctx context.Context) ([]*scrapper.RoutineRow, error) {
	return nil, scrapper.ErrUnsupported
}
