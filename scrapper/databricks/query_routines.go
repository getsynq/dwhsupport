package databricks

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *DatabricksScrapper) QueryRoutines(ctx context.Context) ([]*scrapper.RoutineRow, error) {
	return nil, scrapper.ErrUnsupported
}
