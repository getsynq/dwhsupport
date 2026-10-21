package snowflake

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *SnowflakeScrapper) QueryRoutines(ctx context.Context) ([]*scrapper.RoutineRow, error) {
	return nil, scrapper.ErrUnsupported
}
