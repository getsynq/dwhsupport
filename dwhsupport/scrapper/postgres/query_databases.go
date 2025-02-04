package postgres

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *PostgresScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	return nil, scrapper.ErrUnsupported
}
