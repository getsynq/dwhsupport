package databricks

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *DatabricksScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	return nil, scrapper.ErrUnsupported
}
