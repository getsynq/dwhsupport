package fabric

import (
	"context"
	"fmt"

	"github.com/getsynq/dwhsupport/scrapper"
	scrapperstdsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
)

func (e *FabricScrapper) QueryShape(ctx context.Context, sql string) ([]*scrapper.QueryShapeColumn, error) {
	// Fabric (like SQL Server) supports CTEs and TOP, so wrap the query and take
	// zero rows to fetch column metadata without scanning data.
	wrappedSQL := fmt.Sprintf("WITH _synq_shape_cte AS (%s) SELECT TOP 0 * FROM _synq_shape_cte", sql)

	sqlRows, err := e.executor.QueryRows(ctx, wrappedSQL)
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	columnTypes, err := sqlRows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	return scrapperstdsql.ShapeColumns(e.DialectType(), columnTypes), nil
}
