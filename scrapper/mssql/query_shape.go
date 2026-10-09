package mssql

import (
	"context"
	"fmt"

	"github.com/getsynq/dwhsupport/scrapper"
	scrapperstdsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
)

func (e *MSSQLScrapper) QueryShape(ctx context.Context, sql string) ([]*scrapper.QueryShapeColumn, error) {
	// MSSQL: use TOP 0 to get column metadata without returning rows
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
