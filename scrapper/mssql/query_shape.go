package mssql

import (
	"context"
	"fmt"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *MSSQLScrapper) QueryShape(ctx context.Context, sql string) ([]*scrapper.QueryShapeColumn, error) {
	// MSSQL: use TOP 0 to get column metadata without returning rows
	wrappedSQL := fmt.Sprintf("WITH _synq_shape_cte AS (%s) SELECT TOP 0 * FROM _synq_shape_cte", sql)

	sqlRows, err := e.executor.GetDb().QueryContext(ctx, wrappedSQL)
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	columnTypes, err := sqlRows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	result := make([]*scrapper.QueryShapeColumn, len(columnTypes))
	for i, ct := range columnTypes {
		result[i] = &scrapper.QueryShapeColumn{
			Name:       ct.Name(),
			NativeType: ct.DatabaseTypeName(),
			Position:   int32(i + 1),
		}
	}

	return result, nil
}
