package db2

import (
	"context"
	"fmt"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *Db2Scrapper) QueryShape(ctx context.Context, sql string) ([]*scrapper.QueryShapeColumn, error) {
	wrappedSQL := fmt.Sprintf("WITH synq_shape_cte AS (%s) SELECT * FROM synq_shape_cte FETCH FIRST 0 ROWS ONLY", sql)

	sqlRows, err := e.executor.QueryRows(ctx, wrappedSQL)
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
	return scrapper.SetKinds(e.DialectType(), result), nil
}
