package oracle

import (
	"context"
	"fmt"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *OracleScrapper) QueryShape(ctx context.Context, sql string) ([]*scrapper.QueryShapeColumn, error) {
	// Oracle doesn't support LIMIT, use FETCH FIRST 0 ROWS ONLY (Oracle 12c+)
	wrappedSQL := fmt.Sprintf("WITH _synq_shape_cte AS (%s) SELECT * FROM _synq_shape_cte FETCH FIRST 0 ROWS ONLY", sql)

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
