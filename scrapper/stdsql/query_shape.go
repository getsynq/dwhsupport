package stdsql

import (
	"context"
	"fmt"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/jmoiron/sqlx"
)

func QueryShape(ctx context.Context, db *sqlx.DB, sql string) ([]*scrapper.QueryShapeColumn, error) {
	wrappedSQL := fmt.Sprintf("WITH _synq_shape_cte AS (%s) SELECT * FROM _synq_shape_cte LIMIT 0", sql)

	sqlRows, err := db.QueryContext(ctx, wrappedSQL)
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
