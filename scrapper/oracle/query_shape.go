package oracle

import (
	"context"
	"fmt"

	"github.com/getsynq/dwhsupport/scrapper"
	scrapperstdsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
)

func (e *OracleScrapper) QueryShape(ctx context.Context, sql string) ([]*scrapper.QueryShapeColumn, error) {
	// Oracle doesn't support LIMIT, use FETCH FIRST 0 ROWS ONLY (Oracle 12c+)
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

	return scrapperstdsql.ShapeColumns(e.DialectType(), columnTypes), nil
}
