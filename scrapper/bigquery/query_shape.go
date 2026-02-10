package bigquery

import (
	"context"
	"fmt"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *BigQueryScrapper) QueryShape(ctx context.Context, sql string) ([]*scrapper.QueryShapeColumn, error) {
	wrappedSQL := fmt.Sprintf("WITH _synq_shape_cte AS (%s) SELECT * FROM _synq_shape_cte LIMIT 0", sql)

	query := e.executor.GetBigQueryClient().Query(wrappedSQL)

	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}

	schema := it.Schema
	result := make([]*scrapper.QueryShapeColumn, len(schema))
	for i, field := range schema {
		result[i] = &scrapper.QueryShapeColumn{
			Name:       field.Name,
			NativeType: string(field.Type),
			Position:   int32(i + 1),
		}
	}

	return result, nil
}
