package bigquery

import (
	"context"
	"fmt"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
)

func (e *BigQueryScrapper) QueryTableConstraints(ctx context.Context, database string, schema string, table string) ([]*scrapper.TableConstraintRow, error) {
	tableMeta, err := e.executor.GetBigQueryClient().Dataset(schema).Table(table).Metadata(ctx)
	if err != nil {
		if errIsNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "failed to get table metadata")
	}

	var rows []*scrapper.TableConstraintRow

	if tableMeta.TimePartitioning != nil {
		if tableMeta.TimePartitioning.Field != "" {
			rows = append(rows, &scrapper.TableConstraintRow{
				Database:       e.conf.ProjectId,
				Schema:         schema,
				Table:          table,
				ConstraintName: fmt.Sprintf("time_partitioning_%s", tableMeta.TimePartitioning.Type),
				ColumnName:     tableMeta.TimePartitioning.Field,
				ConstraintType: scrapper.ConstraintTypePartitionBy,
				ColumnPosition: 1,
			})
		}
	}

	if tableMeta.RangePartitioning != nil {
		rows = append(rows, &scrapper.TableConstraintRow{
			Database:       e.conf.ProjectId,
			Schema:         schema,
			Table:          table,
			ConstraintName: "range_partitioning",
			ColumnName:     tableMeta.RangePartitioning.Field,
			ConstraintType: scrapper.ConstraintTypePartitionBy,
			ColumnPosition: 1,
		})
	}

	if tableMeta.Clustering != nil {
		for i, col := range tableMeta.Clustering.Fields {
			rows = append(rows, &scrapper.TableConstraintRow{
				Database:       e.conf.ProjectId,
				Schema:         schema,
				Table:          table,
				ConstraintName: "clustering",
				ColumnName:     col,
				ConstraintType: scrapper.ConstraintTypeClusterBy,
				ColumnPosition: int32(i + 1),
			})
		}
	}

	return rows, nil
}
