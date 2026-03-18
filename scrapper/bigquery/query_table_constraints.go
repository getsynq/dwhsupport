package bigquery

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *BigQueryScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	// Constraints are now provided via QueryTables(WithConstraints()) to avoid
	// a separate Table.Metadata API call for every table, which was causing
	// BigQuery rate limit exhaustion on large projects.
	return nil, scrapper.ErrUnsupported
}

// extractConstraintsFromMeta extracts partitioning and clustering constraints
// from BigQuery table metadata.
func extractConstraintsFromMeta(projectId, datasetID, tableAlias string, tableMeta *bigquery.TableMetadata) []*scrapper.TableConstraintRow {
	var rows []*scrapper.TableConstraintRow

	if tableMeta.TimePartitioning != nil {
		if tableMeta.TimePartitioning.Field != "" {
			rows = append(rows, &scrapper.TableConstraintRow{
				Database:       projectId,
				Schema:         datasetID,
				Table:          tableAlias,
				ConstraintName: fmt.Sprintf("time_partitioning_%s", tableMeta.TimePartitioning.Type),
				ColumnName:     tableMeta.TimePartitioning.Field,
				ConstraintType: scrapper.ConstraintTypePartitionBy,
				ColumnPosition: 1,
			})
		}
	}

	if tableMeta.RangePartitioning != nil {
		rows = append(rows, &scrapper.TableConstraintRow{
			Database:       projectId,
			Schema:         datasetID,
			Table:          tableAlias,
			ConstraintName: "range_partitioning",
			ColumnName:     tableMeta.RangePartitioning.Field,
			ConstraintType: scrapper.ConstraintTypePartitionBy,
			ColumnPosition: 1,
		})
	}

	if tableMeta.Clustering != nil {
		for i, col := range tableMeta.Clustering.Fields {
			rows = append(rows, &scrapper.TableConstraintRow{
				Database:       projectId,
				Schema:         datasetID,
				Table:          tableAlias,
				ConstraintName: "clustering",
				ColumnName:     col,
				ConstraintType: scrapper.ConstraintTypeClusterBy,
				ColumnPosition: int32(i + 1),
			})
		}
	}

	return rows
}
