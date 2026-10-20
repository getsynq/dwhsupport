package bigquery

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
)

func (e *BigQueryScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	// Constraints are now provided via QueryTables(WithConstraints()) to avoid
	// a separate Table.Metadata API call for every table, which was causing
	// BigQuery rate limit exhaustion on large projects.
	return nil, scrapper.ErrUnsupported
}

// extractConstraintsFromMeta extracts the primary key, partitioning and
// clustering constraints from BigQuery table metadata. A BigQuery primary key
// is always NOT ENFORCED.
func extractConstraintsFromMeta(projectId, datasetID, tableAlias string, tableMeta *bigquery.TableMetadata) []*scrapper.TableConstraintRow {
	var rows []*scrapper.TableConstraintRow

	if tableMeta.TableConstraints != nil && tableMeta.TableConstraints.PrimaryKey != nil {
		for i, col := range tableMeta.TableConstraints.PrimaryKey.Columns {
			rows = append(rows, &scrapper.TableConstraintRow{
				Database:       projectId,
				Schema:         datasetID,
				Table:          tableAlias,
				ConstraintName: "primary_key",
				ColumnName:     col,
				ConstraintType: scrapper.ConstraintTypePrimaryKey,
				ColumnPosition: int32(i + 1),
				IsEnforced:     lo.ToPtr(false),
			})
		}
	}

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
