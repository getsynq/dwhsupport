package databricks

import (
	"context"
	"fmt"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
)

func (e *DatabricksScrapper) QueryTableConstraints(
	ctx context.Context,
	database string,
	schema string,
	table string,
) ([]*scrapper.TableConstraintRow, error) {
	fullName := fmt.Sprintf("%s.%s.%s", database, schema, table)
	tableInfo, err := e.client.Tables.GetByFullName(ctx, fullName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get table info")
	}
	if tableInfo == nil {
		return nil, nil
	}

	var results []*scrapper.TableConstraintRow

	// Extract primary key constraints
	for _, constraint := range tableInfo.TableConstraints {
		if constraint.PrimaryKeyConstraint != nil {
			pk := constraint.PrimaryKeyConstraint
			for i, col := range pk.ChildColumns {
				results = append(results, &scrapper.TableConstraintRow{
					Instance:       e.conf.WorkspaceUrl,
					Database:       tableInfo.CatalogName,
					Schema:         tableInfo.SchemaName,
					Table:          tableInfo.Name,
					ConstraintName: pk.Name,
					ColumnName:     col,
					ConstraintType: scrapper.ConstraintTypePrimaryKey,
					ColumnPosition: int32(i + 1),
				})
			}
		}
	}

	// Extract partitioning columns
	for _, col := range tableInfo.Columns {
		if col.PartitionIndex > 0 {
			results = append(results, &scrapper.TableConstraintRow{
				Instance:       e.conf.WorkspaceUrl,
				Database:       tableInfo.CatalogName,
				Schema:         tableInfo.SchemaName,
				Table:          tableInfo.Name,
				ConstraintName: "partitioning",
				ColumnName:     col.Name,
				ConstraintType: scrapper.ConstraintTypePartitionBy,
				ColumnPosition: int32(col.PartitionIndex),
			})
		}
	}

	return results, nil
}
