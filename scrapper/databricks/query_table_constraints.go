package databricks

import (
	"context"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *DatabricksScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	var results []*scrapper.TableConstraintRow
	walk := e.walkUnityCatalog(ctx, scrapper.OmittedObjects)
	err := walk.forEachSchema(ctx, servicecatalog.ListTablesRequest{},
		func(_ servicecatalog.SchemaInfo, tables []servicecatalog.TableInfo) error {
			for _, tableInfo := range tables {
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
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return results, walk.result()
}
