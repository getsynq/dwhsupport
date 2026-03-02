package databricks

import (
	"context"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *DatabricksScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	var results []*scrapper.TableConstraintRow

	catalogs, err := e.client.Catalogs.ListAll(ctx, servicecatalog.ListCatalogsRequest{})
	if err != nil {
		return nil, err
	}
	for _, catalogInfo := range catalogs {
		if e.isIgnoredCatalog(catalogInfo) {
			continue
		}
		if !e.scope.IsDatabaseAccepted(catalogInfo.Name) {
			logging.GetLogger(ctx).Infof("catalog %s excluded by scope filter", catalogInfo.Name)
			continue
		}

		schemas, err := e.client.Schemas.ListAll(ctx, servicecatalog.ListSchemasRequest{CatalogName: catalogInfo.Name})
		if err != nil {
			return nil, err
		}
		for _, schemaInfo := range schemas {
			if schemaInfo.Name == "information_schema" {
				continue
			}
			if !e.scope.IsSchemaAccepted(catalogInfo.Name, schemaInfo.Name) {
				logging.GetLogger(ctx).Infof("schema %s.%s excluded by scope filter", catalogInfo.Name, schemaInfo.Name)
				continue
			}

			tables, err := e.client.Tables.ListAll(ctx, servicecatalog.ListTablesRequest{CatalogName: catalogInfo.Name, SchemaName: schemaInfo.Name})
			if err != nil {
				return nil, err
			}
			for _, tableInfo := range tables {
				if !e.scope.IsObjectAccepted(catalogInfo.Name, schemaInfo.Name, tableInfo.Name) {
					logging.GetLogger(ctx).Infof("table %s.%s.%s excluded by scope filter", catalogInfo.Name, schemaInfo.Name, tableInfo.Name)
					continue
				}

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
		}
	}

	return results, nil
}
