package databricks

import (
	"context"
	"fmt"
	"strings"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
)

func (e *DatabricksScrapper) QueryTableConstraints(ctx context.Context, schema string, table string) ([]*scrapper.TableConstraintRow, error) {
	// schema in Databricks is "catalog.schema" format, but the method receives just the schema name.
	// We need to find the catalog. Try to get the table info using the SDK.
	tableInfo, err := e.getTableInfo(ctx, schema, table)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get table info")
	}
	if tableInfo == nil {
		return nil, nil
	}

	var results []*scrapper.TableConstraintRow

	// Extract primary key and unique constraints from table constraints
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

	// Extract partitioning columns from columns metadata
	if len(tableInfo.Columns) > 0 {
		partitionPos := int32(0)
		for _, col := range tableInfo.Columns {
			if col.PartitionIndex > 0 {
				partitionPos++
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

	return results, nil
}

func (e *DatabricksScrapper) getTableInfo(ctx context.Context, schema string, table string) (*servicecatalog.TableInfo, error) {
	// Try each catalog to find the table
	catalogs, err := e.client.Catalogs.ListAll(ctx, servicecatalog.ListCatalogsRequest{})
	if err != nil {
		return nil, err
	}

	for _, catalogInfo := range catalogs {
		if e.isIgnoredCatalog(catalogInfo) {
			continue
		}
		fullName := fmt.Sprintf("%s.%s.%s", catalogInfo.Name, schema, table)
		tableInfo, err := e.client.Tables.GetByFullName(ctx, fullName)
		if err != nil {
			if strings.Contains(err.Error(), "NOT_FOUND") || strings.Contains(err.Error(), "DOES_NOT_EXIST") {
				continue
			}
			return nil, err
		}
		return tableInfo, nil
	}

	return nil, nil
}
