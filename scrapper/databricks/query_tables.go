package databricks

import (
	"context"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/getsynq/dwhsupport/logging"

	"github.com/samber/lo"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *DatabricksScrapper) QueryTables(ctx context.Context, opts ...scrapper.QueryTablesOption) ([]*scrapper.TableRow, error) {
	var res []*scrapper.TableRow

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
				res = append(res, &scrapper.TableRow{
					Instance:    e.conf.WorkspaceUrl,
					Database:    tableInfo.CatalogName,
					Schema:      tableInfo.SchemaName,
					Table:       tableInfo.Name,
					TableType:   tableInfo.TableType.String(),
					Description: lo.EmptyableToPtr(tableInfo.Comment),
				})
			}
		}
	}
	return res, nil
}
