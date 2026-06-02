package databricks

import (
	"context"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
)

func (e *DatabricksScrapper) QuerySchemas(ctx context.Context) ([]*scrapper.SchemaRow, error) {
	var res []*scrapper.SchemaRow

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
			res = append(res, &scrapper.SchemaRow{
				Instance:    e.conf.WorkspaceUrl,
				Database:    schemaInfo.CatalogName,
				Schema:      schemaInfo.Name,
				Description: lo.EmptyableToPtr(schemaInfo.Comment),
				SchemaOwner: lo.EmptyableToPtr(schemaInfo.Owner),
			})
		}
	}
	return res, nil
}
