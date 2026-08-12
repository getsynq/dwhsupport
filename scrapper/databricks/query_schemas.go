package databricks

import (
	"context"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
)

func (e *DatabricksScrapper) QuerySchemas(ctx context.Context) ([]*scrapper.SchemaRow, error) {
	var res []*scrapper.SchemaRow
	scopeFilter := e.effectiveScope(ctx)

	catalogs, err := e.listCatalogs(ctx)
	if err != nil {
		return nil, err
	}
	for _, catalogInfo := range catalogs {
		if e.isIgnoredCatalog(catalogInfo) {
			continue
		}
		if !scopeFilter.IsDatabaseAccepted(catalogInfo.Name) {
			logging.GetLogger(ctx).Infof("catalog %s excluded by scope filter", catalogInfo.Name)
			continue
		}

		schemas, err := e.listSchemas(ctx, catalogInfo.Name)
		if err != nil {
			return nil, err
		}
		for _, schemaInfo := range schemas {
			if schemaInfo.Name == "information_schema" {
				continue
			}
			if !scopeFilter.IsSchemaAccepted(catalogInfo.Name, schemaInfo.Name) {
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
