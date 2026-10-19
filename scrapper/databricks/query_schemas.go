package databricks

import (
	"context"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
)

func (e *DatabricksScrapper) QuerySchemas(ctx context.Context) ([]*scrapper.SchemaRow, error) {
	var res []*scrapper.SchemaRow
	walk := e.walkUnityCatalog(ctx, scrapper.OmittedObjects)
	err := walk.forEachCatalog(ctx, func(_ servicecatalog.CatalogInfo, schemas []servicecatalog.SchemaInfo) error {
		walk.markRead()
		for _, schemaInfo := range schemas {
			res = append(res, &scrapper.SchemaRow{
				Instance:    e.conf.WorkspaceUrl,
				Database:    schemaInfo.CatalogName,
				Schema:      schemaInfo.Name,
				Description: lo.EmptyableToPtr(schemaInfo.Comment),
				SchemaOwner: lo.EmptyableToPtr(schemaInfo.Owner),
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, walk.result()
}
