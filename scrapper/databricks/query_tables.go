package databricks

import (
	"context"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"

	"github.com/samber/lo"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *DatabricksScrapper) QueryTables(ctx context.Context, opts ...scrapper.QueryTablesOption) ([]*scrapper.TableRow, error) {
	var res []*scrapper.TableRow
	walk := e.walkUnityCatalog(ctx, scrapper.OmittedObjects)
	err := walk.forEachSchema(ctx, servicecatalog.ListTablesRequest{},
		func(_ servicecatalog.SchemaInfo, tables []servicecatalog.TableInfo) error {
			for _, tableInfo := range tables {
				res = append(res, &scrapper.TableRow{
					Instance:    e.conf.WorkspaceUrl,
					Database:    tableInfo.CatalogName,
					Schema:      tableInfo.SchemaName,
					Table:       tableInfo.Name,
					TableType:   tableInfo.TableType.String(),
					Description: lo.EmptyableToPtr(tableInfo.Comment),
				})
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return res, walk.result()
}
