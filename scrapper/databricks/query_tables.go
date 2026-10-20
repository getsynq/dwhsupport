package databricks

import (
	"context"
	"time"

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
					Owner:       lo.EmptyableToPtr(tableInfo.Owner),
					CreatedAt:   epochMillisToTime(tableInfo.CreatedAt),
				})
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return res, walk.result()
}

// epochMillisToTime converts a Unity Catalog timestamp in epoch milliseconds,
// nil for 0, which the API sends when it does not know the time.
func epochMillisToTime(millis int64) *time.Time {
	if millis == 0 {
		return nil
	}
	return lo.ToPtr(time.UnixMilli(millis).UTC())
}
