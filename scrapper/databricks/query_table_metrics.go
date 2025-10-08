package databricks

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"sync"
	"time"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/xxjwxc/gowp/workpool"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *DatabricksScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {

	log := logging.GetLogger(ctx)

	var res []*scrapper.TableMetricsRow
	var errorMessages []string
	var mutex sync.Mutex

	noScan := " NOSCAN"
	if e.conf.RefreshTableMetricsUseScan {
		noScan = ""
	}

	pool := workpool.New(16)

	catalogs, err := e.client.Catalogs.ListAll(ctx, servicecatalog.ListCatalogsRequest{})
	if err != nil {
		return nil, err
	}
	for _, catalogInfo := range catalogs {
		if e.isIgnoredCatalog(catalogInfo) {
			continue
		}
		if e.blocklist.IsBlocked(catalogInfo.FullName) {
			log.Infof("catalog %s excluded by blocklist", catalogInfo.FullName)
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
			if e.blocklist.IsBlocked(schemaInfo.FullName) {
				log.Infof("schema %s excluded by blocklist", schemaInfo.FullName)
				continue
			}

			tables, err := e.client.Tables.ListAll(
				ctx,
				servicecatalog.ListTablesRequest{
					CatalogName:          catalogInfo.Name,
					SchemaName:           schemaInfo.Name,
					OmitColumns:          true,
					IncludeDeltaMetadata: true,
				},
			)
			if err != nil {
				return nil, err
			}

			for _, tableInfo := range tables {
				if e.blocklist.IsBlocked(tableInfo.FullName) {
					log.Infof("table %s excluded by blocklist", tableInfo.FullName)
					continue
				}
				updatedAt := time.UnixMilli(tableInfo.UpdatedAt)
				metricsRow := &scrapper.TableMetricsRow{
					Instance:  e.conf.WorkspaceUrl,
					Database:  tableInfo.CatalogName,
					Schema:    tableInfo.SchemaName,
					Table:     tableInfo.Name,
					RowCount:  nil,
					UpdatedAt: &updatedAt,
				}
				res = append(res, metricsRow)

				if extractedRowCount, ok := extractNumericProperty(tableInfo.Properties, "spark.sql.statistics.numRows"); ok {
					metricsRow.RowCount = &extractedRowCount
				}

				if extractedByteSize, ok := extractNumericProperty(tableInfo.Properties, "spark.sql.statistics.totalSize"); ok {
					metricsRow.SizeBytes = &extractedByteSize
				}

				if e.shouldRefreshTableInfo(lastMetricsFetchTime, tableInfo) {
					pool.Do(func() error {
						sql := fmt.Sprintf("ANALYZE TABLE %s COMPUTE STATISTICS%s", tableInfo.FullName, noScan)
						log.WithField("sql", sql).WithFields(logrus.Fields{
							"table_updated_at": time.UnixMilli(tableInfo.UpdatedAt).Format(time.RFC3339),
							"last_analyzed_at": lastMetricsFetchTime.Format(time.RFC3339),
						}).Infof("Analyzing table %s", tableInfo.FullName)
						if sqlClient, err := e.lazyExecutor.Get(); err == nil {
							_, err := sqlClient.GetDb().ExecContext(ctx, sql)
							if err != nil {
								err = errors.Wrapf(err, "failed to %s", sql)
								log.Warn(err)
								mutex.Lock()
								errorMessages = append(errorMessages, err.Error())
								mutex.Unlock()
							}
						}

						r, err := e.client.Tables.Get(ctx, servicecatalog.GetTableRequest{
							FullName:             tableInfo.FullName,
							IncludeDeltaMetadata: true,
						})
						if err != nil {
							err = errors.Wrapf(err, "failed to get properties of %s", tableInfo.FullName)
							log.Warn(err)
							mutex.Lock()
							errorMessages = append(errorMessages, err.Error())
							mutex.Unlock()
						}
						if r != nil {
							if extractedRowCount, ok := extractNumericProperty(r.Properties, "spark.sql.statistics.numRows"); ok {
								metricsRow.RowCount = &extractedRowCount
							}

							if extractedByteSize, ok := extractNumericProperty(r.Properties, "spark.sql.statistics.totalSize"); ok {
								metricsRow.SizeBytes = &extractedByteSize
							}
						}
						return nil
					})
				}
			}
		}
	}

	err = pool.Wait()
	if err != nil {
		return nil, errors.Wrap(err, "failed to process metrics of a table")
	}

	return res, nil

}

func (e *DatabricksScrapper) shouldRefreshTableInfo(lastMetricsFetchTime time.Time, tableInfo servicecatalog.TableInfo) bool {
	if tableInfo.TableType == servicecatalog.TableTypeView || tableInfo.TableType == servicecatalog.TableTypeMaterializedView {
		return false
	}
	if !e.conf.RefreshTableMetrics {
		return false
	}

	tableUpdate := time.UnixMilli(tableInfo.UpdatedAt)
	if tableUpdate.IsZero() {
		return false
	}
	return lastMetricsFetchTime.Before(tableUpdate)
}

func extractNumericProperty(properties map[string]string, s string) (int64, bool) {
	if v, found := properties[s]; found {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	}
	return 0, false
}
