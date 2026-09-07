package databricks

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"sync"
	"time"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *DatabricksScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {

	log := logging.GetLogger(ctx)

	scopeFilter := e.effectiveScope(ctx)

	throttleBefore := e.ThrottleStats()
	defer func() { e.logThrottleStats(ctx, throttleBefore) }()

	var res []*scrapper.TableMetricsRow
	var mutex sync.Mutex

	noScan := " NOSCAN"
	if e.conf.RefreshTableMetricsUseScan {
		noScan = ""
	}

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(16)

	catalogs, err := e.listCatalogs(ctx)
	if err != nil {
		return nil, err
	}
	for _, catalogInfo := range catalogs {
		if e.isIgnoredCatalog(catalogInfo) {
			continue
		}
		if !scopeFilter.IsDatabaseAccepted(catalogInfo.Name) {
			log.Infof("catalog %s excluded by scope filter", catalogInfo.Name)
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
				log.Infof("schema %s.%s excluded by scope filter", catalogInfo.Name, schemaInfo.Name)
				continue
			}

			tables, err := e.listTables(
				ctx,
				servicecatalog.ListTablesRequest{
					CatalogName: catalogInfo.Name,
					SchemaName:  schemaInfo.Name,
					OmitColumns: true,
				},
			)
			if err != nil {
				return nil, err
			}

			for _, tableInfo := range tables {
				if !scopeFilter.IsObjectAccepted(catalogInfo.Name, schemaInfo.Name, tableInfo.Name) {
					log.Infof("table %s.%s.%s excluded by scope filter", catalogInfo.Name, schemaInfo.Name, tableInfo.Name)
					continue
				}

				select {
				case <-groupCtx.Done():
					return nil, groupCtx.Err()
				default:
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

				if extractedRowCount, ok := extractNumericProperty(tableInfo.Properties, "spark.sql.statistics.numRows"); ok {
					metricsRow.RowCount = &extractedRowCount
				}

				if extractedByteSize, ok := extractNumericProperty(tableInfo.Properties, "spark.sql.statistics.totalSize"); ok {
					metricsRow.SizeBytes = &extractedByteSize
				}

				mutex.Lock()
				res = append(res, metricsRow)
				mutex.Unlock()

				if e.shouldRefreshTableInfo(lastMetricsFetchTime, tableInfo) {
					// Capture variables in loop
					tableInfo := tableInfo
					metricsRow := metricsRow
					g.Go(func() error {
						sql := fmt.Sprintf("ANALYZE TABLE %s COMPUTE STATISTICS%s", tableInfo.FullName, noScan)
						log.WithField("sql", sql).WithFields(logrus.Fields{
							"table_updated_at": time.UnixMilli(tableInfo.UpdatedAt).Format(time.RFC3339),
							"last_analyzed_at": lastMetricsFetchTime.Format(time.RFC3339),
						}).Infof("Analyzing table %s", tableInfo.FullName)
						if sqlClient, err := e.lazyExecutor.Get(); err == nil {
							err := sqlClient.Exec(groupCtx, sql)
							if err != nil {
								log.Warn(errors.Wrapf(err, "failed to %s", sql))
							}
						}

						r, err := e.client.Tables.Get(groupCtx, servicecatalog.GetTableRequest{
							FullName:             tableInfo.FullName,
							IncludeDeltaMetadata: true,
						})
						if err != nil {
							err = errors.Wrapf(err, "failed to get properties of %s", tableInfo.FullName)
							// A table whose properties the workspace refused to serve for exceeding
							// its quota fails the run. The row counts and sizes of every table read
							// before it are already in res, and reporting those as the workspace's
							// metrics is how a rate-limited run looks like tables that stopped
							// growing — the pacing has already spent its attempts by the time this
							// is reached, so there is nothing left to wait out.
							if dwhexecdatabricks.IsRateLimitError(err) {
								return err
							}
							log.Warn(err)
						}
						if r != nil {
							mutex.Lock()
							if extractedRowCount, ok := extractNumericProperty(r.Properties, "spark.sql.statistics.numRows"); ok {
								metricsRow.RowCount = &extractedRowCount
							}

							if extractedByteSize, ok := extractNumericProperty(r.Properties, "spark.sql.statistics.totalSize"); ok {
								metricsRow.SizeBytes = &extractedByteSize
							}
							mutex.Unlock()
						}
						return nil
					})
				}
			}
		}
	}

	err = g.Wait()
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
