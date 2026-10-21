package databricks

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/databricks/databricks-sdk-go/apierr"
	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *DatabricksScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {

	log := logging.GetLogger(ctx)

	throttleBefore := e.ThrottleStats()
	defer func() { e.logThrottleStats(ctx, throttleBefore) }()

	var res []*scrapper.TableMetricsRow
	// omitted holds the rows of tables whose statistics could not be read after the
	// listing, left out for a caller that accepts incomplete results.
	omitted := map[*scrapper.TableMetricsRow]bool{}
	var mutex sync.Mutex

	noScan := " NOSCAN"
	if e.conf.RefreshTableMetricsUseScan {
		noScan = ""
	}

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(16)

	walk := e.walkUnityCatalog(ctx, scrapper.OmittedMetrics)
	err := walk.forEachSchema(ctx, servicecatalog.ListTablesRequest{OmitColumns: true},
		func(_ servicecatalog.SchemaInfo, tables []servicecatalog.TableInfo) error {
			for _, tableInfo := range tables {
				if err := groupCtx.Err(); err != nil {
					return err
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

				refresh := e.shouldRefreshTableInfo(lastMetricsFetchTime, tableInfo)
				if !refresh && e.refreshSkippedAsUnchanged(tableInfo) {
					metricsRow.MarkNotReRead()
				}

				mutex.Lock()
				res = append(res, metricsRow)
				mutex.Unlock()

				if refresh {
					g.Go(func() error {
						sql, err := analyzeTableStatement(tableInfo, noScan)
						if err != nil {
							return errors.Wrapf(err, "failed to build ANALYZE TABLE for %s", tableInfo.FullName)
						}
						log.WithField("sql", sql).WithFields(logrus.Fields{
							"table_updated_at": time.UnixMilli(tableInfo.UpdatedAt).Format(time.RFC3339),
							"last_analyzed_at": lastMetricsFetchTime.Format(time.RFC3339),
						}).Infof("Analyzing table %s", tableInfo.FullName)
						// A failed ANALYZE is not a metric left out: it only refreshes the
						// statistics the read below returns, which are still the table's. It
						// needs MODIFY on the table, which a read-only role lacks for every
						// table, so reporting it would flag the whole workspace on every run.
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
							// A table dropped since the listing keeps the numbers the listing
							// carried, as does every table for a caller that did not ask for
							// incomplete results. Any other failure leaves the table out and
							// reports it, rather than pass the numbers from before the refresh
							// off as current.
							if !walk.acceptIncomplete || apierr.IsMissing(err) {
								log.Warn(err)
								return nil
							}
							omission := scrapper.Omission{
								Database: tableInfo.CatalogName, Schema: tableInfo.SchemaName, Table: tableInfo.Name,
								Part: scrapper.OmittedMetrics, Err: err,
							}
							if err := walk.leaveOut(groupCtx, omission); err != nil {
								return err
							}
							mutex.Lock()
							omitted[metricsRow] = true
							mutex.Unlock()
							return nil
						}
						mutex.Lock()
						if extractedRowCount, ok := extractNumericProperty(r.Properties, "spark.sql.statistics.numRows"); ok {
							metricsRow.RowCount = &extractedRowCount
						}

						if extractedByteSize, ok := extractNumericProperty(r.Properties, "spark.sql.statistics.totalSize"); ok {
							metricsRow.SizeBytes = &extractedByteSize
						}
						mutex.Unlock()
						return nil
					})
				}
			}
			return nil
		})

	// The walk stops when a table read fails the run, and then it is that failure the
	// run reports, not the cancellation the walk saw.
	if waitErr := g.Wait(); waitErr != nil {
		return nil, errors.Wrap(waitErr, "failed to process metrics of a table")
	}
	if err != nil {
		return nil, err
	}

	return lo.Reject(res, func(row *scrapper.TableMetricsRow, _ int) bool { return omitted[row] }), walk.result()

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

// refreshSkippedAsUnchanged says whether a table shouldRefreshTableInfo turned down was turned
// down only for not changing since the last fetch, so the statistics its last refresh read still
// hold. A view has none, and with refreshing off or no update time there was no refresh to skip.
func (e *DatabricksScrapper) refreshSkippedAsUnchanged(tableInfo servicecatalog.TableInfo) bool {
	if tableInfo.TableType == servicecatalog.TableTypeView || tableInfo.TableType == servicecatalog.TableTypeMaterializedView {
		return false
	}
	return e.conf.RefreshTableMetrics && tableInfo.UpdatedAt > 0
}

func analyzeTableStatement(tableInfo servicecatalog.TableInfo, noScan string) (string, error) {
	name, err := quotedName(tableInfo.CatalogName, tableInfo.SchemaName, tableInfo.Name)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ANALYZE TABLE %s COMPUTE STATISTICS%s", name, noScan), nil
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
