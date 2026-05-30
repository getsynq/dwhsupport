package bigquery

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"golang.org/x/sync/errgroup"
)

// QueryTableMetrics collects table metrics (row counts, sizes, freshness) via the
// BigQuery Tables.Get API. Covers all table types including materialized views.
// Requires bigquery.tables.get + bigquery.tables.list (metadata permissions).
func (e *BigQueryScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	log := logging.GetLogger(ctx)

	allDatasets, err := e.listDatasets(ctx)
	if err != nil {
		return nil, err
	}

	var mutex sync.Mutex
	var results []*scrapper.TableMetricsRow

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(e.rateLimitCfg.MetadataConcurrency)

	for _, dataset := range allDatasets {
		if isPrivateDataset(dataset.DatasetID) {
			continue
		}

		if !e.scope.IsSchemaAccepted(e.conf.ProjectId, dataset.DatasetID) {
			log.Infof("dataset %s excluded by scope filter", dataset.DatasetID)
			continue
		}

		tableIds, err := e.listTableIDs(groupCtx, dataset.DatasetID)
		if err != nil {
			return nil, err
		}

		for _, tableId := range tableIds {
			tableId := tableId

			select {
			case <-groupCtx.Done():
				return nil, groupCtx.Err()
			default:
			}

			g.Go(func() error {
				tableMeta, err := withRateLimitRetry(groupCtx, e.rateLimitCfg, func(ctx context.Context) (*bigquery.TableMetadata, error) {
					return e.executor.GetBigQueryClient().Dataset(dataset.DatasetID).Table(tableId).Metadata(ctx)
				})
				if err != nil {
					if errIsNotFound(err) || errIsAccessDenied(err) {
						return nil
					}
					return e.scrapeError(groupCtx, "table.metadata", dataset.DatasetID, tableId, err)
				}

				numRows := int64(tableMeta.NumRows)
				numBytes := tableMeta.NumBytes
				var updatedAt *time.Time
				if !tableMeta.LastModifiedTime.IsZero() {
					t := tableMeta.LastModifiedTime
					updatedAt = &t
				}

				mutex.Lock()
				defer mutex.Unlock()
				results = append(results, &scrapper.TableMetricsRow{
					Database:  e.conf.ProjectId,
					Schema:    dataset.DatasetID,
					Table:     tableId,
					RowCount:  &numRows,
					SizeBytes: &numBytes,
					UpdatedAt: updatedAt,
				})

				return nil
			})
		}
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return collapseShardedMetrics(results)
}
