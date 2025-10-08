package bigquery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

var (
	tableMetricsSql = `
	SELECT
		project_id as database,
		dataset_id as schema,
		table_id as table,
		row_count as row_count,
		size_bytes as size_bytes,
		TIMESTAMP_MILLIS(last_modified_time) as updated_at
	FROM %s.%s.__TABLES__
`
)

func (e *BigQueryScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	log := logging.GetLogger(ctx)

	datasets := e.executor.GetBigQueryClient().Datasets(ctx)
	datasets.ListHidden = true

	var mutex sync.Mutex
	var results []*scrapper.TableMetricsRow

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(8)

	for {
		dataset, err := datasets.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			if errIsNotFound(err) || errIsAccessDenied(err) {
				continue
			}
			return nil, err
		}

		if isPrivateDataset(dataset.DatasetID) {
			continue
		}

		if e.blocklist.IsBlocked(dataset.DatasetID) {
			log.Infof("dataset %s blacklisted by config", dataset.DatasetID)
			continue
		}

		select {
		case <-groupCtx.Done():
			return nil, groupCtx.Err()
		default:
		}

		g.Go(func() error {
			var datasetResults []*scrapper.TableMetricsRow
			log.Infof("querying dataset %s", dataset.DatasetID)

			q := fmt.Sprintf(tableMetricsSql, quoteTable(dataset.ProjectID), quoteTable(dataset.DatasetID))
			rows, err := e.queryRows(groupCtx, q)
			if err != nil {
				return err
			}

			for {
				var res map[string]bigquery.Value

				err := rows.Next(&res)

				if errors.Is(err, iterator.Done) {
					break
				}

				if err != nil {
					return err
				}

				var t *time.Time = nil
				if updatedAt, ok := res["updated_at"].(time.Time); ok {
					t = &updatedAt
				} else {
					log.Warnf("error extracting updated_at from %s", res["table"].(string))
				}

				var rowCount *int64
				if f, ok := res["row_count"]; ok {
					if c, ok := f.(int64); ok {
						u := int64(c)
						p := &u
						rowCount = p
					}
				}
				var sizeBytes *int64
				if f, ok := res["size_bytes"]; ok {
					if c, ok := f.(int64); ok {
						u := int64(c)
						p := &u
						sizeBytes = p
					}
				}

				datasetResults = append(datasetResults, &scrapper.TableMetricsRow{
					Database:  res["database"].(string),
					Schema:    res["schema"].(string),
					Table:     res["table"].(string),
					RowCount:  rowCount,
					SizeBytes: sizeBytes,
					UpdatedAt: t,
				})
			}

			mutex.Lock()
			defer mutex.Unlock()
			results = append(results, datasetResults...)

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return nil, err
	}

	return collapseShardedMetrics(results)
}

func quoteTable(id string) string {
	return fmt.Sprintf("`%s`", id)
}
