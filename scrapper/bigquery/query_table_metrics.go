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
	tableMetricsLegacySql = `
	SELECT
		project_id as database,
		dataset_id as schema,
		table_id as table,
		row_count as row_count,
		size_bytes as size_bytes,
		TIMESTAMP_MILLIS(last_modified_time) as updated_at
	FROM %s.%s.__TABLES__
`

	tableMetricsInfoSchemaSql = `
	SELECT
		t.table_catalog AS database,
		t.table_schema AS schema,
		t.table_name AS ` + "`table`" + `,
		COALESCE(SUM(p.total_rows), 0) AS row_count,
		COALESCE(SUM(p.total_logical_bytes), 0) AS size_bytes,
		MAX(p.last_modified_time) AS updated_at
	FROM %[1]s.%[2]s.INFORMATION_SCHEMA.TABLES AS t
	LEFT JOIN %[1]s.%[2]s.INFORMATION_SCHEMA.PARTITIONS AS p
		ON t.table_catalog = p.table_catalog
		AND t.table_schema = p.table_schema
		AND t.table_name = p.table_name
	GROUP BY 1, 2, 3
`
)

func (e *BigQueryScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	if e.conf.UseInformationSchema {
		return e.queryTableMetricsPerDataset(ctx, tableMetricsInfoSchemaSql, "INFORMATION_SCHEMA")
	}
	return e.queryTableMetricsPerDataset(ctx, tableMetricsLegacySql, "__TABLES__")
}

// QueryTableMetricsV2 uses the BigQuery Tables.Get API instead of SQL queries.
// It requires only bigquery.tables.get + bigquery.tables.list (metadata permissions)
// rather than bigquery.tables.getData needed by __TABLES__.
// Covers all table types including materialized views and has no table count limits.
// Exposed for side-by-side comparison testing against QueryTableMetrics.
func (e *BigQueryScrapper) QueryTableMetricsV2(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	return e.queryTableMetricsApi(ctx)
}

// queryTableMetricsApi collects table metrics via the BigQuery Tables.Get API.
func (e *BigQueryScrapper) queryTableMetricsApi(ctx context.Context) ([]*scrapper.TableMetricsRow, error) {
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

		tables := e.executor.GetBigQueryClient().Dataset(dataset.DatasetID).Tables(groupCtx)
		for {
			table, err := tables.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				if errIsNotFound(err) || errIsAccessDenied(err) {
					break
				}
				return nil, err
			}

			select {
			case <-groupCtx.Done():
				return nil, groupCtx.Err()
			default:
			}

			g.Go(func() error {
				tableMeta, err := withRateLimitRetry(groupCtx, e.rateLimitCfg, func() (*bigquery.TableMetadata, error) {
					return table.Metadata(groupCtx)
				})
				if err != nil {
					if errIsNotFound(err) || errIsAccessDenied(err) {
						return nil
					}
					return err
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
					Table:     table.TableID,
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

func (e *BigQueryScrapper) queryTableMetricsPerDataset(ctx context.Context, sqlTemplate string, source string) ([]*scrapper.TableMetricsRow, error) {
	log := logging.GetLogger(ctx)

	allDatasets, err := e.listDatasets(ctx)
	if err != nil {
		return nil, err
	}

	var mutex sync.Mutex
	var results []*scrapper.TableMetricsRow

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(8)

	for _, dataset := range allDatasets {
		if isPrivateDataset(dataset.DatasetID) {
			continue
		}

		if !e.scope.IsSchemaAccepted(e.conf.ProjectId, dataset.DatasetID) {
			log.Infof("dataset %s excluded by scope filter", dataset.DatasetID)
			continue
		}

		select {
		case <-groupCtx.Done():
			return nil, groupCtx.Err()
		default:
		}

		g.Go(func() error {
			log.Infof("querying dataset %s via %s", dataset.DatasetID, source)

			project := quoteTable(dataset.ProjectID)
			ds := quoteTable(dataset.DatasetID)
			q := fmt.Sprintf(sqlTemplate, project, ds)
			rows, err := e.queryRows(groupCtx, q)
			if err != nil {
				if errIsAccessDenied(err) || errIsNotFound(err) {
					log.Warnf("skipping dataset %s for metrics (%s): %v", dataset.DatasetID, source, err)
					return nil
				}
				return err
			}

			var datasetResults []*scrapper.TableMetricsRow
			for {
				var res map[string]bigquery.Value

				err := rows.Next(&res)
				if errors.Is(err, iterator.Done) {
					break
				}
				if err != nil {
					return err
				}

				var t *time.Time
				if updatedAt, ok := res["updated_at"].(time.Time); ok {
					t = &updatedAt
				} else {
					log.Warnf("error extracting updated_at from %s", res["table"].(string))
				}

				var rowCount *int64
				if f, ok := res["row_count"]; ok {
					if c, ok := f.(int64); ok {
						rowCount = &c
					}
				}
				var sizeBytes *int64
				if f, ok := res["size_bytes"]; ok {
					if c, ok := f.(int64); ok {
						sizeBytes = &c
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

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return collapseShardedMetrics(results)
}

func quoteTable(id string) string {
	return fmt.Sprintf("`%s`", id)
}
