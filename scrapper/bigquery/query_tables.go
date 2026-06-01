package bigquery

import (
	"context"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
)

func (e *BigQueryScrapper) QueryTables(ctx context.Context, opts ...scrapper.QueryTablesOption) ([]*scrapper.TableRow, error) {
	cfg := scrapper.ApplyQueryTablesOptions(opts...)

	log := logging.
		GetLogger(ctx).
		WithField("executor", "bigquery").
		WithField("project_id", e.conf.ProjectId)

	allDatasets, err := e.listDatasets(ctx)
	if err != nil {
		return nil, err
	}

	var rows []*scrapper.TableRow
	var mutex sync.Mutex

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(e.rateLimitCfg.MetadataConcurrency)

	numTablesTotal := 0

	for _, dataset := range allDatasets {
		if isPrivateDataset(dataset.DatasetID) {
			continue
		}

		if !e.scope.IsSchemaAccepted(e.conf.ProjectId, dataset.DatasetID) {
			log.Infof("dataset %s excluded by scope filter", dataset.DatasetID)
			continue
		}

		log = log.WithField("dataset", dataset.DatasetID)

		datasetMeta, err := withRateLimitRetry(groupCtx, e.rateLimitCfg, func(ctx context.Context) (*bigquery.DatasetMetadata, error) {
			return e.executor.GetBigQueryClient().Dataset(dataset.DatasetID).Metadata(ctx)
		})
		if err != nil {
			return nil, e.scrapeError(groupCtx, "dataset.metadata", dataset.DatasetID, "", err)
		}
		datasetTags := labelsToTags(datasetMeta.Labels)

		numTables := 0

		tableIds, err := e.listTableIDs(groupCtx, dataset.DatasetID)
		if err != nil {
			return nil, err
		}

		// Collect sharded tables
		shardLowerBound, shardUpperBound := getValidShardDateRange()
		shardedTables := getShardedTables(tableIds, shardLowerBound, shardUpperBound)
		if len(shardedTables) > 0 {
			log.Infof("found %d sharded tables", len(shardedTables))
		}

		for _, unsafeTableId := range tableIds {
			tableId := unsafeTableId
			tableAlias := tableId
			if shardedTableName, hasShardSuffix := shardedTableName(strings.ToLower(tableId), shardLowerBound, shardUpperBound); hasShardSuffix {
				if shard, shardExists := shardedTables[shardedTableName]; shardExists {
					if shard.LatestShardId != tableId {
						continue
					} else {
						tableAlias = shard.TableName
					}
				}
			}

			numTables++

			select {
			case <-groupCtx.Done():
				return nil, groupCtx.Err()
			default:
			}

			includeConstraints := cfg.IncludeConstraints
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

				var tableTags []*scrapper.Tag = datasetTags
				tableTags = append(tableTags, labelsToTags(tableMeta.Labels)...)

				row := &scrapper.TableRow{
					Instance:    "",
					Database:    e.conf.ProjectId,
					Schema:      dataset.DatasetID,
					Table:       tableAlias,
					TableType:   string(tableMeta.Type),
					Description: lo.EmptyableToPtr(tableMeta.Description),
				}

				if includeConstraints {
					row.Constraints = extractConstraintsFromMeta(e.conf.ProjectId, dataset.DatasetID, tableAlias, tableMeta)
				}

				mutex.Lock()
				defer mutex.Unlock()
				rows = append(rows, row)

				return nil
			})
		}

		log.Infof("found %d tables, %d total so far", numTables, numTablesTotal)
		numTablesTotal += numTables
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return rows, nil
}
