package bigquery

import (
	"context"
	"strings"
	"sync"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/xxjwxc/gowp/workpool"
	"google.golang.org/api/iterator"
)

func (e *BigQueryScrapper) QueryTables(ctx context.Context) ([]*scrapper.TableRow, error) {
	log := logging.
		GetLogger(ctx).
		WithField("executor", "bigquery").
		WithField("project_id", e.conf.ProjectId)

	datasets := e.executor.GetBigQueryClient().Datasets(ctx)
	datasets.ListHidden = true

	var rows []*scrapper.TableRow
	var mutex sync.Mutex

	pool := workpool.New(50)

	numTablesTotal := 0

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

		log = log.WithField("dataset", dataset.DatasetID)

		datasetMeta, err := e.executor.GetBigQueryClient().Dataset(dataset.DatasetID).Metadata(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get dataset metadata")
		}
		datasetTags := labelsToTags(datasetMeta.Labels)

		numTables := 0
		tables := e.executor.GetBigQueryClient().Dataset(dataset.DatasetID).Tables(ctx)

		// Collect table IDs
		tableIds := make([]string, 0)
		for {
			table, err := tables.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				if errIsNotFound(err) || errIsAccessDenied(err) {
					continue
				}
				return nil, err
			}

			tableIds = append(tableIds, table.TableID)
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

			pool.Do(func() error {
				log = log.WithField("table", tableId)
				log.Debugf("processing table %s", tableId)

				tableMeta, err := e.executor.GetBigQueryClient().Dataset(dataset.DatasetID).Table(tableId).Metadata(ctx)
				if err != nil {
					if errIsNotFound(err) || errIsAccessDenied(err) {
						return nil
					}
					return err
				}

				var tableTags []*scrapper.Tag = datasetTags
				tableTags = append(tableTags, labelsToTags(tableMeta.Labels)...)

				mutex.Lock()
				defer mutex.Unlock()

				rows = append(rows, &scrapper.TableRow{
					Instance:    "",
					Database:    e.conf.ProjectId,
					Schema:      dataset.DatasetID,
					Table:       tableAlias,
					TableType:   string(tableMeta.Type),
					Description: lo.EmptyableToPtr(tableMeta.Description),
				})

				return nil
			})
		}

		log.Infof("found %d tables, %d total so far", numTables, numTablesTotal)
		numTablesTotal += numTables
	}

	if err := pool.Wait(); err != nil {
		return nil, err
	}

	return rows, nil
}
