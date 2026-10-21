package bigquery

import (
	"context"
	"strings"
	"sync"
	"time"

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
	reads := e.newDatasetReads(ctx, scrapper.OmittedObjects)

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
			if err := reads.datasetFailed(groupCtx, "dataset.metadata", dataset.DatasetID, err); err != nil {
				return nil, err
			}
			continue
		}
		datasetLabels := datasetMeta.Labels

		numTables := 0

		tableIds, err := e.listTableIDs(groupCtx, dataset.DatasetID)
		if err != nil {
			if err := reads.datasetFailed(groupCtx, "tables.list", dataset.DatasetID, err); err != nil {
				return nil, err
			}
			continue
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
					return reads.tableFailed(groupCtx, dataset.DatasetID, tableId, tableAlias, err)
				}
				reads.markRead()

				tableTags := tableLabelTags(datasetLabels, tableMeta.Labels)

				row := &scrapper.TableRow{
					Instance:    "",
					Database:    e.conf.ProjectId,
					Schema:      dataset.DatasetID,
					Table:       tableAlias,
					TableType:   string(tableMeta.Type),
					Description: lo.EmptyableToPtr(tableMeta.Description),
					Tags:        tableTags,
					CreatedAt:   scrapper.UTCTimePtr(&tableMeta.CreationTime),
					CopyOf:      copyOf(tableMeta, shardLowerBound, shardUpperBound),
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

		if numTables == 0 {
			reads.markRead()
		}
		log.Infof("found %d tables, %d total so far", numTables, numTablesTotal)
		numTablesTotal += numTables
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return rows, reads.result()
}

// copyOf names the base table of a table snapshot or a table clone, nil for
// any other table. The base is named the way QueryTables names it: a shard
// by the name of its sharded table, which the scan reports in its place.
func copyOf(meta *bigquery.TableMetadata, shardLowerBound, shardUpperBound time.Time) *scrapper.TableCopyOf {
	var kind scrapper.TableCopyKind
	var base *bigquery.Table
	var at time.Time
	switch {
	case meta.SnapshotDefinition != nil:
		kind, base, at = scrapper.TableCopySnapshot, meta.SnapshotDefinition.BaseTableReference, meta.SnapshotDefinition.SnapshotTime
	case meta.CloneDefinition != nil:
		kind, base, at = scrapper.TableCopyClone, meta.CloneDefinition.BaseTableReference, meta.CloneDefinition.CloneTime
	default:
		return nil
	}
	if base == nil || base.ProjectID == "" || base.DatasetID == "" || base.TableID == "" {
		return nil
	}
	table := base.TableID
	if sharded, ok := shardedTableName(table, shardLowerBound, shardUpperBound); ok {
		table = sharded
	}
	return &scrapper.TableCopyOf{
		Kind:     kind,
		Database: base.ProjectID,
		Schema:   base.DatasetID,
		Table:    table,
		At:       scrapper.UTCTimePtr(&at),
	}
}
