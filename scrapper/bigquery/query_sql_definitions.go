package bigquery

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

var (
	sqlDefinitionsSql = `
	select
		table_catalog as database,
		table_schema as schema,
		table_name as table,
		ddl as sql,
		table_type = 'VIEW' OR table_type = 'MATERIALIZED VIEW' as is_view
	from
		{TABLES}
	WHERE table_catalog IS NOT NULL AND table_schema IS NOT NULL AND table_name IS NOT NULL AND ddl IS NOT NULL
	`
)

// datasetQueryConcurrency bounds the INFORMATION_SCHEMA.TABLES queries run at
// once when each dataset is read on its own. Each is a query job, and a
// project runs a limited number of interactive queries at a time.
const datasetQueryConcurrency = 8

// QuerySqlDefinitions reads the DDL of every table, view, snapshot and clone
// from the region's INFORMATION_SCHEMA.TABLES. A role granted only on the
// datasets may not read that, but may read each dataset's own, which has the
// same rows, so a refused region read falls back to reading each dataset, and
// the scrapper remembers the refusal and reads each dataset from then on.
func (e *BigQueryScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	if e.regionTablesRefused.known() {
		return e.querySqlDefinitionsPerDataset(ctx)
	}
	sqlDefs, err := e.querySqlDefinitionsSql(ctx)
	if e.regionTablesRefused.remember(ctx, "TABLES", err) {
		return e.querySqlDefinitionsPerDataset(ctx)
	}
	return sqlDefs, err
}

// querySqlDefinitionsPerDataset reads each dataset's INFORMATION_SCHEMA.TABLES.
// A dataset that refuses even that is read through the tables API, which has
// the queries of views and materialized views but no table DDL, so each table
// read that way is reported as missing its definition to a caller that accepts
// incomplete results.
func (e *BigQueryScrapper) querySqlDefinitionsPerDataset(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	allDatasets, err := e.listDatasets(ctx)
	if err != nil {
		return nil, err
	}

	reads := e.newDatasetReads(ctx, scrapper.OmittedDefinitions)
	var rows []*scrapper.SqlDefinitionRow
	refused := map[string]error{}
	var mutex sync.Mutex

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(datasetQueryConcurrency)
	for _, dataset := range allDatasets {
		datasetId := dataset.DatasetID
		if isPrivateDataset(datasetId) || !e.scope.IsSchemaAccepted(e.conf.ProjectId, datasetId) {
			continue
		}

		g.Go(func() error {
			tables := fmt.Sprintf("`%s`.`%s`.INFORMATION_SCHEMA.TABLES", e.conf.ProjectId, datasetId)
			datasetRows, err := e.readSqlDefinitions(groupCtx, strings.ReplaceAll(sqlDefinitionsSql, "{TABLES}", tables))
			if isRefused(err) {
				mutex.Lock()
				defer mutex.Unlock()
				refused[datasetId] = err
				return nil
			}
			if err != nil {
				return reads.datasetFailed(groupCtx, "INFORMATION_SCHEMA.TABLES", datasetId, err)
			}
			reads.markRead()

			mutex.Lock()
			defer mutex.Unlock()
			rows = append(rows, datasetRows...)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	if len(refused) > 0 {
		datasetIds := make([]string, 0, len(refused))
		for datasetId := range refused {
			datasetIds = append(datasetIds, datasetId)
		}
		sort.Strings(datasetIds)

		apiRows, err := e.querySqlDefinitionsApi(ctx, reads, datasetIds, func(ctx context.Context, datasetId, tableId, table string) {
			reads.degraded(ctx, "INFORMATION_SCHEMA.TABLES", datasetId, tableId, table, errors.Wrap(
				refused[datasetId], "the dataset's INFORMATION_SCHEMA.TABLES was refused, and the tables API has no DDL of a table",
			))
		})
		if err != nil {
			return nil, err
		}
		rows = append(rows, apiRows...)
	}

	collapsed, err := collapseShardedSqlDefinitions(rows)
	if err != nil {
		return nil, err
	}
	return collapsed, reads.result()
}

// querySqlDefinitionsApi reads the queries of the views and materialized views
// of the datasets through the tables API, one table at a time. Tables, snapshots
// and clones have no DDL there: noDefinition is called for each of them, with
// the name the result would have reported it under.
func (e *BigQueryScrapper) querySqlDefinitionsApi(
	ctx context.Context,
	reads *datasetReads,
	datasetIds []string,
	noDefinition func(ctx context.Context, datasetId, tableId, table string),
) ([]*scrapper.SqlDefinitionRow, error) {
	log := logging.
		GetLogger(ctx).
		WithField("executor", "bigquery").
		WithField("project_id", e.conf.ProjectId)

	var rows []*scrapper.SqlDefinitionRow
	var mutex sync.Mutex

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(e.rateLimitCfg.MetadataConcurrency)

	numTablesTotal := 0
	for _, datasetId := range datasetIds {
		log := log.WithField("dataset", datasetId)

		numTables := 0

		tableIds, err := e.listTableIDs(groupCtx, datasetId)
		if err != nil {
			if err := reads.datasetFailed(groupCtx, "tables.list", datasetId, err); err != nil {
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
			if shardedTableName, hasShardSuffix := shardedTableName(tableId, shardLowerBound, shardUpperBound); hasShardSuffix {
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

			g.Go(func() error {
				meta, err := withRateLimitRetry(groupCtx, e.rateLimitCfg, func(ctx context.Context) (*bigquery.TableMetadata, error) {
					return e.executor.GetBigQueryClient().Dataset(datasetId).Table(tableId).Metadata(ctx)
				})
				if err != nil {
					return reads.tableFailed(groupCtx, datasetId, tableId, tableAlias, err)
				}
				reads.markRead()

				if meta.ViewQuery == "" && (meta.MaterializedView == nil || meta.MaterializedView.Query == "") {
					noDefinition(groupCtx, datasetId, tableId, tableAlias)
					return nil
				}

				mutex.Lock()
				defer mutex.Unlock()

				if meta.MaterializedView != nil {
					if meta.MaterializedView.Query != "" {
						rows = append(rows, &scrapper.SqlDefinitionRow{
							Database: e.conf.ProjectId,
							Schema:   datasetId,
							Table:    tableAlias,
							Sql:      meta.MaterializedView.Query,
							IsView:   false,
						})
					}
				}

				if meta.ViewQuery != "" {
					rows = append(rows, &scrapper.SqlDefinitionRow{
						Database: e.conf.ProjectId,
						Schema:   datasetId,
						Table:    tableAlias,
						Sql:      meta.ViewQuery,
						IsView:   true,
					})
				}

				return nil
			})
		}

		if numTables == 0 {
			reads.markRead()
		}
		numTablesTotal += numTables
		log.Infof("found %d tables, %d total so far", numTables, numTablesTotal)
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return rows, nil
}

func (e *BigQueryScrapper) querySqlDefinitionsSql(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	tables := fmt.Sprintf("region-%s.INFORMATION_SCHEMA.TABLES", e.conf.Region)
	results, err := e.readSqlDefinitions(ctx, strings.ReplaceAll(sqlDefinitionsSql, "{TABLES}", tables))
	if err != nil {
		return nil, err
	}
	return collapseShardedSqlDefinitions(results)
}

// readSqlDefinitions runs a definitions query and returns its rows of the
// datasets in scope.
func (e *BigQueryScrapper) readSqlDefinitions(ctx context.Context, q string) ([]*scrapper.SqlDefinitionRow, error) {
	log := logging.GetLogger(ctx)

	rows, err := e.queryRows(ctx, q)
	if err != nil {
		return nil, err
	}

	var results []*scrapper.SqlDefinitionRow
	for {
		var res map[string]bigquery.Value

		err := rows.Next(&res)

		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, err
		}

		dataset := res["schema"].(string)

		if !e.scope.IsSchemaAccepted(e.conf.ProjectId, dataset) {
			log.Debugf("dataset %s excluded by scope filter", dataset)
			continue
		}

		results = append(results, &scrapper.SqlDefinitionRow{
			Database: res["database"].(string),
			Schema:   res["schema"].(string),
			Table:    res["table"].(string),
			IsView:   res["is_view"].(bool),
			Sql:      res["sql"].(string),
		})
	}

	return results, nil
}
