package bigquery

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/xxjwxc/gowp/workpool"
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
		{REGION}INFORMATION_SCHEMA.TABLES
	WHERE table_catalog IS NOT NULL AND table_schema IS NOT NULL AND table_name IS NOT NULL AND ddl IS NOT NULL
	`
)

func (e *BigQueryScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	sqlDefs, err := e.querySqlDefinitionsSql(ctx)
	if errIsAccessDenied(err) {
		logging.GetLogger(ctx).Warn("access denied, falling back to API")
		return e.querySqlDefinitionsApi(ctx)
	}
	return sqlDefs, err
}

func (e *BigQueryScrapper) querySqlDefinitionsApi(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	log := logging.
		GetLogger(ctx).
		WithField("executor", "bigquery").
		WithField("project_id", e.conf.ProjectId)

	datasets := e.executor.GetBigQueryClient().Datasets(ctx)
	datasets.ListHidden = true

	var rows []*scrapper.SqlDefinitionRow

	pool := workpool.New(50)

	numTablesTotal := 0
	for {
		dataset, err := datasets.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			if errIsNotFound(err) {
				log.Infof("dataset %s not found", dataset.DatasetID)
				continue
			}
			if errIsAccessDenied(err) {
				log.Infof("dataset %s access denied", dataset.DatasetID)
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

		tables := e.executor.GetBigQueryClient().Dataset(dataset.DatasetID).Tables(ctx)
		numTables := 0

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

			pool.Do(func() error {
				meta, err := e.executor.GetBigQueryClient().Dataset(dataset.DatasetID).Table(tableId).Metadata(ctx)

				if err != nil {
					if errIsNotFound(err) || errIsAccessDenied(err) {
						return nil
					}
					return err
				}

				if meta.MaterializedView != nil {
					if meta.MaterializedView.Query != "" {
						rows = append(rows, &scrapper.SqlDefinitionRow{
							Database: e.conf.ProjectId,
							Schema:   dataset.DatasetID,
							Table:    tableAlias,
							Sql:      meta.MaterializedView.Query,
							IsView:   false,
						})
					}
				}

				if meta.ViewQuery != "" {
					rows = append(rows, &scrapper.SqlDefinitionRow{
						Database: e.conf.ProjectId,
						Schema:   dataset.DatasetID,
						Table:    tableAlias,
						Sql:      meta.ViewQuery,
						IsView:   true,
					})
				}

				return nil
			})
		}

		numTablesTotal += numTables
		log.Infof("found %d tables, %d total so far", numTables, numTablesTotal)
	}

	if err := pool.Wait(); err != nil {
		return nil, err
	}

	return collapseShardedSqlDefinitions(rows)
}

func (e *BigQueryScrapper) querySqlDefinitionsSql(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	log := logging.GetLogger(ctx)

	q := sqlDefinitionsSql
	if e.conf.Region == "" {
		q = strings.ReplaceAll(q, "{REGION}", "")
	} else {
		q = strings.ReplaceAll(q, "{REGION}", fmt.Sprintf("region-%[1]s.", e.conf.Region))
	}

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

		if e.blocklist.IsBlocked(dataset) {
			log.Debugf("dataset %s blacklisted by config", dataset)
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

	return collapseShardedSqlDefinitions(results)
}
