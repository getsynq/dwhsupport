package bigquery

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

func (e *BigQueryScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	log := logging.
		GetLogger(ctx).
		WithField("executor", "bigquery").
		WithField("project_id", e.conf.ProjectId)

	datasets := e.executor.GetBigQueryClient().Datasets(ctx)
	datasets.ListHidden = true

	var rows []*scrapper.CatalogColumnRow
	var mutex sync.Mutex

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(50)

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

		datasetMeta, err := e.executor.GetBigQueryClient().Dataset(dataset.DatasetID).Metadata(groupCtx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get dataset metadata")
		}
		datasetTags := labelsToTags(datasetMeta.Labels)

		numTables := 0
		tables := e.executor.GetBigQueryClient().Dataset(dataset.DatasetID).Tables(groupCtx)

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

			select {
			case <-groupCtx.Done():
				return nil, groupCtx.Err()
			default:
			}

			g.Go(func() error {
				tableMeta, err := e.executor.GetBigQueryClient().Dataset(dataset.DatasetID).Table(tableId).Metadata(groupCtx)
				if err != nil {
					if errIsNotFound(err) || errIsAccessDenied(err) {
						return nil
					}
					return err
				}

				var tableTags []*scrapper.Tag = datasetTags
				tableTags = append(tableTags, labelsToTags(tableMeta.Labels)...)

				isView := tableMeta.Type == bigquery.ViewTable || tableMeta.Type == bigquery.MaterializedView

				mutex.Lock()
				defer mutex.Unlock()

				for i, field := range tableMeta.Schema {
					rows = append(rows, &scrapper.CatalogColumnRow{
						Database:       e.conf.ProjectId,
						Schema:         dataset.DatasetID,
						Table:          tableAlias,
						IsView:         isView,
						Column:         field.Name,
						Type:           string(field.Type),
						Position:       int32(i + 1),
						Comment:        lo.EmptyableToPtr(field.Description),
						TableComment:   lo.EmptyableToPtr(tableMeta.Description),
						TableTags:      tableTags,
						IsStructColumn: field.Type == bigquery.RecordFieldType || field.Type == bigquery.JSONFieldType,
						IsArrayColumn:  field.Repeated,
						FieldSchemas:   toFieldSchemas(field.Schema),
					})
				}

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

func labelsToTags(labels map[string]string) []*scrapper.Tag {
	var res []*scrapper.Tag
	for k, v := range labels {
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if k == "" {
			continue
		}
		res = append(res, &scrapper.Tag{
			TagName:  fmt.Sprintf("label.%s", k),
			TagValue: v,
		})
	}
	return res
}

func toFieldSchemas(schema bigquery.Schema) []*scrapper.SchemaColumnField {
	if len(schema) == 0 {
		return nil
	}
	var schemaColumnFields []*scrapper.SchemaColumnField
	for i, fieldSchema := range schema {
		schemaColumnField := &scrapper.SchemaColumnField{
			Name:            strings.ToLower(fieldSchema.Name),
			HumanName:       fieldSchema.Name,
			NativeType:      string(fieldSchema.Type),
			OrdinalPosition: int32(i + 1),
			IsStruct:        fieldSchema.Type == bigquery.RecordFieldType || fieldSchema.Type == bigquery.JSONFieldType,
			IsRepeated:      fieldSchema.Repeated,
			Fields:          toFieldSchemas(fieldSchema.Schema),
		}
		schemaColumnFields = append(schemaColumnFields, schemaColumnField)
	}

	return schemaColumnFields
}
