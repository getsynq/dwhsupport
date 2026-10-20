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
)

func (e *BigQueryScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	log := logging.
		GetLogger(ctx).
		WithField("executor", "bigquery").
		WithField("project_id", e.conf.ProjectId)

	allDatasets, err := e.listDatasets(ctx)
	if err != nil {
		return nil, err
	}

	var rows []*scrapper.CatalogColumnRow
	var mutex sync.Mutex

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(e.rateLimitCfg.MetadataConcurrency)
	reads := e.newDatasetReads(ctx, scrapper.OmittedColumns)

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
		datasetTags := labelsToTags(datasetMeta.Labels)

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

			g.Go(func() error {
				tableMeta, err := withRateLimitRetry(groupCtx, e.rateLimitCfg, func(ctx context.Context) (*bigquery.TableMetadata, error) {
					return e.executor.GetBigQueryClient().Dataset(dataset.DatasetID).Table(tableId).Metadata(ctx)
				})
				if err != nil {
					return reads.tableFailed(groupCtx, dataset.DatasetID, tableId, tableAlias, err)
				}
				reads.markRead()

				var tableTags []*scrapper.Tag = datasetTags
				tableTags = append(tableTags, labelsToTags(tableMeta.Labels)...)

				isView := tableMeta.Type == bigquery.ViewTable || tableMeta.Type == bigquery.MaterializedView

				mutex.Lock()
				defer mutex.Unlock()

				for i, field := range tableMeta.Schema {
					precision, scale, sized := decimalSize(field)
					row := &scrapper.CatalogColumnRow{
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
						IsNullable:     lo.ToPtr(fieldIsNullable(field)),
					}
					if sized {
						row.Precision, row.Scale = &precision, &scale
					}
					rows = append(rows, row.ResolveNumeric(e.DialectType()))
				}

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

// fieldIsNullable mirrors INFORMATION_SCHEMA.COLUMNS.is_nullable: a REQUIRED
// field is NOT NULL, and so is a REPEATED one, since an array is never NULL
// (it is empty instead).
func fieldIsNullable(field *bigquery.FieldSchema) bool {
	return !field.Required && !field.Repeated
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
