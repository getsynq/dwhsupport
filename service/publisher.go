package service

import (
	"context"
	"time"

	ingestdwhv1 "github.com/getsynq/api/ingest/dwh/v1"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Publisher struct {
	dwhServiceClient ingestdwhv1.DwhServiceClient
}

func NewPublisher(dwhServiceClient ingestdwhv1.DwhServiceClient) *Publisher {
	return &Publisher{
		dwhServiceClient: dwhServiceClient,
	}
}

func (f *Publisher) PublishCatalog(ctx context.Context, connectionId string, stateAt time.Time, tableRows []*scrapper.TableRow, catalogRows []*scrapper.CatalogColumnRow, sqlDefinitionRows []*scrapper.SqlDefinitionRow) error {

	uploadId := uuid.NewString()

	tableInfo := map[scrapper.DwhFqn]*ingestdwhv1.TableInfo{}
	getOrCreate := func(fqn scrapper.DwhFqn) *ingestdwhv1.TableInfo {
		if info, ok := tableInfo[fqn]; ok {
			return info
		}
		info := &ingestdwhv1.TableInfo{
			Fqn: fqnToProto(fqn),
		}
		tableInfo[fqn] = info
		return info
	}

	tagsPerFqn := map[scrapper.DwhFqn]map[string]*ingestdwhv1.Tag{}

	for _, tableRow := range tableRows {
		info := getOrCreate(tableRow.TableFqn())
		if tableRow.TableType != "" && info.TableType == "" {
			info.TableType = tableRow.TableType
		}
		if info.Description == nil && tableRow.Description != nil {
			info.Description = lo.ToPtr(*tableRow.Description)
		}
	}

	catalogRowsPerFqn := map[scrapper.DwhFqn][]*scrapper.CatalogColumnRow{}
	sqlDefinitionRowsPerFqn := map[scrapper.DwhFqn][]*scrapper.SqlDefinitionRow{}

	for _, columnRow := range catalogRows {
		fqn := columnRow.TableFqn()
		info := getOrCreate(fqn)
		info.IsView = info.IsView || columnRow.IsView
		if info.Description == nil && columnRow.TableComment != nil {
			info.Description = lo.ToPtr(*columnRow.TableComment)
		}

		for _, tableTag := range columnRow.TableTags {
			if _, ok := tagsPerFqn[fqn]; !ok {
				tagsPerFqn[fqn] = map[string]*ingestdwhv1.Tag{}
			}
			tag := &ingestdwhv1.Tag{
				TagName:  tableTag.TagName,
				TagValue: tableTag.TagValue,
			}
			tagsPerFqn[fqn][tag.String()] = tag
		}

		catalogRowsPerFqn[fqn] = append(catalogRowsPerFqn[fqn], columnRow)
	}

	for _, sqlDefinitionRow := range sqlDefinitionRows {
		fqn := sqlDefinitionRow.TableFqn()
		info := getOrCreate(fqn)
		info.IsView = info.IsView || sqlDefinitionRow.IsView
		sqlDefinitionRowsPerFqn[fqn] = append(sqlDefinitionRowsPerFqn[fqn], sqlDefinitionRow)
	}

	allFqns := lo.Keys(tableInfo)

	for _, fqnsChunk := range lo.Chunk(allFqns, 100) {
		ingestCatalogColumnsRequest := &ingestdwhv1.IngestSchemasRequest{
			ConnectionId: connectionId,
			UploadId:     uploadId,
			StateAt:      timestamppb.New(stateAt),
		}

		for _, fqn := range fqnsChunk {
			ingestCatalogColumnsRequest.Schemas = append(ingestCatalogColumnsRequest.Schemas,
				&ingestdwhv1.Schema{
					Fqn:     fqnToProto(fqn),
					Columns: columnsToProto(catalogRowsPerFqn[fqn]),
				},
			)
		}

		_, err := f.dwhServiceClient.IngestSchemas(ctx, ingestCatalogColumnsRequest)
		if err != nil {
			return errors.Wrap(err, "failed to ingest schemas")
		}
	}

	for _, fqnsChunk := range lo.Chunk(allFqns, 100) {
		ingestSqlDefinitionsRequest := &ingestdwhv1.IngestSqlDefinitionsRequest{
			ConnectionId: connectionId,
			UploadId:     uploadId,
			StateAt:      timestamppb.New(stateAt),
		}

		for _, fqn := range fqnsChunk {
			for _, sqlDefinitionRow := range sqlDefinitionRowsPerFqn[fqn] {
				if len(sqlDefinitionRow.Sql) == 0 {
					continue
				}

				ingestSqlDefinitionsRequest.SqlDefinitions = append(ingestSqlDefinitionsRequest.SqlDefinitions,
					&ingestdwhv1.SqlDefinition{
						Fqn: fqnToProto(fqn),
						Sql: sqlDefinitionRow.Sql,
					},
				)
			}
		}

		if len(ingestSqlDefinitionsRequest.SqlDefinitions) > 0 {
			_, err := f.dwhServiceClient.IngestSqlDefinitions(ctx, ingestSqlDefinitionsRequest)
			if err != nil {
				return errors.Wrap(err, "failed to ingest sql definitions")
			}
		}
	}

	// Always send table information, even if there are no tables
	if len(allFqns) == 0 {
		ingestTableInformationRequest := &ingestdwhv1.IngestTableInformationRequest{
			ConnectionId: connectionId,
			UploadId:     uploadId,
			StateAt:      timestamppb.New(stateAt),
		}
		_, err := f.dwhServiceClient.IngestTableInformation(ctx, ingestTableInformationRequest)
		if err != nil {
			return errors.Wrap(err, "failed to ingest table information")
		}
	} else {
		for _, fqnsChunk := range lo.Chunk(allFqns, 1000) {
			ingestTableInformationRequest := &ingestdwhv1.IngestTableInformationRequest{
				ConnectionId: connectionId,
				UploadId:     uploadId,
				StateAt:      timestamppb.New(stateAt),
			}
			for _, fqn := range fqnsChunk {
				info := tableInfo[fqn]
				info.TableTags = lo.Values(tagsPerFqn[fqn])
				ingestTableInformationRequest.Tables = append(ingestTableInformationRequest.Tables, info)
			}

			if len(ingestTableInformationRequest.Tables) > 0 {
				_, err := f.dwhServiceClient.IngestTableInformation(ctx, ingestTableInformationRequest)
				if err != nil {
					return errors.Wrap(err, "failed to ingest table information")
				}
			}
		}
	}

	return nil
}

func columnsToProto(rows []*scrapper.CatalogColumnRow) []*ingestdwhv1.SchemaColumn {
	var res []*ingestdwhv1.SchemaColumn
	for _, row := range rows {
		res = append(res, &ingestdwhv1.SchemaColumn{
			Name:            row.Column,
			NativeType:      row.Type,
			OrdinalPosition: row.Position,
			Description:     row.Comment,
			ColumnTags:      toTags(row.ColumnTags),
			IsStructColumn:  row.IsStructColumn,
			IsArrayColumn:   row.IsArrayColumn,
			FieldSchemas:    columnFieldsToProto(row.FieldSchemas),
		})
	}
	return res
}

func toTags(tags []*scrapper.Tag) []*ingestdwhv1.Tag {
	var res []*ingestdwhv1.Tag
	for _, tag := range tags {
		res = append(res, &ingestdwhv1.Tag{
			TagName:  tag.TagName,
			TagValue: tag.TagValue,
		})
	}
	return res
}

func columnFieldsToProto(fields []*scrapper.SchemaColumnField) []*ingestdwhv1.SchemaColumnField {
	var res []*ingestdwhv1.SchemaColumnField
	for _, field := range fields {
		res = append(res, &ingestdwhv1.SchemaColumnField{
			Name:            field.Name,
			NativeType:      field.NativeType,
			Description:     field.Description,
			OrdinalPosition: field.OrdinalPosition,
			IsStruct:        field.IsStruct,
			IsRepeated:      field.IsRepeated,
			Fields:          columnFieldsToProto(field.Fields),
		})
	}
	return res
}

func fqnToProto(fqn scrapper.DwhFqn) *ingestdwhv1.Fqn {
	return &ingestdwhv1.Fqn{
		Instance: fqn.Instance,
		Database: fqn.Database,
		Schema:   fqn.Schema,
		Table:    fqn.Table,
	}
}
