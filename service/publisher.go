package service

import (
	"context"
	"strings"
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

	tableInfo := map[scrapper.DwhFqn]*ingestdwhv1.ObjectInformation{}
	getOrCreate := func(fqn scrapper.DwhFqn) *ingestdwhv1.ObjectInformation {
		if info, ok := tableInfo[fqn]; ok {
			return info
		}
		info := &ingestdwhv1.ObjectInformation{
			Fqn: fqnToProto(fqn),
		}
		tableInfo[fqn] = info
		return info
	}

	tagsPerFqn := map[scrapper.DwhFqn]map[string]*ingestdwhv1.Tag{}

	for _, tableRow := range tableRows {
		fqn := tableRow.TableFqn()
		if f.RejectedFqn(fqn) {
			continue
		}
		info := getOrCreate(fqn)
		if tableRow.TableType != "" && info.ObjectNativeType == "" {
			info.ObjectNativeType = tableRow.TableType
		}
		if info.Description == nil && tableRow.Description != nil {
			info.Description = lo.ToPtr(*tableRow.Description)
		}
	}

	catalogRowsPerFqn := map[scrapper.DwhFqn][]*scrapper.CatalogColumnRow{}
	sqlDefinitionRowsPerFqn := map[scrapper.DwhFqn][]*scrapper.SqlDefinitionRow{}

	for _, columnRow := range catalogRows {
		fqn := columnRow.TableFqn()
		if f.RejectedFqn(fqn) {
			continue
		}
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
		if f.RejectedFqn(fqn) {
			continue
		}
		info := getOrCreate(fqn)
		info.IsView = info.IsView || sqlDefinitionRow.IsView
		sqlDefinitionRowsPerFqn[fqn] = append(sqlDefinitionRowsPerFqn[fqn], sqlDefinitionRow)
	}

	allFqns := lo.Keys(tableInfo)

	ingestCatalogColumnsRequest := &ingestdwhv1.IngestSchemasRequest{
		ConnectionId: connectionId,
		UploadId:     uploadId,
		StateAt:      timestamppb.New(stateAt),
	}

	for _, fqn := range allFqns {
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

	ingestSqlDefinitionsRequest := &ingestdwhv1.IngestSqlDefinitionsRequest{
		ConnectionId: connectionId,
		UploadId:     uploadId,
		StateAt:      timestamppb.New(stateAt),
	}

	for _, fqn := range allFqns {
		for _, sqlDefinitionRow := range sqlDefinitionRowsPerFqn[fqn] {
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

	ingestTableInformationRequest := &ingestdwhv1.IngestObjectInformationRequest{
		ConnectionId: connectionId,
		UploadId:     uploadId,
		StateAt:      timestamppb.New(stateAt),
	}
	for _, fqn := range allFqns {
		info := tableInfo[fqn]
		info.Tags = lo.Values(tagsPerFqn[fqn])
		info.IsTable = !info.IsView
		ingestTableInformationRequest.Objects = append(ingestTableInformationRequest.Objects, info)
	}

	_, err = f.dwhServiceClient.IngestObjectInformation(ctx, ingestTableInformationRequest)
	if err != nil {
		return errors.Wrap(err, "failed to ingest table information")
	}

	return nil
}

func (f *Publisher) PublishMetrics(ctx context.Context, connectionId string, stateAt time.Time, rows []*scrapper.TableMetricsRow) error {
	if len(rows) == 0 {
		return nil
	}

	uploadId := uuid.NewString()

	for _, rowsChunk := range lo.Chunk(rows, 1000) {
		ingestObjectMetricsRequest := &ingestdwhv1.IngestObjectMetricsRequest{
			ConnectionId: connectionId,
			UploadId:     uploadId,
			StateAt:      timestamppb.New(stateAt),
		}
		for _, tableMetricsRow := range rowsChunk {
			if f.RejectedFqn(tableMetricsRow.TableFqn()) {
				continue
			}

			var updatedAt *timestamppb.Timestamp
			if tableMetricsRow.UpdatedAt != nil {
				updatedAt = timestamppb.New(*tableMetricsRow.UpdatedAt)
			}
			ingestObjectMetricsRequest.Metrics = append(ingestObjectMetricsRequest.Metrics, &ingestdwhv1.ObjectMetrics{
				Fqn:       fqnToProto(tableMetricsRow.TableFqn()),
				RowCount:  tableMetricsRow.RowCount,
				UpdatedAt: updatedAt,
				SizeBytes: tableMetricsRow.SizeBytes,
			})
		}
		_, err := f.dwhServiceClient.IngestObjectMetrics(ctx, ingestObjectMetricsRequest)
		if err != nil {
			return errors.Wrap(err, "failed to ingest table information")
		}
	}

	return nil
}

func (f *Publisher) RejectedFqn(fqn scrapper.DwhFqn) bool {
	if strings.ToLower(fqn.SchemaName) == "information_schema" {
		return true
	}
	return false
}

func columnsToProto(rows []*scrapper.CatalogColumnRow) []*ingestdwhv1.SchemaColumn {
	var res []*ingestdwhv1.SchemaColumn
	for _, row := range rows {
		res = append(res, &ingestdwhv1.SchemaColumn{
			Name:            row.Column,
			NativeType:      row.Type,
			OrdinalPosition: row.Position,
			Description:     row.Comment,
			Tags:            toTags(row.ColumnTags),
			IsStruct:        row.IsStructColumn,
			IsRepeated:      row.IsArrayColumn,
			Fields:          columnFieldsToProto(row.FieldSchemas),
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
		InstanceName: fqn.InstanceName,
		DatabaseName: fqn.DatabaseName,
		SchemaName:   fqn.SchemaName,
		ObjectName:   fqn.ObjectName,
	}
}
