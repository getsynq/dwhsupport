package databricks

import (
	"context"
	"fmt"
	"strings"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"github.com/samber/lo"
)

type Tags struct {
	CatalogName string `db:"catalog_name" json:"catalog_name"`
	SchemaName  string `db:"schema_name"  json:"schema_name"`
	TableName   string `db:"table_name"   json:"table_name"`
	ColumnName  string `db:"column_name"  json:"column_name"`
	TagName     string `db:"tag_name"     json:"tag_name"`
	TagValue    string `db:"tag_value"    json:"tag_value"`
}

func (e *DatabricksScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	log := logging.GetLogger(ctx)
	var res []*scrapper.CatalogColumnRow

	catalogs, err := e.client.Catalogs.ListAll(ctx, servicecatalog.ListCatalogsRequest{})
	if err != nil {
		return nil, err
	}

	tablesFound := 0

	for _, catalogInfo := range catalogs {
		if e.isIgnoredCatalog(catalogInfo) {
			continue
		}
		if !e.scope.IsDatabaseAccepted(catalogInfo.Name) {
			log.Infof("catalog %s excluded by scope filter", catalogInfo.Name)
			continue
		}

		tagsLookup, err := e.createTagsLookup(ctx, catalogInfo.Name)
		schemas, err := e.client.Schemas.ListAll(ctx, servicecatalog.ListSchemasRequest{CatalogName: catalogInfo.Name})
		if err != nil {
			return nil, err
		}
		for _, schemaInfo := range schemas {
			if schemaInfo.Name == "information_schema" {
				continue
			}
			if !e.scope.IsSchemaAccepted(catalogInfo.Name, schemaInfo.Name) {
				log.Infof("schema %s.%s excluded by scope filter", catalogInfo.Name, schemaInfo.Name)
				continue
			}

			tables, err := e.client.Tables.ListAll(
				ctx,
				servicecatalog.ListTablesRequest{CatalogName: catalogInfo.Name, SchemaName: schemaInfo.Name, OmitProperties: true},
			)
			if err != nil {
				return nil, err
			}

			tablesFound += len(tables)
			log.Infof("Found %d tables in catalog '%s' schema '%s', %d total", len(tables), catalogInfo.Name, schemaInfo.Name, tablesFound)

			for _, tableInfo := range tables {
				if !e.scope.IsObjectAccepted(catalogInfo.Name, schemaInfo.Name, tableInfo.Name) {
					log.Infof("table %s.%s.%s excluded by scope filter", catalogInfo.Name, schemaInfo.Name, tableInfo.Name)
					continue
				}
				tableTags := getTableTags(tagsLookup, tableInfo.CatalogName, tableInfo.SchemaName, tableInfo.Name)
				for _, columnInfo := range tableInfo.Columns {
					catalogRow := &scrapper.CatalogColumnRow{
						Instance: e.conf.WorkspaceUrl,
						Database: tableInfo.CatalogName,
						Schema:   tableInfo.SchemaName,
						Table:    tableInfo.Name,
						IsView: tableInfo.TableType == servicecatalog.TableTypeMaterializedView ||
							tableInfo.TableType == servicecatalog.TableTypeView,
						Column:       columnInfo.Name,
						Type:         columnInfo.TypeText,
						Position:     int32(columnInfo.Position + 1),
						Comment:      lo.EmptyableToPtr(columnInfo.Comment),
						TableComment: lo.EmptyableToPtr(tableInfo.Comment),
						TableTags:    tableTags,
						ColumnTags:   getColumnTags(tagsLookup, tableInfo.CatalogName, tableInfo.SchemaName, tableInfo.Name, columnInfo.Name),
					}

					res = append(res, catalogRow)
				}
			}
		}
	}

	return res, nil
}

func (e *DatabricksScrapper) queryTags(ctx context.Context, executor *dwhexecdatabricks.DatabricksExecutor, catalog, informationSchemaTable string) ([]*Tags, error) {
	var tags []*Tags
	err := executor.Select(ctx, &tags, fmt.Sprintf("SELECT * FROM `%s`.information_schema.%s", catalog, informationSchemaTable))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to fetch tags from %s.INFORMATION_SCHEMA.%s", catalog, informationSchemaTable)
	}
	var res = make([]*Tags, 0, len(tags))
	for _, tag := range tags {
		tag.TagName = strings.TrimSpace(tag.TagName)
		tag.TagValue = strings.TrimSpace(tag.TagValue)
		if tag.TagName == "" {
			continue
		}
		res = append(res, tag)
	}
	return res, nil
}

func (e *DatabricksScrapper) createTagsLookup(ctx context.Context, catalogName string) (map[string][]*Tags, error) {
	tagsLookup := make(map[string][]*Tags)
	if e.conf.FetchTableTags {
		databricksExecutor, err := e.lazyExecutor.Get()
		if err != nil {
			return nil, err
		}

		catalogTags, err := e.queryTags(ctx, databricksExecutor, catalogName, "CATALOG_TAGS")
		if err != nil {
			return nil, err
		}
		schemaTags, err := e.queryTags(ctx, databricksExecutor, catalogName, "SCHEMA_TAGS")
		if err != nil {
			return nil, err
		}
		tableTags, err := e.queryTags(ctx, databricksExecutor, catalogName, "TABLE_TAGS")
		if err != nil {
			return nil, err
		}
		columnTags, err := e.queryTags(ctx, databricksExecutor, catalogName, "COLUMN_TAGS")
		if err != nil {
			return nil, err
		}

		for _, tag := range catalogTags {
			key := tag.CatalogName
			tagsLookup[key] = append(tagsLookup[key], tag)
		}
		for _, tag := range schemaTags {
			key := fmt.Sprintf("%s.%s", tag.CatalogName, tag.SchemaName)
			tagsLookup[key] = append(tagsLookup[key], tag)
		}
		for _, tag := range tableTags {
			key := fmt.Sprintf("%s.%s.%s", tag.CatalogName, tag.SchemaName, tag.TableName)
			tagsLookup[key] = append(tagsLookup[key], tag)
		}
		for _, tag := range columnTags {
			key := fmt.Sprintf("%s.%s.%s.%s", tag.CatalogName, tag.SchemaName, tag.TableName, tag.ColumnName)
			tagsLookup[key] = append(tagsLookup[key], tag)
		}
	}
	return tagsLookup, nil
}

func getTableTags(lookup map[string][]*Tags, catalogName string, schemaName string, tableName string) []*scrapper.Tag {
	var res []*Tags
	for _, tags := range lookup[catalogName] {
		res = append(res, tags)
	}
	for _, tags := range lookup[fmt.Sprintf("%s.%s", catalogName, schemaName)] {
		res = append(res, tags)
	}
	for _, tags := range lookup[fmt.Sprintf("%s.%s.%s", catalogName, schemaName, tableName)] {
		res = append(res, tags)
	}

	var tableTags []*scrapper.Tag
	for _, rawTableTag := range res {
		tableTags = append(tableTags, &scrapper.Tag{
			TagName:  fmt.Sprintf("tag.%s", rawTableTag.TagName),
			TagValue: rawTableTag.TagValue,
		})
	}

	return tableTags
}

func getColumnTags(lookup map[string][]*Tags, catalogName string, schemaName string, tableName string, columnName string) []*scrapper.Tag {
	var tableTags []*scrapper.Tag
	for _, rawTableTag := range lookup[fmt.Sprintf("%s.%s.%s.%s", catalogName, schemaName, tableName, columnName)] {
		tableTags = append(tableTags, &scrapper.Tag{
			TagName:  fmt.Sprintf("tag.%s", rawTableTag.TagName),
			TagValue: rawTableTag.TagValue,
		})
	}

	return tableTags
}
