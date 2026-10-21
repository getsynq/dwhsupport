package databricks

import (
	"context"
	"fmt"
	"slices"
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
	tablesFound := 0

	// Tags are read once per catalog, on its first schema whose tables listed, so a
	// catalog whose listing is refused costs no statement on the SQL warehouse.
	var tagsCatalog string
	var tagsLookup map[string][]*Tags

	walk := e.walkUnityCatalog(ctx, scrapper.OmittedColumns)
	err := walk.forEachSchema(ctx, servicecatalog.ListTablesRequest{OmitProperties: true},
		func(schemaInfo servicecatalog.SchemaInfo, tables []servicecatalog.TableInfo) error {
			if tagsLookup == nil || tagsCatalog != schemaInfo.CatalogName {
				tagsCatalog, tagsLookup = schemaInfo.CatalogName, e.createTagsLookup(ctx, schemaInfo.CatalogName)
			}

			tablesFound += len(tables)
			log.Infof("Found %d tables in catalog '%s' schema '%s', %d total", len(tables), schemaInfo.CatalogName, schemaInfo.Name, tablesFound)

			for _, tableInfo := range tables {
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
						IsNullable:   columnNullable(columnInfo),
					}

					res = append(res, catalogRow.ResolveNumeric(e.DialectType()))
				}
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return res, walk.result()
}

// columnNullable reads ColumnInfo.Nullable, nil when the response left it out.
// The SDK field is a plain bool, so false alone cannot tell NOT NULL from
// absent; the SDK records a field the response did send as false in
// ForceSendFields, and that is what tells the two apart.
func columnNullable(columnInfo servicecatalog.ColumnInfo) *bool {
	if columnInfo.Nullable || slices.Contains(columnInfo.ForceSendFields, "Nullable") {
		return lo.ToPtr(columnInfo.Nullable)
	}
	return nil
}

func (e *DatabricksScrapper) queryTags(
	ctx context.Context,
	executor *dwhexecdatabricks.DatabricksExecutor,
	catalog, informationSchemaTable string,
) ([]*Tags, error) {
	sql, err := tagsStatement(catalog, informationSchemaTable)
	if err != nil {
		return nil, err
	}
	var tags []*Tags
	err = executor.Select(ctx, &tags, sql)
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

func tagsStatement(catalog, informationSchemaTable string) (string, error) {
	name, err := quotedName(catalog, "information_schema", informationSchemaTable)
	if err != nil {
		return "", err
	}
	return "SELECT * FROM " + name, nil
}

// createTagsLookup reads the tags of a catalog's objects, keyed by the object's dotted
// name. Tags are best-effort: they come from the catalog's information_schema over a
// SQL warehouse, which can be refused, stopped or unreachable while the Unity Catalog
// listing works, and missing tags must not cost the catalog read its tables. A source
// that cannot be read is logged and left out.
func (e *DatabricksScrapper) createTagsLookup(ctx context.Context, catalogName string) map[string][]*Tags {
	tagsLookup := make(map[string][]*Tags)
	if !e.conf.FetchTableTags {
		return tagsLookup
	}
	log := logging.GetLogger(ctx)

	databricksExecutor, err := e.lazyExecutor.Get()
	if err != nil {
		log.WithError(err).Warnf("cannot read the tags of catalog %s, continuing without them", catalogName)
		return tagsLookup
	}

	sources := []struct {
		table string
		key   func(tag *Tags) string
	}{
		{"CATALOG_TAGS", func(tag *Tags) string { return tag.CatalogName }},
		{"SCHEMA_TAGS", func(tag *Tags) string { return fmt.Sprintf("%s.%s", tag.CatalogName, tag.SchemaName) }},
		{"TABLE_TAGS", func(tag *Tags) string { return fmt.Sprintf("%s.%s.%s", tag.CatalogName, tag.SchemaName, tag.TableName) }},
		{"COLUMN_TAGS", func(tag *Tags) string {
			return fmt.Sprintf("%s.%s.%s.%s", tag.CatalogName, tag.SchemaName, tag.TableName, tag.ColumnName)
		}},
	}
	for _, source := range sources {
		tags, err := e.queryTags(ctx, databricksExecutor, catalogName, source.table)
		if err != nil {
			log.WithError(err).Warnf("cannot read %s of catalog %s, continuing without them", source.table, catalogName)
			continue
		}
		for _, tag := range tags {
			key := source.key(tag)
			tagsLookup[key] = append(tagsLookup[key], tag)
		}
	}
	return tagsLookup
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
			TagName:  rawTableTag.TagName,
			TagValue: rawTableTag.TagValue,
			Kind:     scrapper.TagKindLabel,
		})
	}

	return tableTags
}

func getColumnTags(lookup map[string][]*Tags, catalogName string, schemaName string, tableName string, columnName string) []*scrapper.Tag {
	var tableTags []*scrapper.Tag
	for _, rawTableTag := range lookup[fmt.Sprintf("%s.%s.%s.%s", catalogName, schemaName, tableName, columnName)] {
		tableTags = append(tableTags, &scrapper.Tag{
			TagName:  rawTableTag.TagName,
			TagValue: rawTableTag.TagValue,
			Kind:     scrapper.TagKindLabel,
		})
	}

	return tableTags
}
