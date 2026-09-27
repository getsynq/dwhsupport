package databricks

import (
	"strings"
	"testing"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/stretchr/testify/assert"
)

// tableInfo builds a listing row the way Unity Catalog returns one: FullName is
// the three parts joined with dots, unquoted.
func tableInfo(catalog, schema, table string) servicecatalog.TableInfo {
	return servicecatalog.TableInfo{
		CatalogName: catalog,
		SchemaName:  schema,
		Name:        table,
		FullName:    strings.Join([]string{catalog, schema, table}, "."),
	}
}

func TestAnalyzeTableStatementQuotesEachPart(t *testing.T) {
	cases := []struct {
		name   string
		table  servicecatalog.TableInfo
		noScan string
		want   string
	}{
		{
			name:   "plain identifiers",
			table:  tableInfo("my_catalog", "my_schema", "my_table"),
			noScan: " NOSCAN",
			want:   "ANALYZE TABLE `my_catalog`.`my_schema`.`my_table` COMPUTE STATISTICS NOSCAN",
		},
		{
			// Databricks: "[INVALID_IDENTIFIER] The unquoted identifier my-schema is invalid
			// and must be back quoted".
			name:   "schema with a dash",
			table:  tableInfo("my_catalog", "my-schema", "my_table"),
			noScan: " NOSCAN",
			want:   "ANALYZE TABLE `my_catalog`.`my-schema`.`my_table` COMPUTE STATISTICS NOSCAN",
		},
		{
			// The shape test runs leave behind: a timestamp and a uuid, whose digits after a
			// dash Databricks reads as a number ("Syntax error at or near '6216'").
			name:   "schema suffixed with a uuid",
			table:  tableInfo("test_catalog", "temp_schema_20260922t204518073z_7a9b92e6-6216-4375-ac11-87cbbf6131d9", "customer"),
			noScan: "",
			want:   "ANALYZE TABLE `test_catalog`.`temp_schema_20260922t204518073z_7a9b92e6-6216-4375-ac11-87cbbf6131d9`.`customer` COMPUTE STATISTICS",
		},
		{
			name:   "catalog and table with dashes",
			table:  tableInfo("my-catalog", "my_schema", "my-table"),
			noScan: " NOSCAN",
			want:   "ANALYZE TABLE `my-catalog`.`my_schema`.`my-table` COMPUTE STATISTICS NOSCAN",
		},
		{
			name:   "reserved word",
			table:  tableInfo("my_catalog", "select", "order"),
			noScan: " NOSCAN",
			want:   "ANALYZE TABLE `my_catalog`.`select`.`order` COMPUTE STATISTICS NOSCAN",
		},
		{
			// FullName cannot tell this table apart from a schema `my` holding `table.x`;
			// the parts can.
			name:   "dot inside a name",
			table:  tableInfo("my_catalog", "my_schema", "my.table"),
			noScan: " NOSCAN",
			want:   "ANALYZE TABLE `my_catalog`.`my_schema`.`my.table` COMPUTE STATISTICS NOSCAN",
		},
		{
			name:   "backtick inside a name is doubled",
			table:  tableInfo("my_catalog", "my_schema", "we`ird"),
			noScan: " NOSCAN",
			want:   "ANALYZE TABLE `my_catalog`.`my_schema`.`we``ird` COMPUTE STATISTICS NOSCAN",
		},
		{
			name:   "space inside a name",
			table:  tableInfo("my_catalog", "my schema", "my_table"),
			noScan: " NOSCAN",
			want:   "ANALYZE TABLE `my_catalog`.`my schema`.`my_table` COMPUTE STATISTICS NOSCAN",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, analyzeTableStatement(tc.table, tc.noScan))
		})
	}
}

// The statements below were already backtick-quoted, but pasted the name
// between the backticks as it came, so a backtick inside one closed the
// identifier early.
func TestShowCreateTableStatementEscapesBackticks(t *testing.T) {
	assert.Equal(t,
		"SHOW CREATE TABLE `my-catalog`.`my_schema`.`we``ird`",
		showCreateTableStatement("my-catalog", "my_schema", "we`ird"),
	)
}

func TestTagsStatementEscapesBackticks(t *testing.T) {
	assert.Equal(t,
		"SELECT * FROM `my-cata``log`.information_schema.TABLE_TAGS",
		tagsStatement("my-cata`log", "TABLE_TAGS"),
	)
}
