package querybuilder

import (
	"testing"

	. "github.com/getsynq/dwhsupport/sqldialect"
	"github.com/gkampitakis/go-snaps/snaps"
)

func TestMetaQueryBuilderSnapshot(t *testing.T) {
	testCases := []struct {
		name      string
		table     TableExpr
		shouldErr bool
	}{
		{
			name:      "StandardTable",
			table:     TableFqn("my_project", "my_schema", "my_table"),
			shouldErr: false,
		},
		{
			name:      "TableWithUppercase",
			table:     TableFqn("MyProject", "MySchema", "MyTable"),
			shouldErr: false,
		},
		{
			name:      "TableWithUnderscores",
			table:     TableFqn("my_project_123", "my_schema_456", "my_table_789"),
			shouldErr: false,
		},
		{
			name:      "TableWithHyphens",
			table:     TableFqn("my-project", "my-schema", "my-table"),
			shouldErr: false,
		},
		{
			name:      "TableWithSpecialChars",
			table:     TableFqn("project.with.dots", "schema-with-hyphens", "table_with_underscores"),
			shouldErr: false,
		},
		{
			name:      "TableWithMixedCase",
			table:     TableFqn("MyProject_123", "my_SCHEMA", "Table_Name_1"),
			shouldErr: false,
		},
		{
			name:      "ShortNames",
			table:     TableFqn("p", "s", "t"),
			shouldErr: false,
		},
		{
			name: "LongNames",
			table: TableFqn(
				"very_long_project_name_with_many_characters_1234567890",
				"very_long_schema_name_with_many_characters_1234567890",
				"very_long_table_name_with_many_characters_1234567890",
			),
			shouldErr: false,
		},
		{
			name:      "NumericSuffixes",
			table:     TableFqn("project123", "schema456", "table789"),
			shouldErr: false,
		},
		{
			name:      "PrefixedNames",
			table:     TableFqn("dev_project", "test_schema", "staging_table"),
			shouldErr: false,
		},
	}

	dialects := []struct {
		name    string
		dialect Dialect
	}{
		{"postgres", NewPostgresDialect()},
		{"trino", NewTrinoDialect()},
		{"bigquery", NewBigQueryDialect()},
		{"redshift", NewRedshiftDialect()},
		{"snowflake", NewSnowflakeDialect()},
		{"clickhouse", NewClickHouseDialect()},
		{"duckdb", NewDuckDBDialect()},
		{"mysql", NewMySQLDialect()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewMetaQueryBuilder(tc.table)

			for _, d := range dialects {
				t.Run(d.name, func(t *testing.T) {
					query, err := builder.ToSql(d.dialect)

					if tc.shouldErr {
						if err == nil {
							t.Errorf("Expected error but got none")
						}
						// For error cases, snapshot the error message
						snaps.WithConfig(
							snaps.Filename("meta_query_builder"),
						).MatchSnapshot(t, err.Error())
					} else {
						if d.name == "databricks" {
							// Databricks should return an error
							if err == nil {
								t.Errorf("Expected error for databricks but got none")
							}
							snaps.WithConfig(
								snaps.Filename("meta_query_builder"),
							).MatchSnapshot(t, err.Error())
						} else {
							if err != nil {
								t.Fatalf("Unexpected error: %v", err)
							}
							snaps.WithConfig(
								snaps.Filename("meta_query_builder"),
							).MatchSnapshot(t, query)
						}
					}
				})
			}
		})
	}
}
