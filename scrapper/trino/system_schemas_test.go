package trino

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// sqlExcludesSchemas matches a statement that leaves every one of schemas out.
// The fake cannot evaluate a WHERE clause, so it answers a statement that
// excludes them with the user schema only, and any other with the system schemas
// too, the way a catalog over that database answers.
func sqlExcludesSchemas(match func(string) bool, schemas ...string) func(string) bool {
	return func(sql string) bool {
		if !match(sql) {
			return false
		}
		for _, schema := range schemas {
			if !strings.Contains(sql, "'"+schema+"'") {
				return false
			}
		}
		return true
	}
}

func TestSystemSchemasOfConnectorAreNotListed(t *testing.T) {
	ctx := context.Background()
	tables := sqlContains("FROM mssqlcat.information_schema.tables t")
	columns := sqlContains("FROM mssqlcat.information_schema.tables t\nJOIN mssqlcat.information_schema.columns")
	schemas := sqlContains("FROM mssqlcat.information_schema.schemata")
	schemataColumns := []fakeColumn{{"database", "varchar"}, {"schema", "varchar"}}

	fake := &fakeTrino{t: t, routes: []fakeRoute{
		catalogsRoute(
			[]any{"mssqlcat", "mssqlcat", "sqlserver"},
			[]any{"oraclecat", "oraclecat", "oracle"},
		),
		{
			match:   sqlExcludesSchemas(columns, "sys", "db_owner", "guest"),
			columns: catalogColumns,
			rows:    [][]any{{"mssqlcat", "dbo", "orders", "id", 1, "bigint", nil, ""}},
		},
		{
			match:   columns,
			columns: catalogColumns,
			rows: [][]any{
				{"mssqlcat", "dbo", "orders", "id", 1, "bigint", nil, ""},
				{"mssqlcat", "sys", "objects", "name", 1, "varchar", nil, ""},
			},
		},
		{
			match:   sqlExcludesSchemas(tables, "sys", "db_owner", "guest"),
			columns: tableColumns,
			rows:    [][]any{{"mssqlcat", "dbo", "orders", "BASE TABLE", "", true, false}},
		},
		{
			match:   tables,
			columns: tableColumns,
			rows: [][]any{
				{"mssqlcat", "dbo", "orders", "BASE TABLE", "", true, false},
				{"mssqlcat", "sys", "objects", "VIEW", "", false, true},
			},
		},
		{
			match:   sqlExcludesSchemas(schemas, "sys", "db_owner", "guest"),
			columns: schemataColumns,
			rows:    [][]any{{"mssqlcat", "dbo"}},
		},
		{
			match:   schemas,
			columns: schemataColumns,
			rows:    [][]any{{"mssqlcat", "dbo"}, {"mssqlcat", "sys"}, {"mssqlcat", "db_owner"}, {"mssqlcat", "guest"}},
		},
	}}
	scr := newFakeTrinoScrapper(t, ctx, fake, TrinoScrapperConf{Catalogs: []string{"mssqlcat"}})

	tableRows, err := scr.QueryTables(ctx)
	require.NoError(t, err)
	require.Len(t, tableRows, 1)
	require.Equal(t, "dbo", tableRows[0].Schema)

	columnRows, err := scr.QueryCatalog(ctx)
	require.NoError(t, err)
	require.Len(t, columnRows, 1)
	require.Equal(t, "dbo", columnRows[0].Schema)

	schemaRows, err := scr.QuerySchemas(ctx)
	require.NoError(t, err)
	require.Len(t, schemaRows, 1)
	require.Equal(t, "dbo", schemaRows[0].Schema)
}
