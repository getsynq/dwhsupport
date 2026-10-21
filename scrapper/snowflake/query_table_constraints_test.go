package snowflake

import (
	"context"
	"database/sql/driver"
	"testing"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/require"
)

var showKeysColumns = []string{
	"created_on", "database_name", "schema_name", "table_name", "column_name", "key_sequence", "constraint_name", "rely", "comment",
}

// salesConstraints answers the statements QueryTableConstraints sends the SALES
// database: a primary key, a two-column unique key, no foreign key, and the
// clustering keys information_schema.tables has in its CLUSTERING_KEY column.
func salesConstraints() []fakeRoute {
	return []fakeRoute{
		{
			match:   sqlContains("SHOW PRIMARY KEYS IN DATABASE SALES"),
			columns: showKeysColumns,
			rows:    [][]driver.Value{{nil, "SALES", "PUBLIC", "ORDERS", "ID", int64(1), "PK_ORDERS", "true", nil}},
		},
		{
			match:   sqlContains("SHOW UNIQUE KEYS IN DATABASE SALES"),
			columns: showKeysColumns,
			rows: [][]driver.Value{
				{nil, "SALES", "PUBLIC", "ORDERS", "PRODUCT_ID", int64(2), "UQ_ORDER_PRODUCT", "false", nil},
				{nil, "SALES", "PUBLIC", "ORDERS", "ORDER_NO", int64(1), "UQ_ORDER_PRODUCT", "false", nil},
			},
		},
		{
			match: sqlContains("SHOW IMPORTED KEYS IN DATABASE SALES"),
			columns: []string{
				"pk_database_name", "pk_schema_name", "pk_table_name", "pk_column_name",
				"fk_database_name", "fk_schema_name", "fk_table_name", "fk_column_name", "key_sequence", "fk_name", "rely",
			},
		},
		{
			match:   sqlContains("DESCRIBE TABLE SALES.information_schema.tables"),
			columns: []string{"name", "type"},
			rows:    [][]driver.Value{{"TABLE_SCHEMA", "VARCHAR"}, {"TABLE_NAME", "VARCHAR"}, {"CLUSTERING_KEY", "VARCHAR"}},
		},
		{
			match:   sqlContains("SALES.information_schema.tables", "clustering_key"),
			columns: []string{"TABLE_SCHEMA", "TABLE_NAME", "CLUSTERING_KEY"},
			rows: [][]driver.Value{
				{"PUBLIC", "ORDERS", "LINEAR(CATEGORY, ID)"},
				{"PUBLIC", "EVENTS", `LINEAR(TO_DATE(CREATED_AT), "Region", SUBSTRING(CODE, 1, 3))`},
			},
		},
	}
}

func TestSnowflakeQueryTableConstraints(t *testing.T) {
	ctx := context.Background()
	fake := &fakeSnowflake{t: t, routes: append([]fakeRoute{showDatabases()}, salesConstraints()...)}
	conf := &SnowflakeScrapperConf{SnowflakeConf: dwhexecsnowflake.SnowflakeConf{Account: "ORG-ACCOUNT", Databases: []string{"SALES"}}}
	sc := newFakeSnowflakeScrapper(ctx, fake, conf)

	rows, err := sc.QueryTableConstraints(ctx)
	require.NoError(t, err)

	type key struct{ table, constraintType, name, column, expression string }
	var got []key
	for _, row := range rows {
		require.Equal(t, "ORG-ACCOUNT", row.Instance)
		require.Equal(t, "SALES", row.Database)
		require.Equal(t, "PUBLIC", row.Schema)
		got = append(got, key{row.Table, row.ConstraintType, row.ConstraintName, row.ColumnName, row.ConstraintExpression})
	}

	positions := map[key]int32{}
	for _, row := range rows {
		positions[key{row.Table, row.ConstraintType, row.ConstraintName, row.ColumnName, row.ConstraintExpression}] = row.ColumnPosition
	}
	want := map[key]int32{
		{"ORDERS", scrapper.ConstraintTypePrimaryKey, "PK_ORDERS", "ID", ""}:                               1,
		{"ORDERS", scrapper.ConstraintTypeUniqueIndex, "UQ_ORDER_PRODUCT", "ORDER_NO", ""}:                 1,
		{"ORDERS", scrapper.ConstraintTypeUniqueIndex, "UQ_ORDER_PRODUCT", "PRODUCT_ID", ""}:               2,
		{"ORDERS", scrapper.ConstraintTypeClusterBy, "clustering_key", "CATEGORY", "LINEAR(CATEGORY, ID)"}: 1,
		{"ORDERS", scrapper.ConstraintTypeClusterBy, "clustering_key", "ID", "LINEAR(CATEGORY, ID)"}:       2,
		// An expression of the key is its own entry, a quoted column is named without its quotes.
		{"EVENTS", scrapper.ConstraintTypeClusterBy, "clustering_key", "TO_DATE(CREATED_AT)", `LINEAR(TO_DATE(CREATED_AT), "Region", SUBSTRING(CODE, 1, 3))`}:   1,
		{"EVENTS", scrapper.ConstraintTypeClusterBy, "clustering_key", "Region", `LINEAR(TO_DATE(CREATED_AT), "Region", SUBSTRING(CODE, 1, 3))`}:                2,
		{"EVENTS", scrapper.ConstraintTypeClusterBy, "clustering_key", "SUBSTRING(CODE, 1, 3)", `LINEAR(TO_DATE(CREATED_AT), "Region", SUBSTRING(CODE, 1, 3))`}: 3,
	}
	require.Equal(t, want, positions, "got %v", got)

	for _, row := range rows {
		if row.ConstraintType == scrapper.ConstraintTypeUniqueIndex {
			require.NotNil(t, row.IsEnforced)
			require.False(t, *row.IsEnforced, "RELY false")
		}
	}
}

func TestClusteringKeyEntries(t *testing.T) {
	for _, tc := range []struct {
		key  string
		want []string
	}{
		{"LINEAR(CATEGORY, ID)", []string{"CATEGORY", "ID"}},
		{"linear(category)", []string{"category"}},
		// A bare list, without LINEAR(...).
		{"CATEGORY, ID", []string{"CATEGORY", "ID"}},
		{"LINEAR( CATEGORY ,  ID )", []string{"CATEGORY", "ID"}},
		// Commas inside an expression or a quoted name.
		{`LINEAR("a,b", TO_DATE(TS), SUBSTRING(CODE, 1, 3))`, []string{"a,b", "TO_DATE(TS)", "SUBSTRING(CODE, 1, 3)"}},
		{`LINEAR("we""ird")`, []string{`we"ird`}},
		{`LINEAR(V:"field"::STRING)`, []string{`V:"field"::STRING`}},
		{"", nil},
		{"LINEAR()", nil},
	} {
		require.Equal(t, tc.want, clusteringKeyEntries(tc.key), tc.key)
	}
}
