package redshift

import (
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/require"
)

func TestSortDistKeyConstraints(t *testing.T) {
	// As pg_attribute holds them, in abs(attsortkeyord) order.
	rows := []*sortDistRow{
		// COMPOUND SORTKEY (category, id), DISTKEY (id)
		{SchemaName: "s", TableName: "products", ColumnName: "category", SortKeyOrd: 1},
		{SchemaName: "s", TableName: "products", ColumnName: "id", SortKeyOrd: 2, IsDistKey: true},
		// INTERLEAVED SORTKEY (order_id, product_id): only the first position is negated.
		{SchemaName: "s", TableName: "order_items", ColumnName: "order_id", SortKeyOrd: -1, IsDistKey: true},
		{SchemaName: "s", TableName: "order_items", ColumnName: "product_id", SortKeyOrd: 2},
		// A distribution key alone.
		{SchemaName: "s", TableName: "customers", ColumnName: "id", IsDistKey: true},
	}

	key := func(table, constraintType, name, column string, position int32, expression string) *scrapper.TableConstraintRow {
		return &scrapper.TableConstraintRow{
			Instance:             "host",
			Database:             "db",
			Schema:               "s",
			Table:                table,
			ConstraintName:       name,
			ColumnName:           column,
			ConstraintType:       constraintType,
			ColumnPosition:       position,
			ConstraintExpression: expression,
		}
	}
	sortKey, distKey := scrapper.ConstraintTypeSortingKey, scrapper.ConstraintTypeDistributionKey
	require.Equal(t, []*scrapper.TableConstraintRow{
		key("products", sortKey, "sortkey", "category", 1, ""),
		key("products", sortKey, "sortkey", "id", 2, ""),
		key("products", distKey, "distkey", "id", 1, ""),
		key("order_items", sortKey, "sortkey", "order_id", 1, "INTERLEAVED"),
		key("order_items", distKey, "distkey", "order_id", 1, ""),
		key("order_items", sortKey, "sortkey", "product_id", 2, "INTERLEAVED"),
		key("customers", distKey, "distkey", "id", 1, ""),
	}, sortDistKeyConstraints("host", "db", rows))

	require.Empty(t, sortDistKeyConstraints("host", "db", nil))
}
