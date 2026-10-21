package snowflake

import (
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestImportedKeyRow(t *testing.T) {
	row := map[string]any{
		"pk_database_name": "DWH_TESTING",
		"pk_schema_name":   "SCHEMA_B",
		"pk_table_name":    "CUSTOMERS",
		"pk_column_name":   "ID",
		"fk_database_name": "DWH_TESTING",
		"fk_schema_name":   "SEMANTIC_VIEWS",
		"fk_table_name":    "ORDERS",
		"fk_column_name":   "CUSTOMER_ID",
		"key_sequence":     "1",
		"fk_name":          "FK_SV_ORDERS_CUST",
		"pk_name":          "PK_CUSTOMERS",
		"rely":             "true",
	}

	require.Equal(t, &scrapper.TableConstraintRow{
		Instance:             "account",
		Database:             "DWH_TESTING",
		Schema:               "SEMANTIC_VIEWS",
		Table:                "ORDERS",
		ConstraintName:       "FK_SV_ORDERS_CUST",
		ColumnName:           "CUSTOMER_ID",
		ConstraintType:       scrapper.ConstraintTypeForeignKey,
		ColumnPosition:       1,
		IsEnforced:           lo.ToPtr(true),
		ReferencedDatabase:   "DWH_TESTING",
		ReferencedSchema:     "SCHEMA_B",
		ReferencedTable:      "CUSTOMERS",
		ReferencedColumnName: "ID",
	}, importedKeyRow("account", row))
}

func TestImportedKeyRow_NoRely(t *testing.T) {
	row := map[string]any{"fk_name": "FK", "key_sequence": "2", "rely": "false"}
	got := importedKeyRow("account", row)
	require.Equal(t, int32(2), got.ColumnPosition)
	require.Equal(t, lo.ToPtr(false), got.IsEnforced)

	delete(row, "rely")
	require.Nil(t, importedKeyRow("account", row).IsEnforced, "a key with no rely column is unknown")
}
