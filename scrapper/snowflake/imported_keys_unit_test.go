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
		IsEnforced:           lo.ToPtr(false),
		IsRely:               lo.ToPtr(true),
		ReferencedDatabase:   "DWH_TESTING",
		ReferencedSchema:     "SCHEMA_B",
		ReferencedTable:      "CUSTOMERS",
		ReferencedColumnName: "ID",
	}, importedKeyRow("account", row, hybridTables{}))
}

func TestImportedKeyRow_NoRely(t *testing.T) {
	row := map[string]any{"fk_name": "FK", "key_sequence": "2", "rely": "false"}
	got := importedKeyRow("account", row, hybridTables{})
	require.Equal(t, int32(2), got.ColumnPosition)
	require.Equal(t, lo.ToPtr(false), got.IsEnforced, "Snowflake does not enforce a foreign key on a standard table")
	require.Equal(t, lo.ToPtr(false), got.IsRely)

	delete(row, "rely")
	got = importedKeyRow("account", row, hybridTables{})
	require.Equal(t, lo.ToPtr(false), got.IsEnforced, "whether RELY is known does not change enforcement")
	require.Nil(t, got.IsRely, "a key with no rely column is unknown")
}

func TestImportedKeyRow_HybridTableEnforces(t *testing.T) {
	row := map[string]any{
		"fk_database_name": "DB",
		"fk_schema_name":   "S",
		"fk_table_name":    "ORDERS",
		"fk_name":          "FK",
		"key_sequence":     "1",
		"rely":             "false",
	}
	hybrid := hybridTables{{"DB", "S", "ORDERS"}: true}
	require.Equal(t, lo.ToPtr(true), importedKeyRow("account", row, hybrid).IsEnforced)

	row["fk_table_name"] = "orders"
	require.Equal(t, lo.ToPtr(false), importedKeyRow("account", row, hybrid).IsEnforced, "a quoted lowercase name is another table")
}

func TestKeyRow(t *testing.T) {
	row := map[string]any{
		"database_name":   "DB",
		"schema_name":     "S",
		"table_name":      "PRODUCTS",
		"column_name":     "SKU",
		"key_sequence":    "2",
		"constraint_name": "UQ_SKU",
		"rely":            "true",
	}

	require.Equal(t, &scrapper.TableConstraintRow{
		Instance:       "account",
		Database:       "DB",
		Schema:         "S",
		Table:          "PRODUCTS",
		ConstraintName: "UQ_SKU",
		ColumnName:     "SKU",
		ConstraintType: scrapper.ConstraintTypeUniqueIndex,
		ColumnPosition: 2,
		IsEnforced:     lo.ToPtr(false),
		IsRely:         lo.ToPtr(true),
	}, keyRow("account", scrapper.ConstraintTypeUniqueIndex, row, hybridTables{}))

	hybrid := hybridTables{{"DB", "S", "PRODUCTS"}: true}
	got := keyRow("account", scrapper.ConstraintTypePrimaryKey, row, hybrid)
	require.Equal(t, lo.ToPtr(true), got.IsEnforced, "a hybrid table enforces its keys")
	require.Equal(t, lo.ToPtr(true), got.IsRely)

	row["rely"] = ""
	require.Nil(t, keyRow("account", scrapper.ConstraintTypePrimaryKey, row, nil).IsRely, "an empty rely is unknown")
	require.Equal(t, lo.ToPtr(false), keyRow("account", scrapper.ConstraintTypePrimaryKey, row, nil).IsEnforced)
}
