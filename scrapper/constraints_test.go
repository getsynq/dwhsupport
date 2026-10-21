package scrapper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReferencedTableFqn(t *testing.T) {
	fk := TableConstraintRow{
		Instance:           "host",
		Database:           "db",
		Schema:             "sales",
		Table:              "order_items",
		ConstraintType:     ConstraintTypeForeignKey,
		ReferencedDatabase: "other_db",
		ReferencedSchema:   "catalog",
		ReferencedTable:    "products",
	}
	fqn, ok := fk.ReferencedTableFqn()
	assert.True(t, ok)
	assert.Equal(t, DwhFqn{InstanceName: "host", DatabaseName: "other_db", SchemaName: "catalog", ObjectName: "products"}, fqn)

	pk := TableConstraintRow{Instance: "host", Database: "db", Schema: "sales", Table: "orders", ConstraintType: ConstraintTypePrimaryKey}
	_, ok = pk.ReferencedTableFqn()
	assert.False(t, ok, "a primary key references no table")
}

func TestTableConstraintRow_SanitizeReferencedTable(t *testing.T) {
	row := &TableConstraintRow{
		ConstraintName:       "fk",
		ReferencedDatabase:   "d\x00b",
		ReferencedSchema:     "s\x00",
		ReferencedTable:      "\x00t",
		ReferencedColumnName: "i\x00d",
	}
	row.Sanitize()

	assert.Equal(t, "db", row.ReferencedDatabase)
	assert.Equal(t, "s", row.ReferencedSchema)
	assert.Equal(t, "t", row.ReferencedTable)
	assert.Equal(t, "id", row.ReferencedColumnName)
}
