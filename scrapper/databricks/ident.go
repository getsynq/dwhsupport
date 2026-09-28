package databricks

import (
	"github.com/getsynq/dwhsupport/sqldialect"
)

// quotedName renders a catalog, schema or table name for a statement we build,
// each part backtick-quoted on its own with any backtick inside it doubled.
//
// The parts are the names Unity Catalog listed, so they are canonical: quoting
// them addresses exactly the listed object. A dash is legal in a Unity Catalog
// name and not in an unquoted identifier, which is how an unquoted
// `ANALYZE TABLE cat.my-schema.tbl` fails with INVALID_IDENTIFIER. Never quote
// a TableInfo.FullName instead: it joins the parts with dots unquoted, so a
// name containing a dot cannot be split back apart.
func quotedName(parts ...string) (string, error) {
	idents := make([]sqldialect.Ident, len(parts))
	for i, part := range parts {
		idents[i] = sqldialect.CanonicalIdent(part)
	}
	return sqldialect.QualifiedIdent(idents...).ToSql(sqldialect.NewDatabricksDialect())
}
