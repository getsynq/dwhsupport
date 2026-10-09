package stdsql

import (
	"database/sql"

	"github.com/getsynq/dwhsupport/scrapper"
)

// ShapeColumns describes the columns of a database/sql result as QueryShape
// and RunRawQuery report them: name, native type, position, the Kind the
// native type has on dialect (Scrapper.DialectType()), and an exact numeric's
// precision and scale.
func ShapeColumns(dialect string, columnTypes []*sql.ColumnType) []*scrapper.QueryShapeColumn {
	cols := make([]*scrapper.QueryShapeColumn, len(columnTypes))
	for i, ct := range columnTypes {
		col := &scrapper.QueryShapeColumn{
			Name:       ct.Name(),
			NativeType: ct.DatabaseTypeName(),
			Position:   int32(i + 1),
			Kind:       scrapper.NativeValueKind(dialect, ct.DatabaseTypeName()),
		}
		cols[i] = col.SetDecimalSize(ct.DecimalSize())
	}
	return cols
}
