package bigquery

import (
	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/scrapper"
)

// shapeColumns describes a BigQuery result schema as QueryShape and
// RunRawQuery report it, an exact numeric's precision and scale included.
func shapeColumns(dialect string, schema bigquery.Schema) []*scrapper.QueryShapeColumn {
	cols := make([]*scrapper.QueryShapeColumn, len(schema))
	for i, field := range schema {
		col := &scrapper.QueryShapeColumn{
			Name:       field.Name,
			NativeType: string(field.Type),
			Position:   int32(i + 1),
			Kind:       scrapper.NativeValueKind(dialect, string(field.Type)),
		}
		cols[i] = col.SetDecimalSize(decimalSize(field))
	}
	return cols
}

// decimalSize is a NUMERIC or BIGNUMERIC field's precision and scale. The
// schema carries them only for a parameterised column (NUMERIC(20, 6)); an
// unparameterised one has the type's fixed size, NUMERIC(38, 9) and
// BIGNUMERIC(76.76, 38), whose precision is reported as its whole digits.
func decimalSize(field *bigquery.FieldSchema) (int64, int64, bool) {
	if field.Precision > 0 {
		return field.Precision, field.Scale, true
	}
	switch field.Type {
	case bigquery.NumericFieldType:
		return 38, 9, true
	case bigquery.BigNumericFieldType:
		return 76, 38, true
	default:
		return 0, 0, false
	}
}
