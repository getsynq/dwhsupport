package scrapper

import (
	"regexp"
	"strconv"
	"strings"
)

type QueryShapeColumn struct {
	Name       string `json:"name"`
	NativeType string `json:"native_type"`
	Position   int32  `json:"position"`
	// Kind is what NativeType means on this warehouse, filled in by
	// RunRawQuery and QueryShape. Pass it to SqlLiteral to write a value of
	// this column back into SQL. KindUnknown when the type is not one
	// NativeValueKind recognises.
	Kind ValueKind `json:"kind,omitempty"`
	// Precision and Scale are an exact numeric column's declared size
	// (KindNumeric), from the driver (sql.ColumnType.DecimalSize) or, when it
	// reports none, from NativeType (DECIMAL(20,6)). Both are nil for every
	// other kind, and for a numeric whose size is not known: one declared
	// without it (a Postgres NUMERIC, an Oracle NUMBER), or one whose driver
	// reports neither and names it bare (Databricks, Athena). A nil scale is
	// not a scale of 0.
	Precision *int64 `json:"precision,omitempty"`
	Scale     *int64 `json:"scale,omitempty"`
}

// maxDecimalPrecision is the widest exact numeric any supported warehouse
// declares (Postgres NUMERIC). A driver reporting more is describing a column
// declared without a size.
const maxDecimalPrecision = 1000

// decimalSizeInType matches a native type name that spells its size:
// DECIMAL(20,6), Decimal(38, 6), NUMBER(19).
var decimalSizeInType = regexp.MustCompile(`^\w+\(\s*(\d+)\s*(?:,\s*(-?\d+)\s*)?\)$`)

// SetDecimalSize records an exact numeric column's precision and scale, as
// sql.ColumnType.DecimalSize reports them, and returns c. Kind must be set
// first: any other kind is left without a size, since drivers report one for
// timestamps and floats too, where it means something else.
//
// When the driver reports nothing, the size is read from NativeType. A size no
// column can have is not recorded, which is how the drivers describe a numeric
// declared without one: lib/pq reports an unconstrained NUMERIC as 65535
// digits, go-mssqldb an unknown size as math.MaxInt64, and go-ora a NUMBER
// without precision as (38, 255). A scale above the precision is one of
// those, though Postgres 15 can declare it: such a column is reported
// without a size rather than with a wrong one.
func (c *QueryShapeColumn) SetDecimalSize(precision, scale int64, ok bool) *QueryShapeColumn {
	if c.Kind != KindNumeric {
		return c
	}
	if !ok {
		precision, scale, ok = parseDecimalSize(c.NativeType)
	}
	if !ok || precision < 1 || precision > maxDecimalPrecision || scale < 0 || scale > precision {
		return c
	}
	c.Precision = &precision
	c.Scale = &scale
	return c
}

// parseDecimalSize reads the size out of a native type name that spells it. A
// precision without a scale is a scale of 0, as SQL defines it.
func parseDecimalSize(nativeType string) (int64, int64, bool) {
	m := decimalSizeInType.FindStringSubmatch(strings.TrimSpace(nativeType))
	if m == nil {
		return 0, 0, false
	}
	precision, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	if m[2] == "" {
		return precision, 0, true
	}
	scale, err := strconv.ParseInt(m[2], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return precision, scale, true
}
