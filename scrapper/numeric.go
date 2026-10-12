package scrapper

// NumericCategory is the kind of number a column holds, which decides how many
// significant digits its values carry. It is the one answer to that question
// for every surface that describes a column: a query's shape
// (QueryShapeColumn.NumericCategory) and a table's catalog
// (CatalogColumnRow.NumericCategory) both come from NumericCategoryOf.
type NumericCategory string

const (
	// NumericCategoryNone is a column that does not hold a number, or whose
	// type is not recognised.
	NumericCategoryNone NumericCategory = ""
	// NumericCategoryInteger is a whole number: an integer type, or an exact
	// numeric declared with scale 0.
	NumericCategoryInteger NumericCategory = "integer"
	// NumericCategoryDecimal is an exact decimal that may hold fractions,
	// sized by its precision and scale when they are known.
	NumericCategoryDecimal NumericCategory = "decimal"
	// NumericCategoryFloat is a double-precision binary float, about 15
	// significant digits.
	NumericCategoryFloat NumericCategory = "float"
	// NumericCategorySinglePrecisionFloat is a single-precision binary float,
	// about 7 significant digits.
	NumericCategorySinglePrecisionFloat NumericCategory = "single_precision_float"
)

// NumericCategoryOf is what kind of number a column of kind holds. scale is
// its declared scale when known, and singlePrecision is
// IsSinglePrecisionFloat (or what the driver reported) for a float.
//
// An exact numeric declared with scale 0 holds only whole numbers and is an
// integer: Snowflake declares every integer type as NUMBER(38, 0), and
// Oracle's NUMBER(19) is one too. One whose scale is not known may hold
// fractions, and stays a decimal.
func NumericCategoryOf(kind ValueKind, scale *int64, singlePrecision bool) NumericCategory {
	switch kind {
	case KindInteger:
		return NumericCategoryInteger
	case KindNumeric:
		if scale != nil && *scale == 0 {
			return NumericCategoryInteger
		}
		return NumericCategoryDecimal
	case KindFloat:
		if singlePrecision {
			return NumericCategorySinglePrecisionFloat
		}
		return NumericCategoryFloat
	default:
		return NumericCategoryNone
	}
}

// NumericCategory is what kind of number the column holds (NumericCategoryOf).
func (c *QueryShapeColumn) NumericCategory() NumericCategory {
	return NumericCategoryOf(c.Kind, c.Scale, c.SinglePrecision)
}

// ResolveNumeric fills a catalog row's Precision, Scale and NumericCategory
// from its Type on dialect (Scrapper.DialectType()), and returns r. Every
// QueryCatalog calls it on each row it returns.
//
// A catalog query that reads the size from the warehouse's own columns
// (Snowflake's numeric_precision and numeric_scale) scans it into Precision
// and Scale first; otherwise the size is read from Type (DECIMAL(20,6)). Either
// way it is kept only for an exact numeric, since information_schema reports a
// precision for integers and floats too, in bits on some engines, and only
// when it is a size a column can have (SetDecimalSize). A numeric declared
// without a size keeps both nil: a nil scale is not a scale of 0.
func (r *CatalogColumnRow) ResolveNumeric(dialect string) *CatalogColumnRow {
	kind := NativeValueKind(dialect, r.Type)
	precision, scale, ok := int64(0), int64(0), false
	if r.Precision != nil && r.Scale != nil {
		precision, scale, ok = *r.Precision, *r.Scale, true
	}
	r.Precision, r.Scale = nil, nil
	if kind == KindNumeric {
		if !ok {
			precision, scale, ok = parseDecimalSize(r.Type)
		}
		if ok && validDecimalSize(precision, scale) {
			r.Precision, r.Scale = &precision, &scale
		}
	}
	r.NumericCategory = NumericCategoryOf(kind, r.Scale, IsSinglePrecisionFloat(dialect, r.Type))
	return r
}
