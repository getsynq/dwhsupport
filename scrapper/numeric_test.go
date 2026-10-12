package scrapper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNumericCategoryOf(t *testing.T) {
	zero, six := int64(0), int64(6)
	tests := []struct {
		name            string
		kind            ValueKind
		scale           *int64
		singlePrecision bool
		want            NumericCategory
	}{
		{name: "integer", kind: KindInteger, want: NumericCategoryInteger},
		{name: "decimal with scale", kind: KindNumeric, scale: &six, want: NumericCategoryDecimal},
		{name: "decimal with scale zero is an integer", kind: KindNumeric, scale: &zero, want: NumericCategoryInteger},
		{name: "decimal of unknown scale stays a decimal", kind: KindNumeric, want: NumericCategoryDecimal},
		{name: "double", kind: KindFloat, want: NumericCategoryFloat},
		{name: "single precision float", kind: KindFloat, singlePrecision: true, want: NumericCategorySinglePrecisionFloat},
		{name: "single precision flag on a non-float", kind: KindInteger, singlePrecision: true, want: NumericCategoryInteger},
		{name: "text", kind: KindText, want: NumericCategoryNone},
		{name: "timestamp", kind: KindTimestamp, want: NumericCategoryNone},
		{name: "unknown", kind: KindUnknown, want: NumericCategoryNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, NumericCategoryOf(tt.kind, tt.scale, tt.singlePrecision))
			col := &QueryShapeColumn{Kind: tt.kind, Scale: tt.scale, SinglePrecision: tt.singlePrecision}
			assert.Equal(t, tt.want, col.NumericCategory(), "QueryShapeColumn")
		})
	}
}

func TestCatalogColumnRowResolveNumeric(t *testing.T) {
	type size struct{ precision, scale int64 }
	ptr := func(v int64) *int64 { return &v }
	tests := []struct {
		name             string
		dialect          string
		nativeType       string
		precision, scale *int64
		want             *size
		category         NumericCategory
	}{
		{name: "size in the type name", dialect: "postgres", nativeType: "numeric(20,6)", want: &size{20, 6}, category: NumericCategoryDecimal},
		{name: "numeric without a size", dialect: "postgres", nativeType: "numeric", category: NumericCategoryDecimal},
		{name: "precision alone is scale zero", dialect: "oracle", nativeType: "NUMBER(10)", want: &size{10, 0}, category: NumericCategoryInteger},
		{name: "Oracle NUMBER without precision", dialect: "oracle", nativeType: "NUMBER", category: NumericCategoryDecimal},
		{name: "Oracle FLOAT is a numeric of unknown size", dialect: "oracle", nativeType: "FLOAT(126)", category: NumericCategoryDecimal},
		{
			name:       "MySQL unsigned decimal",
			dialect:    "mysql",
			nativeType: "decimal(10,2) unsigned",
			want:       &size{10, 2},
			category:   NumericCategoryDecimal,
		},
		{
			name:       "ClickHouse nullable decimal",
			dialect:    "clickhouse",
			nativeType: "Nullable(Decimal(38, 6))",
			want:       &size{38, 6},
			category:   NumericCategoryDecimal,
		},
		{name: "Db2 DECFLOAT", dialect: "db2", nativeType: "DECFLOAT(34)", category: NumericCategoryDecimal},
		{
			name:       "size from the catalog's own columns",
			dialect:    "snowflake",
			nativeType: "NUMBER",
			precision:  ptr(20),
			scale:      ptr(6),
			want:       &size{20, 6},
			category:   NumericCategoryDecimal,
		},
		{
			name:       "Snowflake integer is NUMBER(38,0)",
			dialect:    "snowflake",
			nativeType: "NUMBER",
			precision:  ptr(38),
			scale:      ptr(0),
			want:       &size{38, 0},
			category:   NumericCategoryInteger,
		},
		{
			name:       "catalog size wins over the type name",
			dialect:    "duckdb",
			nativeType: "DECIMAL(10,2)",
			precision:  ptr(20),
			scale:      ptr(6),
			want:       &size{20, 6},
			category:   NumericCategoryDecimal,
		},
		{
			name:       "precision without scale falls back to the type name",
			dialect:    "duckdb",
			nativeType: "DECIMAL(10,2)",
			precision:  ptr(20),
			want:       &size{10, 2},
			category:   NumericCategoryDecimal,
		},
		{
			name:       "impossible size is dropped",
			dialect:    "snowflake",
			nativeType: "NUMBER",
			precision:  ptr(38),
			scale:      ptr(255),
			category:   NumericCategoryDecimal,
		},
		{
			name:       "information_schema precision of an integer is dropped",
			dialect:    "snowflake",
			nativeType: "BIGINT",
			precision:  ptr(64),
			scale:      ptr(0),
			category:   NumericCategoryInteger,
		},
		{
			name:       "information_schema precision of a float is dropped",
			dialect:    "postgres",
			nativeType: "double precision",
			precision:  ptr(53),
			scale:      ptr(0),
			category:   NumericCategoryFloat,
		},
		{name: "single precision float", dialect: "postgres", nativeType: "real", category: NumericCategorySinglePrecisionFloat},
		{name: "bare FLOAT is single on MySQL", dialect: "mysql", nativeType: "float", category: NumericCategorySinglePrecisionFloat},
		{name: "Snowflake REAL is a double", dialect: "snowflake", nativeType: "REAL", category: NumericCategoryFloat},
		{name: "integer", dialect: "postgres", nativeType: "bigint", category: NumericCategoryInteger},
		{name: "text keeps no size", dialect: "postgres", nativeType: "character varying(20)", precision: ptr(20), scale: ptr(0)},
		{name: "unknown type", dialect: "postgres", nativeType: "tsvector"},
		{name: "empty type", dialect: "postgres", nativeType: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := &CatalogColumnRow{Type: tt.nativeType, Precision: tt.precision, Scale: tt.scale}
			assert.Same(t, row, row.ResolveNumeric(tt.dialect))
			assert.Equal(t, tt.category, row.NumericCategory, "category")
			if tt.want == nil {
				assert.Nil(t, row.Precision, "precision")
				assert.Nil(t, row.Scale, "scale")
				return
			}
			if assert.NotNil(t, row.Precision, "precision") {
				assert.Equal(t, tt.want.precision, *row.Precision, "precision")
			}
			if assert.NotNil(t, row.Scale, "scale") {
				assert.Equal(t, tt.want.scale, *row.Scale, "scale")
			}
		})
	}
}

// The size a row is resolved with is its own copy, so changing it does not
// write through to the values a caller scanned into it.
func TestCatalogColumnRowResolveNumericCopiesTheSize(t *testing.T) {
	precision, scale := int64(20), int64(6)
	row := &CatalogColumnRow{Type: "NUMBER", Precision: &precision, Scale: &scale}
	row.ResolveNumeric("snowflake")
	*row.Scale = 2
	assert.Equal(t, int64(6), scale)
}

// Resolving twice gives the same answer, so a row that passes through it again
// is not changed.
func TestCatalogColumnRowResolveNumericIsIdempotent(t *testing.T) {
	row := (&CatalogColumnRow{Type: "numeric(20,6)"}).ResolveNumeric("postgres")
	again := *row
	again.ResolveNumeric("postgres")
	assert.Equal(t, row.NumericCategory, again.NumericCategory)
	assert.Equal(t, *row.Precision, *again.Precision)
	assert.Equal(t, *row.Scale, *again.Scale)
}
