package scrapper

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetDecimalSize(t *testing.T) {
	type size struct{ precision, scale int64 }
	tests := []struct {
		name             string
		kind             ValueKind
		nativeType       string
		precision, scale int64
		ok               bool
		want             *size
	}{
		{name: "driver reports the size", kind: KindNumeric, nativeType: "NUMERIC", precision: 20, scale: 10, ok: true, want: &size{20, 10}},
		{name: "scale zero is a size", kind: KindNumeric, nativeType: "FIXED", precision: 38, scale: 0, ok: true, want: &size{38, 0}},
		{
			name:       "driver size wins over the type name",
			kind:       KindNumeric,
			nativeType: "DECIMAL(10,2)",
			precision:  20,
			scale:      6,
			ok:         true,
			want:       &size{20, 6},
		},
		{name: "size read from the type name", kind: KindNumeric, nativeType: "DECIMAL(20,6)", want: &size{20, 6}},
		{name: "spaced type name", kind: KindNumeric, nativeType: " Decimal( 38 , 6 ) ", want: &size{38, 6}},
		{name: "precision alone is scale zero", kind: KindNumeric, nativeType: "NUMBER(19)", want: &size{19, 0}},
		{name: "bare type name", kind: KindNumeric, nativeType: "DECIMAL"},
		{name: "wrapped type name", kind: KindNumeric, nativeType: "Nullable(Decimal(20, 6))"},
		{name: "lib/pq unconstrained NUMERIC", kind: KindNumeric, nativeType: "NUMERIC", precision: 65535, scale: 65531, ok: true},
		{name: "go-mssqldb unknown size", kind: KindNumeric, nativeType: "DECIMAL", precision: math.MaxInt64, scale: math.MaxInt64, ok: true},
		{name: "go-ora NUMBER without precision", kind: KindNumeric, nativeType: "NUMBER", precision: 38, scale: 255, ok: true},
		{name: "precision zero", kind: KindNumeric, nativeType: "NUMBER", precision: 0, scale: 0, ok: true},
		{name: "negative scale", kind: KindNumeric, nativeType: "NUMBER", precision: 126, scale: -127, ok: true},
		{name: "scale equal to precision", kind: KindNumeric, nativeType: "NUMERIC", precision: 5, scale: 5, ok: true, want: &size{5, 5}},
		{name: "negative scale in the type name", kind: KindNumeric, nativeType: "NUMBER(5,-2)"},
		{name: "float", kind: KindFloat, nativeType: "DOUBLE", precision: math.MaxInt64, scale: 31, ok: true},
		{name: "timestamp", kind: KindTimestamp, nativeType: "TIMESTAMP_NTZ", precision: 9, scale: 0, ok: true},
		{name: "unknown kind", kind: KindUnknown, nativeType: "DECIMAL(20,6)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := &QueryShapeColumn{NativeType: tt.nativeType, Kind: tt.kind}
			assert.Same(t, col, col.SetDecimalSize(tt.precision, tt.scale, tt.ok))
			if tt.want == nil {
				assert.Nil(t, col.Precision, "precision")
				assert.Nil(t, col.Scale, "scale")
				return
			}
			if assert.NotNil(t, col.Precision, "precision") {
				assert.Equal(t, tt.want.precision, *col.Precision, "precision")
			}
			if assert.NotNil(t, col.Scale, "scale") {
				assert.Equal(t, tt.want.scale, *col.Scale, "scale")
			}
		})
	}
}
