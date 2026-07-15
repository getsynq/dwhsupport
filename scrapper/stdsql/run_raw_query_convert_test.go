package stdsql

import (
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/assert"
)

func TestConvertToRawValueComplex(t *testing.T) {
	tests := []struct {
		name       string
		in         any
		nativeType string
		want       scrapper.Value
	}{
		// Scalars unchanged.
		{"string", "hello", "String", scrapper.StringValue("hello")},
		{"int", int64(42), "Int64", scrapper.IntValue(42)},
		{"blob_stays_string", []byte("hi"), "String", scrapper.StringValue("hi")},

		// ClickHouse native nested types -> JsonValue.
		{"ch_array_int", []int32{1, 2, 3}, "Array(Int32)", scrapper.JsonValue(`[1,2,3]`)},
		{"ch_array_uint8", []uint8{1, 2, 3}, "Array(UInt8)", scrapper.JsonValue(`[1,2,3]`)},
		{"ch_map", map[string]int64{"a": 1}, "Map(String, Int64)", scrapper.JsonValue(`{"a":1}`)},
		{"ch_tuple", []any{int64(1), "x"}, "Tuple(Int64, String)", scrapper.JsonValue(`[1,"x"]`)},
		{"ch_nullable_array", []int32{1, 2}, "Array(Nullable(Int32))", scrapper.JsonValue(`[1,2]`)},

		// JSON-text types (Snowflake ARRAY/OBJECT/VARIANT) -> JsonValue (compacted).
		{"sf_array", "[\n  1,\n  2\n]", "ARRAY", scrapper.JsonValue(`[1,2]`)},
		{"sf_object", `{"a": 1}`, "OBJECT", scrapper.JsonValue(`{"a":1}`)},

		// Redshift SUPER: JSON []byte with empty native type.
		{"super_bytes", []byte(`[1,2,3]`), "", scrapper.JsonValue(`[1,2,3]`)},
		// A plain text column that merely looks like JSON must stay a string.
		{"varchar_looks_json", "[not,json]", "VARCHAR", scrapper.StringValue("[not,json]")},

		// Generic driver container from a dialect we do not special-case.
		{"generic_slice", []any{int64(1), int64(2)}, "SOMETHING", scrapper.JsonValue(`[1,2]`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, convertToRawValue(tt.in, tt.nativeType))
		})
	}
}

func TestIsNativeNestedType(t *testing.T) {
	assert.True(t, isNativeNestedType("Array(Int32)"))
	assert.True(t, isNativeNestedType("Map(String, Int64)"))
	assert.True(t, isNativeNestedType("Tuple(Int64, String)"))
	assert.True(t, isNativeNestedType("Nullable(Array(Int32))"))
	assert.True(t, isNativeNestedType("LowCardinality(Array(String))"))
	assert.False(t, isNativeNestedType("String"))
	assert.False(t, isNativeNestedType("Int64"))
	assert.False(t, isNativeNestedType("ARRAY")) // JSON-text type, not native nested
}
