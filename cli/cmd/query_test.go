package cmd

import (
	"reflect"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
)

func TestColumnValueToAny(t *testing.T) {
	cases := []struct {
		name string
		in   *scrapper.ColumnValue
		want any
	}{
		{"null_flag", &scrapper.ColumnValue{IsNull: true}, nil},
		{"nil_value", &scrapper.ColumnValue{Value: nil}, nil},
		{"string", &scrapper.ColumnValue{Value: scrapper.StringValue("hi")}, "hi"},
		{"int", &scrapper.ColumnValue{Value: scrapper.IntValue(42)}, int64(42)},
		{"double", &scrapper.ColumnValue{Value: scrapper.DoubleValue(3.5)}, 3.5},
		{
			// A JSON object must decode into a nested map so structured renderers
			// emit an object, not a double-encoded string.
			"json_object_nested",
			&scrapper.ColumnValue{Value: scrapper.JsonValue(`{"a":1,"items":[1,2]}`)},
			map[string]any{"a": float64(1), "items": []any{float64(1), float64(2)}},
		},
		{
			"json_array_nested",
			&scrapper.ColumnValue{Value: scrapper.JsonValue(`[1,"x",true]`)},
			[]any{float64(1), "x", true},
		},
		{
			"malformed_json_falls_back_to_text",
			&scrapper.ColumnValue{Value: scrapper.JsonValue(`{not json`)},
			`{not json`,
		},
		{
			// Documents the known display-only precision limitation: nested
			// integers beyond 2^53 collapse to float64 on the CLI's structured
			// output path (the JsonValue wire text itself stays lossless). Pinned
			// so a future change that preserves precision updates this on purpose.
			"nested_big_int_collapses_to_float64",
			&scrapper.ColumnValue{Value: scrapper.JsonValue(`[9223372036854775807]`)},
			[]any{float64(9223372036854775807)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := columnValueToAny(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("columnValueToAny() = %#v, want %#v", got, tc.want)
			}
		})
	}
}
