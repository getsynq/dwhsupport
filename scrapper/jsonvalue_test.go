package scrapper

import (
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewJsonValueFromGo(t *testing.T) {
	u8 := uint8(7)
	tests := []struct {
		name        string
		in          any
		bytesAsInts bool
		want        string
	}{
		{"int_slice", []int64{1, 2, 3}, false, `[1,2,3]`},
		{"string_slice", []string{"a", "b"}, false, `["a","b"]`},
		{"clickhouse_uint8_array", []uint8{1, 2, 3}, true, `[1,2,3]`},
		{"byte_blob_as_string", []byte("hello"), false, `"hello"`},
		{"map_string_key", map[string]uint8{"a": 1, "b": 2}, false, `{"a":1,"b":2}`},
		{"map_int_key", map[int32]string{1: "x", 2: "y"}, false, `{"1":"x","2":"y"}`},
		{"unnamed_tuple", []any{int64(1), "x", 3.5}, false, `[1,"x",3.5]`},
		{"nested_array", [][]int64{{1, 2}, {3}}, false, `[[1,2],[3]]`},
		{"array_of_tuple", [][]any{{int64(1), "a"}, {int64(2), "b"}}, false, `[[1,"a"],[2,"b"]]`},
		{"pointer_elems_with_nil", []*uint8{&u8, nil}, false, `[7,null]`},
		{"big_int", big.NewInt(9223372036854775807), false, `9223372036854775807`},
		{"nested_struct_like", map[string]any{"items": []any{
			map[string]any{"id": int64(1)},
			map[string]any{"id": int64(2)},
		}}, false, `{"items":[{"id":1},{"id":2}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := NewJsonValueFromGo(tt.in, tt.bytesAsInts)
			require.True(t, ok)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

func TestNewJsonValueFromGo_BigRatDecimal(t *testing.T) {
	// BigQuery NUMERIC surfaces as *big.Rat; must render as an exact decimal.
	r := new(big.Rat)
	r.SetString("123.45")
	got, ok := NewJsonValueFromGo([]any{r}, false)
	require.True(t, ok)
	assert.Equal(t, `[123.45]`, string(got))
}

func TestNewJsonValueFromGo_Time(t *testing.T) {
	ts := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	got, ok := NewJsonValueFromGo([]any{ts}, false)
	require.True(t, ok)
	assert.Equal(t, `["2024-01-02T03:04:05Z"]`, string(got))
}

func TestNewJsonValueFromGo_InvalidUTF8Scrubbed(t *testing.T) {
	got, ok := NewJsonValueFromGo([]string{"a\xffb"}, false)
	require.True(t, ok)
	assert.Equal(t, `["ab"]`, string(got))
}

func TestNewJsonValueFromJSONText(t *testing.T) {
	// Pretty-printed Snowflake JSON compacts to canonical form.
	got, ok := NewJsonValueFromJSONText("[\n  1,\n  2,\n  3\n]")
	require.True(t, ok)
	assert.Equal(t, `[1,2,3]`, string(got))

	_, ok = NewJsonValueFromJSONText("not json")
	assert.False(t, ok)
}
