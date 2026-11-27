package stdsql

import (
	"database/sql"
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestValueFromRawKeepsTextByteExact(t *testing.T) {
	texts := map[string]string{
		// 16 bytes, the length of a binary UUID.
		"sixteen_bytes": "Orders per day..",
		// The canonical text of a UUID, upper-cased, so a reformat would show.
		"uuid_like": "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
		// JSON stays the text it was, spaces included.
		"json_object": `{"a": 1}`,
		"json_array":  `[1, 2]`,
		// A Postgres array literal is not an array without a column type.
		"pg_array_literal": "{1,2,3}",
		"number_like":      "0042",
		"empty":            "",
	}
	for name, text := range texts {
		for form, raw := range map[string]any{"string": text, "bytes": []byte(text), "raw_bytes": sql.RawBytes(text)} {
			t.Run(name+"/"+form, func(t *testing.T) {
				v := ValueFromRaw(raw)
				assert.Equal(t, scrapper.StringValue(text), v)
				s, ok := scrapper.AsString(v)
				assert.True(t, ok)
				assert.Equal(t, text, s)
			})
		}
	}
}

func TestValueFromRawDropsInvalidUTF8(t *testing.T) {
	assert.Equal(t, scrapper.StringValue("ab"), ValueFromRaw([]byte("a\xffb")))
	assert.Equal(t, scrapper.StringValue("ab"), ValueFromRaw("a\xffb"))
}

func TestValueFromRawNull(t *testing.T) {
	v := ValueFromRaw(nil)
	assert.Equal(t, scrapper.NullValue{}, v)

	s, ok := scrapper.AsString(v)
	assert.False(t, ok)
	assert.Equal(t, "", s)
	_, ok = scrapper.AsInt64(v)
	assert.False(t, ok)
	_, ok = scrapper.AsFloat64(v)
	assert.False(t, ok)
	_, ok = scrapper.AsBigRat(v)
	assert.False(t, ok)
	_, ok = scrapper.AsTime(v)
	assert.False(t, ok)
	assert.Nil(t, scrapper.AsGo(v))
}

func TestValueFromRawNonText(t *testing.T) {
	ts := time.Date(2024, 3, 15, 12, 20, 30, 123456000, time.FixedZone("", 2*3600))
	huge, _ := new(big.Int).SetString("123456789012345678901234567890", 10)
	id := [16]byte{0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a, 0x11}

	tests := []struct {
		name string
		in   any
		want scrapper.Value
	}{
		{"int64", int64(42), scrapper.IntValue(42)},
		{"int32", int32(-7), scrapper.IntValue(-7)},
		{"uint8", uint8(200), scrapper.IntValue(200)},
		{"float64", 12.5, scrapper.DoubleValue(12.5)},
		{"bool", true, scrapper.IntValue(1)},
		{"time_to_utc", ts, scrapper.TimeValue(time.Date(2024, 3, 15, 10, 20, 30, 123456000, time.UTC))},
		{"big_int_small", big.NewInt(5), scrapper.IntValue(5)},
		{"big_int_huge", huge, scrapper.NewBigIntValue(huge)},
		// A *big.Rat is exact by its type (BigQuery NUMERIC), so it is decoded
		// like RunRawQuery decodes BigQuery NUMERIC, not through float64.
		{"big_rat_fraction", big.NewRat(12345678901234567, 1000), scrapper.StringValue("12345678901234.567")},
		{"big_rat_whole", big.NewRat(9007199254740993, 1), scrapper.IntValue(9007199254740993)},
		{"uuid_array", id, scrapper.StringValue("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")},
		{"uuid_named_type", uuid.UUID(id), scrapper.StringValue("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")},
		// A typed nil pointer is a NULL that `v == nil` does not catch.
		{"nil_big_rat", (*big.Rat)(nil), scrapper.NullValue{}},
		{"nil_big_int", (*big.Int)(nil), scrapper.NullValue{}},
		{"nested_slice", []any{int64(1), "x"}, scrapper.JsonValue(`[1,"x"]`)},
		{"nested_map", map[string]any{"a": int64(1)}, scrapper.JsonValue(`{"a":1}`)},
		// BigQuery's QueryMaps hands back civil values for DATE, TIME, DATETIME.
		{"civil_date", civil.Date{Year: 2024, Month: 3, Day: 15}, scrapper.StringValue("2024-03-15")},
		{"civil_time", civil.Time{Hour: 10, Minute: 20, Second: 30}, scrapper.StringValue("10:20:30")},
		{
			"civil_datetime",
			civil.DateTime{Date: civil.Date{Year: 2024, Month: 3, Day: 15}, Time: civil.Time{Hour: 10, Minute: 20, Second: 30}},
			scrapper.StringValue("2024-03-15T10:20:30"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ValueFromRaw(tt.in))
		})
	}
}

// An unexpected type is any value but NullValue for which an accessor says no.
func TestValueFromRawTellsNullFromUnexpectedType(t *testing.T) {
	_, ok := scrapper.AsInt64(ValueFromRaw("not a number"))
	assert.False(t, ok)
	assert.NotEqual(t, scrapper.NullValue{}, ValueFromRaw("not a number"))
}
