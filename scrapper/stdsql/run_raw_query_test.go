package stdsql

import (
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestConvertToRawValue(t *testing.T) {
	canonical := "04fcaa82-51fd-4767-b42a-2df6cdc5d0ca"
	parsed := uuid.MustParse(canonical)

	tests := []struct {
		name    string
		in      any
		want    scrapper.Value
		wantStr string
	}{
		{
			name: "plain string preserved",
			in:   "04fcaa82-51fd-4767-b42a-2df6cdc5d0ca",
			want: scrapper.StringValue(canonical),
		},
		{
			name: "byte slice with UTF-8 text preserved",
			in:   []byte("hello"),
			want: scrapper.StringValue("hello"),
		},
		{
			name: "16-byte array rendered as canonical UUID (native pgx)",
			in:   [16]byte(parsed),
			want: scrapper.StringValue(canonical),
		},
		{
			name: "16-byte slice rendered as canonical UUID (binary uuid column)",
			in:   parsed[:],
			want: scrapper.StringValue(canonical),
		},
		{
			name: "36-byte canonical-form slice round-trips",
			in:   []byte(canonical),
			want: scrapper.StringValue(canonical),
		},
		{
			name: "int preserved via IntValue",
			in:   int64(42),
			want: scrapper.IntValue(42),
		},
		{
			name: "Stringer fallback for unknown types",
			in:   stringerType{s: "via-Stringer"},
			want: scrapper.StringValue("via-Stringer"),
		},
		{
			name:    "fmt.Sprint fallback for opaque types",
			in:      opaqueType{n: 7},
			wantStr: "{7}",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := convertToRawValue(tc.in, rawCol(""))
			if tc.wantStr != "" {
				sv, ok := got.(scrapper.StringValue)
				assert.True(t, ok, "expected StringValue, got %T", got)
				assert.Equal(t, tc.wantStr, string(sv))
				return
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

// A 16-byte text column is not a binary UUID. go-sql-driver/mysql hands every
// VARCHAR back as []byte, so this used to render any 16-character string as a
// UUID.
func TestConvertToRawValue_SixteenByteTextIsNotUUID(t *testing.T) {
	assert.Equal(t, scrapper.StringValue("abcdefghijklmnop"), convertToRawValue([]byte("abcdefghijklmnop"), rawCol("VARCHAR")))
}

// go-mssqldb hands a UNIQUEIDENTIFIER back in SQL Server's storage order,
// whose first three groups are little-endian. Read as a plain UUID it comes
// out as 99bceea0-0b9c-f84e-... for a0eebc99-9c0b-4ef8-...
func TestConvertToRawValue_MSSQLUniqueIdentifier(t *testing.T) {
	stored := []byte{0x99, 0xbc, 0xee, 0xa0, 0x0b, 0x9c, 0xf8, 0x4e, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a, 0x11}
	assert.Equal(t, scrapper.StringValue("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"), convertToRawValue(stored, rawCol("UNIQUEIDENTIFIER")))
}

// An exact numeric keeps its digits: whole values become integers, fractional
// ones stay text, and neither goes through float64.
func TestConvertToRawValue_ExactNumeric(t *testing.T) {
	assert.Equal(t, scrapper.IntValue(9007199254740993), convertToRawValue("9007199254740993", rawCol("FIXED")))
	assert.Equal(t, scrapper.IntValue(1), convertToRawValue("1", rawCol("NUMBER")))
	assert.Equal(t, scrapper.StringValue("12345678901234.567890"), convertToRawValue([]byte("12345678901234.567890"), rawCol("NUMERIC")))
	// A 16-character numeric is not a UUID either.
	assert.Equal(t, scrapper.StringValue("1234567890.12345"), convertToRawValue([]byte("1234567890.12345"), rawCol("DECIMAL")))
	assert.Equal(t, scrapper.StringValue("12.50"), convertToRawValue(stringerType{s: "12.50"}, rawCol("DECIMAL(10,2)")))
}

// trino-go-client and the Athena driver label a zone-less timestamp with
// time.Local. It must keep its wall clock, not be converted to UTC.
func TestConvertToRawValue_ZonelessTimeKeepsWallClock(t *testing.T) {
	local := time.Date(2024, 3, 15, 10, 20, 30, 123456000, time.Local)
	got := convertToRawValue(local, rawCol("TIMESTAMP"))
	assert.Equal(t, scrapper.TimeValue(time.Date(2024, 3, 15, 10, 20, 30, 123456000, time.UTC)), got)
}

func TestConvertToRawValue_TimeStillTyped(t *testing.T) {
	// time.Time is a Stringer but must keep going through convertToScrapperValue
	// so it lands as TimeValue, not a string.
	ts := time.Date(2026, 5, 11, 8, 0, 3, 0, time.UTC)
	got := convertToRawValue(ts, rawCol(""))
	_, ok := got.(scrapper.TimeValue)
	assert.True(t, ok, "expected TimeValue, got %T", got)
}

type stringerType struct{ s string }

func (s stringerType) String() string { return s.s }

type opaqueType struct{ n int }

// rawCol is the column RunRawQuery would describe for nativeType, with the
// kind any dialect gives it.
func rawCol(nativeType string) *scrapper.QueryShapeColumn {
	return &scrapper.QueryShapeColumn{NativeType: nativeType, Kind: scrapper.NativeValueKind("", nativeType)}
}
