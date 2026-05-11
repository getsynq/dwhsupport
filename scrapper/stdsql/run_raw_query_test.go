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
			got := convertToRawValue(tc.in)
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

func TestConvertToRawValue_TimeStillTyped(t *testing.T) {
	// time.Time is a Stringer but must keep going through convertToScrapperValue
	// so it lands as TimeValue, not a string.
	ts := time.Date(2026, 5, 11, 8, 0, 3, 0, time.UTC)
	got := convertToRawValue(ts)
	_, ok := got.(scrapper.TimeValue)
	assert.True(t, ok, "expected TimeValue, got %T", got)
}

type stringerType struct{ s string }

func (s stringerType) String() string { return s.s }

type opaqueType struct{ n int }
