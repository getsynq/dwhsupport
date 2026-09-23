package scrapper

import (
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSqlLiteral_PerDialect pins the literal every dialect writes for each
// kind. What makes them right is checked on real engines by
// scrappertest.ValueRoundTripSuite; this keeps them from moving by accident.
func TestSqlLiteral_PerDialect(t *testing.T) {
	ts := TimeValue(time.Date(2024, 3, 15, 10, 20, 30, 123456789, time.UTC))
	date := TimeValue(time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC))
	cases := []struct {
		kind ValueKind
		v    Value
	}{
		{KindTimestamp, ts},
		{KindTimestampTz, ts},
		{KindDate, date},
		{KindNumeric, StringValue("12345678901234.567890")},
		{KindNumeric, IntValue(-42)},
		{KindInteger, NewBigIntValue(new(big.Int).Lsh(big.NewInt(1), 70))},
		{KindFloat, DoubleValue(12.5)},
		{KindText, StringValue(`it's a back\slash`)},
		{KindUUID, StringValue("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")},
	}

	for _, dialect := range sqldialect.DialectsToTest() {
		t.Run(dialect.Name, func(t *testing.T) {
			var out strings.Builder
			for _, c := range cases {
				lit, err := SqlLiteral(dialect.Dialect, c.kind, c.v)
				require.NoError(t, err)
				out.WriteString(string(c.kind) + ": " + lit + "\n")
			}
			snaps.MatchSnapshot(t, out.String())
		})
	}
}

func TestSqlLiteral_Refuses(t *testing.T) {
	d := sqldialect.NewPostgresDialect()
	for _, tc := range []struct {
		kind ValueKind
		v    Value
	}{
		{KindUnknown, nil},
		{KindUnknown, IgnoredValue{}},
		{KindUnknown, JsonValue(`[1]`)},
		{KindNumeric, StringValue("1e10")},
		{KindNumeric, StringValue("1; DROP TABLE x")},
		{KindInteger, StringValue("abc")},
		{KindTimestamp, StringValue("not a time")},
		{KindText, TimeValue(time.Now())},
	} {
		_, err := SqlLiteral(d, tc.kind, tc.v)
		assert.Errorf(t, err, "%s %T %v", tc.kind, tc.v, tc.v)
	}
}

// TestSqlLiteral_KeepsFractionalSeconds: a cutoff watermark written back as
// 2006-01-02 15:04:05 moves by up to a second and loses its zone.
func TestSqlLiteral_KeepsFractionalSeconds(t *testing.T) {
	ts := TimeValue(time.Date(2024, 3, 15, 10, 20, 30, 123456000, time.UTC))
	lit, err := SqlLiteral(sqldialect.NewPostgresDialect(), KindTimestampTz, ts)
	require.NoError(t, err)
	assert.Equal(t, "TIMESTAMPTZ '2024-03-15 10:20:30.123456+00:00'", lit)
}
