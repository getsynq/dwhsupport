package scrapper

import (
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseTimestamp_WarehouseTextForms holds the text each warehouse
// returned for CAST(x AS VARCHAR) of 2024-03-15 10:20:30.123456 UTC, as
// captured by scrappertest.ValueRoundTripSuite, plus the forms documented for
// engines we could not run it against.
func TestParseTimestamp_WarehouseTextForms(t *testing.T) {
	instant := time.Date(2024, 3, 15, 10, 20, 30, 123456000, time.UTC)
	millis := time.Date(2024, 3, 15, 10, 20, 30, 123000000, time.UTC)
	date := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		source string
		text   string
		want   time.Time
	}{
		{"Postgres timestamptz", "2024-03-15 10:20:30.123456+00", instant},
		{"Postgres timestamptz, half-hour offset", "2024-03-15 15:50:30.123456+05:30", instant},
		{"Postgres timestamp", "2024-03-15 10:20:30.123456", instant},
		{"DuckDB timestamptz in a CET session", "2024-03-15 11:20:30.123456+01", instant},
		{"BigQuery TIMESTAMP", "2024-03-15 10:20:30.123456+00", instant},
		{"BigQuery DATETIME", "2024-03-15 10:20:30.123456", instant},
		{"Snowflake TIMESTAMP_TZ", "2024-03-15 12:20:30.123 +0200", millis},
		{"Snowflake TIMESTAMP_TZ in UTC", "2024-03-15 10:20:30.123 Z", millis},
		{"Snowflake TIMESTAMP_NTZ", "2024-03-15 10:20:30.123", millis},
		{"MSSQL datetimeoffset", "2024-03-15 12:20:30.1234560 +02:00", instant},
		{"Trino timestamp with time zone", "2024-03-15 12:20:30.123456 +02:00", instant},
		{"Trino timestamp with time zone, region", "2024-03-15 11:20:30.123456 Europe/Warsaw", instant},
		{"Athena timestamp with time zone", "2024-03-15 10:20:30.123456 UTC", instant},
		{"Oracle TIMESTAMP", "15-MAR-24 10.20.30.123456000 AM", instant},
		{"Oracle TIMESTAMP WITH TIME ZONE", "15-MAR-24 12.20.30.123456000 PM +02:00", instant},
		{"Oracle DATE", "15-MAR-24", date},
		{"Databricks / RFC 3339", "2024-03-15T10:20:30.123456Z", instant},
		{"RFC 3339 with offset", "2024-03-15T12:20:30.123456+02:00", instant},
		{"ISO without zone", "2024-03-15T10:20:30.123456", instant},
		{"date", "2024-03-15", date},
	}
	for _, tt := range tests {
		t.Run(tt.source, func(t *testing.T) {
			got, ok := ParseTimestamp(tt.text)
			require.Truef(t, ok, "%q should parse", tt.text)
			assert.Truef(t, tt.want.Equal(got), "want %s, got %s", tt.want, got)
			assert.Equal(t, time.UTC, got.Location())
		})
	}
}

func TestParseTimestamp_RejectsNonTimestamps(t *testing.T) {
	for _, text := range []string{"", "{}", "abc", "12345", "12.5", "2024", "not-a-date", "15-XYZ-24"} {
		_, ok := ParseTimestamp(text)
		assert.Falsef(t, ok, "%q should not parse", text)
	}
}

// TestMetricValueFromText_StaysNarrow pins what the metrics path reads out of
// text. It is kept to what it has always been, because every text form it
// learns to read is more customer data a monitor can take out. ParseTimestamp
// reads the rest, on the raw path.
func TestMetricValueFromText_StaysNarrow(t *testing.T) {
	want := time.Date(2024, 3, 15, 10, 20, 30, 123456000, time.UTC)
	assert.Equal(t, TimeValue(want), MetricValueFromText("2024-03-15T10:20:30.123456Z"))
	assert.Equal(t, TimeValue(want), MetricValueFromText("2024-03-15 10:20:30.123456"))
	for _, text := range []string{"2024-03-15 10:20:30.123456+00", "2024-03-15", "15-MAR-24", "hello"} {
		assert.Equalf(t, IgnoredValue{}, MetricValueFromText(text), "%q", text)
	}
	assert.Equal(t, IntValue(42), MetricValueFromText("42"))
	assert.Equal(t, DoubleValue(1.5), MetricValueFromText("1.5"))
	assert.Equal(t, IntValue(1), MetricValueFromText("true"))
	assert.Equal(t, IgnoredValue{}, MetricValueFromText("hello"))
}

func TestNormalizeTime(t *testing.T) {
	warsaw, err := time.LoadLocation("Europe/Warsaw")
	require.NoError(t, err)

	// An instant keeps its instant.
	zoned := time.Date(2024, 3, 15, 11, 20, 30, 0, warsaw)
	assert.Equal(t, time.Date(2024, 3, 15, 10, 20, 30, 0, time.UTC), NormalizeTime(zoned))

	// A value a driver labelled with time.Local keeps its wall clock.
	local := time.Date(2024, 3, 15, 10, 20, 30, 0, time.Local)
	assert.Equal(t, time.Date(2024, 3, 15, 10, 20, 30, 0, time.UTC), NormalizeTime(local))
}

func TestNormalizeTimeOfKind(t *testing.T) {
	warsaw, err := time.LoadLocation("Europe/Warsaw")
	require.NoError(t, err)
	labelled := time.Date(2024, 3, 15, 11, 20, 30, 0, warsaw)

	// A zone-less column keeps the wall clock, whatever zone the driver chose.
	assert.Equal(t, time.Date(2024, 3, 15, 11, 20, 30, 0, time.UTC), NormalizeTimeOfKind(labelled, KindTimestamp))
	assert.Equal(t, time.Date(2024, 3, 15, 11, 20, 30, 0, time.UTC), NormalizeTimeOfKind(labelled, KindDate))
	// An instant is converted.
	assert.Equal(t, time.Date(2024, 3, 15, 10, 20, 30, 0, time.UTC), NormalizeTimeOfKind(labelled, KindTimestampTz))
	// Without a kind it falls back to NormalizeTime.
	assert.Equal(t, time.Date(2024, 3, 15, 10, 20, 30, 0, time.UTC), NormalizeTimeOfKind(labelled, KindUnknown))
}

func TestDecimalValueFromText(t *testing.T) {
	assert.Equal(t, IntValue(9007199254740993), DecimalValueFromText("9007199254740993"))
	assert.Equal(t, StringValue("12345678901234.567890"), DecimalValueFromText("12345678901234.567890"))
	assert.Equal(t, StringValue("NaN"), DecimalValueFromText("NaN"))
	big38 := DecimalValueFromText("12345678901234567890123456789012345678")
	require.IsType(t, &BigIntValue{}, big38)
	assert.Equal(t, "12345678901234567890123456789012345678", big38.(*BigIntValue).String())
}

func TestDecimalValueFromRat(t *testing.T) {
	r, _ := new(big.Rat).SetString("12345678901234.567890")
	assert.Equal(t, StringValue("12345678901234.56789"), DecimalValueFromRat(r))
	assert.Equal(t, IntValue(7), DecimalValueFromRat(big.NewRat(7, 1)))
}

func TestAccessors(t *testing.T) {
	ts := time.Date(2024, 3, 15, 10, 20, 30, 123456000, time.UTC)

	s, ok := AsString(TimeValue(ts))
	assert.True(t, ok)
	assert.Equal(t, "2024-03-15T10:20:30.123456Z", s)
	_, ok = AsString(IgnoredValue{})
	assert.False(t, ok, "IgnoredValue is not a value, not \"{}\"")

	i, ok := AsInt64(StringValue("42"))
	assert.True(t, ok)
	assert.Equal(t, int64(42), i)
	_, ok = AsInt64(DoubleValue(1.5))
	assert.False(t, ok)

	f, ok := AsFloat64(StringValue("12.5"))
	assert.True(t, ok)
	assert.Equal(t, 12.5, f)

	r, ok := AsBigRat(StringValue("12345678901234.567890"))
	assert.True(t, ok)
	assert.Equal(t, "12345678901234.567890", r.FloatString(6))

	got, ok := AsTime(StringValue("2024-03-15 12:20:30.123456 +02:00"))
	assert.True(t, ok)
	assert.True(t, ts.Equal(got))

	assert.Equal(t, int64(3), AsGo(IntValue(3)))
	assert.Nil(t, AsGo(IgnoredValue{}))
}

func TestNativeValueKind(t *testing.T) {
	tests := []struct {
		dialect, nativeType string
		want                ValueKind
	}{
		{"postgres", "TIMESTAMP", KindTimestamp},
		{"postgres", "TIMESTAMPTZ", KindTimestampTz},
		{"postgres", "timestamp(6) with time zone", KindTimestampTz},
		{"bigquery", "TIMESTAMP", KindTimestampTz},
		{"bigquery", "DATETIME", KindTimestamp},
		{"bigquery", "FLOAT", KindFloat},
		{"databricks", "TIMESTAMP", KindTimestampTz},
		{"databricks", "TIMESTAMP_NTZ", KindTimestamp},
		{"clickhouse", "DateTime64(6, 'UTC')", KindTimestampTz},
		{"clickhouse", "Nullable(DateTime)", KindTimestampTz},
		{"clickhouse", "LowCardinality(Nullable(String))", KindText},
		{"clickhouse", "Decimal(38, 6)", KindNumeric},
		{"mysql", "DATETIME", KindTimestamp},
		{"mysql", "UNSIGNED BIGINT", KindInteger},
		{"oracle", "DATE", KindTimestamp},
		{"oracle", "TimeStampTZ_DTY", KindTimestampTz},
		{"mssql", "DATETIMEOFFSET", KindTimestampTz},
		{"mssql", "UNIQUEIDENTIFIER", KindUUID},
		{"mssql", "BIT", KindBool},
		{"snowflake", "FIXED", KindNumeric},
		{"snowflake", "TIMESTAMP_LTZ", KindTimestampTz},
		{"trino", "TIMESTAMP WITH TIME ZONE", KindTimestampTz},
		{"athena", "decimal", KindNumeric},
		{"duckdb", "DECIMAL(20,6)", KindNumeric},
		{"duckdb", "GEOMETRY", KindUnknown},
	}
	for _, tt := range tests {
		assert.Equalf(t, tt.want, NativeValueKind(tt.dialect, tt.nativeType), "%s %s", tt.dialect, tt.nativeType)
	}
}
