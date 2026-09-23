package scrapper

import (
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// This file is the one place outside the drivers that knows what a value read
// from a warehouse looks like. Consumers read a Value through the As*
// accessors and write one back into SQL through SqlLiteral (literal.go), so no
// consumer has to switch over driver types or parse warehouse text itself.

// NormalizeTime returns t as an instant in UTC, the form every TimeValue
// carries.
//
// A driver that has no zone for a value (a zone-less TIMESTAMP or a DATE)
// either labels it UTC (most drivers) or labels it with the process's local
// zone (trino-go-client, the Athena driver, go-sql-driver/mysql with
// loc=Local). Converting the second kind to UTC would move its wall clock by
// the host's offset, so a value labelled with time.Local is taken to be that
// wall clock in UTC instead. No driver we use labels a value it does know the
// zone of with time.Local: they build the zone from the value (FixedZone or
// LoadLocation), which is never the time.Local pointer.
func NormalizeTime(t time.Time) time.Time {
	if t.Location() == time.Local {
		return wallClockUTC(t)
	}
	return t.UTC()
}

// NormalizeTimeOfKind is NormalizeTime for a value whose column kind is
// known, which settles the question NormalizeTime has to guess at: a
// zone-less timestamp or date keeps its wall clock whatever zone the driver
// labelled it with, and an instant is converted to UTC. Any other kind falls
// back to NormalizeTime.
func NormalizeTimeOfKind(t time.Time, kind ValueKind) time.Time {
	switch kind {
	case KindTimestamp, KindDate:
		return wallClockUTC(t)
	case KindTimestampTz:
		return t.UTC()
	}
	return NormalizeTime(t)
}

func wallClockUTC(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
}

// timestampLayouts are the text forms the supported warehouses render a
// timestamp or date in, which is what CAST(x AS VARCHAR) hands back. Each is
// tried as written; a fractional second of any length is accepted after the
// seconds field because Go treats ".999999999" as optional digits.
//
//	RFC 3339 / ISO 8601         2024-03-15T10:20:30.123456Z, ...+02:00    Databricks, JSON
//	Postgres, DuckDB, Redshift  2024-03-15 10:20:30.123456+00             timestamptz
//	                            2024-03-15 10:20:30.123456+05:30
//	BigQuery                    2024-03-15 10:20:30.123456+00             TIMESTAMP
//	Snowflake                   2024-03-15 10:20:30.123 +0200             TIMESTAMP_TZ / _LTZ
//	                            2024-03-15 10:20:30.123 Z                 TIMESTAMP_TZ in UTC
//	MSSQL                       2024-03-15 10:20:30.1234560 +02:00        datetimeoffset
//	Trino, Athena               2024-03-15 10:20:30.123 UTC               timestamp with time zone
//	                            2024-03-15 10:20:30.123 +02:00
//	Oracle (NLS defaults)       15-MAR-24 10.20.30.123456 AM              TIMESTAMP
//	                            15-MAR-24 10.20.30.123456 AM +02:00       TIMESTAMP WITH TIME ZONE
//	zone-less, everywhere       2024-03-15 10:20:30.123456, 2024-03-15T10:20:30, 2024-03-15
//
// A zone named by region (Trino's "Europe/Warsaw") is handled separately in
// ParseTimestamp, because Go's layouts only accept abbreviations.
var timestampLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999Z0700",
	"2006-01-02 15:04:05.999999999Z07",
	"2006-01-02 15:04:05.999999999 Z07:00",
	"2006-01-02 15:04:05.999999999 Z0700",
	"2006-01-02 15:04:05.999999999 Z07",
	"2006-01-02 15:04:05.999999999 MST",
	"2006-01-02T15:04:05.999999999Z0700",
	"2006-01-02T15:04:05.999999999Z07",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999",
	"02-Jan-06 03.04.05.999999999 PM Z07:00",
	"02-Jan-06 03.04.05.999999999 PM",
	"02-Jan-06",
	time.DateOnly,
}

// ParseTimestamp reads a timestamp or date a warehouse rendered as text and
// returns it as an instant in UTC. Text that carries a zone or offset is
// converted to UTC; text without one is a wall clock and is read as UTC, which
// is how a zone-less timestamp is decoded from a driver too (NormalizeTime).
//
// It reports false for text that is not a timestamp in any form listed in
// timestampLayouts. It does not guess.
func ParseTimestamp(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if len(s) < len("02-Jan-06") || !startsWithDigit(s) {
		return time.Time{}, false
	}
	for _, layout := range timestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	// Trino renders a timestamp with time zone under the zone it was written
	// in, which may be a region name. Go's layouts cannot parse one, so split
	// it off and resolve it through the tz database.
	if idx := strings.LastIndexByte(s, ' '); idx > 0 && strings.ContainsRune(s[idx+1:], '/') {
		if loc, err := time.LoadLocation(s[idx+1:]); err == nil {
			if t, err := time.ParseInLocation("2006-01-02 15:04:05.999999999", s[:idx], loc); err == nil {
				return t.UTC(), true
			}
		}
	}
	return time.Time{}, false
}

func startsWithDigit(s string) bool {
	return s != "" && s[0] >= '0' && s[0] <= '9'
}

// MetricValueFromText is how the metrics path (QueryCustomMetrics) reads a
// cell a driver handed back as text: an integer, a float, a boolean (as 1/0)
// or a timestamp in any form ParseTimestamp knows. Anything else is
// IgnoredValue. The metrics path carries numbers and times only, and its
// consumers reject a StringValue.
func MetricValueFromText(s string) Value {
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return IntValue(v)
	}
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return DoubleValue(v)
	}
	if v, err := strconv.ParseBool(s); err == nil {
		if v {
			return IntValue(1)
		}
		return IntValue(0)
	}
	if t, ok := ParseTimestamp(s); ok {
		return TimeValue(t)
	}
	return IgnoredValue{}
}

// DecimalValueFromText decodes the text of an exact numeric (NUMERIC,
// DECIMAL, NUMBER, Snowflake's FIXED) without going through float64: an
// integer becomes IntValue, or BigIntValue when it does not fit, and anything
// with a fractional part stays StringValue with its digits exactly as the
// warehouse wrote them. Text that is not a plain decimal number (Postgres'
// 'NaN', an exponent) also stays StringValue.
func DecimalValueFromText(s string) Value {
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return IntValue(v)
	}
	if isPlainInteger(s) {
		if b, ok := new(big.Int).SetString(s, 10); ok {
			return NewBigIntValue(b)
		}
	}
	return StringValue(s)
}

// DecimalValueFromRat decodes an exact rational (BigQuery NUMERIC and
// BIGNUMERIC arrive as *big.Rat) the way DecimalValueFromText decodes text.
// The fraction is written with the fewest digits that represent it exactly,
// which for a base-10 warehouse decimal is at most its scale.
func DecimalValueFromRat(r *big.Rat) Value {
	if r.IsInt() {
		if r.Num().IsInt64() {
			return IntValue(r.Num().Int64())
		}
		return NewBigIntValue(new(big.Int).Set(r.Num()))
	}
	return StringValue(string(ratToJSONNumber(r)))
}

func isPlainInteger(s string) bool {
	if s == "" {
		return false
	}
	if s[0] == '-' || s[0] == '+' {
		s = s[1:]
	}
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// AsString renders a value as text: a StringValue or JsonValue verbatim, a
// number in its shortest exact form, a time as RFC 3339 in UTC with every
// fractional digit it carries. It reports false for nil and IgnoredValue,
// which mean the value was not read, so a caller can say so instead of
// writing "{}" or "<nil>".
func AsString(v Value) (string, bool) {
	switch t := v.(type) {
	case StringValue:
		return string(t), true
	case JsonValue:
		return string(t), true
	case IntValue:
		return strconv.FormatInt(int64(t), 10), true
	case *BigIntValue:
		if t == nil {
			return "", false
		}
		return t.String(), true
	case DoubleValue:
		return strconv.FormatFloat(float64(t), 'g', -1, 64), true
	case TimeValue:
		return time.Time(t).UTC().Format(time.RFC3339Nano), true
	}
	return "", false
}

// AsInt64 reads a value as an int64: an integer, a double with no fractional
// part, or text holding an integer. It reports false when the value is not a
// whole number or does not fit.
func AsInt64(v Value) (int64, bool) {
	switch t := v.(type) {
	case IntValue:
		return int64(t), true
	case *BigIntValue:
		if t != nil && t.BigInt().IsInt64() {
			return t.BigInt().Int64(), true
		}
	case DoubleValue:
		f := float64(t)
		if f == math.Trunc(f) && f >= math.MinInt64 && f < math.MaxInt64 {
			return int64(f), true
		}
	case StringValue:
		if i, err := strconv.ParseInt(strings.TrimSpace(string(t)), 10, 64); err == nil {
			return i, true
		}
	}
	return 0, false
}

// AsFloat64 reads a value as a float64: any number, or text holding one (an
// exact decimal arrives as StringValue). Precision beyond float64 is lost, so
// use AsBigRat when the digits matter.
func AsFloat64(v Value) (float64, bool) {
	switch t := v.(type) {
	case IntValue:
		return float64(t), true
	case DoubleValue:
		return float64(t), true
	case *BigIntValue:
		if t == nil {
			return 0, false
		}
		f, _ := new(big.Float).SetInt(t.BigInt()).Float64()
		return f, true
	case StringValue:
		if f, err := strconv.ParseFloat(strings.TrimSpace(string(t)), 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// AsBigRat reads a value as an exact rational: an integer, a double, or text
// holding a decimal number. This is the accessor for an exact numeric column,
// whose fractional values arrive as StringValue.
func AsBigRat(v Value) (*big.Rat, bool) {
	switch t := v.(type) {
	case IntValue:
		return new(big.Rat).SetInt64(int64(t)), true
	case *BigIntValue:
		if t == nil {
			return nil, false
		}
		return new(big.Rat).SetInt(t.BigInt()), true
	case DoubleValue:
		if math.IsInf(float64(t), 0) || math.IsNaN(float64(t)) {
			return nil, false
		}
		return new(big.Rat).SetFloat64(float64(t)), true
	case StringValue:
		if r, ok := new(big.Rat).SetString(strings.TrimSpace(string(t))); ok {
			return r, true
		}
	}
	return nil, false
}

// AsTime reads a value as an instant in UTC: a TimeValue, or text in any form
// ParseTimestamp knows.
func AsTime(v Value) (time.Time, bool) {
	switch t := v.(type) {
	case TimeValue:
		return time.Time(t).UTC(), true
	case StringValue:
		return ParseTimestamp(string(t))
	}
	return time.Time{}, false
}

// AsGo unwraps a value to the plain Go value it holds: int64, float64,
// *big.Int, string, time.Time (UTC), or the JSON text of a JsonValue as a
// string. nil and IgnoredValue give nil.
func AsGo(v Value) any {
	switch t := v.(type) {
	case IntValue:
		return int64(t)
	case DoubleValue:
		return float64(t)
	case *BigIntValue:
		if t == nil {
			return nil
		}
		return t.BigInt()
	case StringValue:
		return string(t)
	case JsonValue:
		return string(t)
	case TimeValue:
		return time.Time(t).UTC()
	}
	return nil
}
