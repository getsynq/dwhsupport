package scrapper

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/pkg/errors"
)

// ValueKind is the family of warehouse types a column belongs to, as far as
// reading its values and writing them back as literals is concerned. It is
// what separates cases a Value alone cannot: a TimeValue from a DATE column
// needs a date literal, and a StringValue from a UUID or NUMERIC column needs
// a literal of that type on the engines that will not compare it with text.
type ValueKind string

const (
	KindUnknown ValueKind = ""
	// KindTimestamp is a timestamp without a zone (TIMESTAMP_NTZ, DATETIME,
	// DATETIME2, Postgres TIMESTAMP). Its TimeValue is the wall clock, in UTC.
	KindTimestamp ValueKind = "timestamp"
	// KindTimestampTz is an instant (TIMESTAMPTZ, TIMESTAMP_TZ/_LTZ,
	// DATETIMEOFFSET, BigQuery TIMESTAMP, ClickHouse DateTime).
	KindTimestampTz ValueKind = "timestamptz"
	KindDate        ValueKind = "date"
	KindInteger     ValueKind = "integer"
	// KindNumeric is an exact decimal (NUMERIC, DECIMAL, NUMBER). A fractional
	// value arrives as StringValue holding its exact digits.
	KindNumeric ValueKind = "numeric"
	KindFloat   ValueKind = "float"
	KindBool    ValueKind = "bool"
	KindText    ValueKind = "text"
	KindUUID    ValueKind = "uuid"
)

// NativeValueKind maps a column's native type, as a driver reports it
// (QueryShapeColumn.NativeType from RunRawQuery or QueryShape), to its
// ValueKind. dialect is Scrapper.DialectType(), because the same name means
// different things on different engines: TIMESTAMP is an instant on BigQuery
// and Databricks and a wall clock everywhere else, and DATE carries a time of
// day on Oracle.
//
// Type parameters and ClickHouse's Nullable/LowCardinality wrappers are
// ignored, so "DECIMAL(20,6)", "timestamp(6) with time zone" and
// "Nullable(DateTime64(6, 'UTC'))" all resolve. An unrecognised type is
// KindUnknown.
func NativeValueKind(dialect string, nativeType string) ValueKind {
	t := normalizeNativeType(nativeType)
	switch dialect {
	case "bigquery":
		switch t {
		case "TIMESTAMP":
			return KindTimestampTz
		case "DATETIME":
			return KindTimestamp
		case "FLOAT":
			return KindFloat
		}
	case "databricks":
		if t == "TIMESTAMP" {
			return KindTimestampTz
		}
	case "clickhouse":
		switch t {
		case "DATETIME", "DATETIME64":
			return KindTimestampTz
		case "BOOL":
			return KindBool
		}
	case "oracle":
		switch t {
		case "DATE":
			return KindTimestamp
		case "FLOAT":
			return KindNumeric
		}
	case "db2":
		switch t {
		case "DECFLOAT":
			return KindNumeric
		case "GRAPHIC", "VARGRAPHIC", "LONG VARCHAR", "DBCLOB", "XML":
			return KindText
		}
	case "mssql", "fabric":
		switch t {
		case "BIT":
			return KindBool
		case "DATETIME", "SMALLDATETIME":
			return KindTimestamp
		}
	}
	if kind, ok := nativeTypeKinds[t]; ok {
		return kind
	}
	if strings.HasPrefix(t, "UNSIGNED ") {
		return NativeValueKind(dialect, t[len("UNSIGNED "):])
	}
	return KindUnknown
}

// normalizeNativeType upper-cases a type name and strips what does not change
// its kind: parameters in parentheses, and ClickHouse's Nullable(...) and
// LowCardinality(...) wrappers.
func normalizeNativeType(nativeType string) string {
	t := strings.TrimSpace(nativeType)
	for {
		upper := strings.ToUpper(t)
		switch {
		case strings.HasPrefix(upper, "NULLABLE(") && strings.HasSuffix(t, ")"):
			t = t[len("NULLABLE(") : len(t)-1]
		case strings.HasPrefix(upper, "LOWCARDINALITY(") && strings.HasSuffix(t, ")"):
			t = t[len("LOWCARDINALITY(") : len(t)-1]
		default:
			var b strings.Builder
			depth := 0
			for _, r := range upper {
				switch {
				case r == '(':
					depth++
				case r == ')':
					depth--
				case depth == 0:
					b.WriteRune(r)
				}
			}
			return strings.Join(strings.Fields(b.String()), " ")
		}
	}
}

// nativeTypeKinds holds the names that mean the same on every engine that
// uses them. Engine-specific meanings are in NativeValueKind.
var nativeTypeKinds = map[string]ValueKind{
	// Zone-less timestamps.
	"TIMESTAMP":                   KindTimestamp,
	"TIMESTAMP WITHOUT TIME ZONE": KindTimestamp,
	"TIMESTAMP_NTZ":               KindTimestamp,
	"TIMESTAMPNTZ":                KindTimestamp,
	"TIMESTAMP_S":                 KindTimestamp,
	"TIMESTAMP_MS":                KindTimestamp,
	"TIMESTAMP_NS":                KindTimestamp,
	"DATETIME":                    KindTimestamp,
	"DATETIME2":                   KindTimestamp,
	"SMALLDATETIME":               KindTimestamp,
	"TIMESTAMPDTY":                KindTimestamp, // go-ora
	// Instants.
	"TIMESTAMPTZ":                    KindTimestampTz,
	"TIMESTAMP WITH TIME ZONE":       KindTimestampTz,
	"TIMESTAMP WITH LOCAL TIME ZONE": KindTimestampTz,
	"TIMESTAMP_TZ":                   KindTimestampTz,
	"TIMESTAMP_LTZ":                  KindTimestampTz,
	"TIMESTAMPTZ_DTY":                KindTimestampTz, // go-ora
	"TIMESTAMPLTZ_DTY":               KindTimestampTz, // go-ora
	"DATETIMEOFFSET":                 KindTimestampTz,
	// Dates.
	"DATE":   KindDate,
	"DATE32": KindDate,
	// Integers.
	"TINYINT": KindInteger, "SMALLINT": KindInteger, "MEDIUMINT": KindInteger, "INT": KindInteger,
	"INTEGER": KindInteger, "BIGINT": KindInteger, "INT2": KindInteger, "INT4": KindInteger,
	"INT8": KindInteger, "INT16": KindInteger, "INT32": KindInteger, "INT64": KindInteger,
	"INT128": KindInteger, "INT256": KindInteger, "UINT8": KindInteger, "UINT16": KindInteger,
	"UINT32": KindInteger, "UINT64": KindInteger, "UINT128": KindInteger, "UINT256": KindInteger,
	"HUGEINT": KindInteger, "UHUGEINT": KindInteger, "UTINYINT": KindInteger, "USMALLINT": KindInteger,
	"UINTEGER": KindInteger, "UBIGINT": KindInteger, "BYTEINT": KindInteger, "SERIAL": KindInteger,
	"BIGSERIAL": KindInteger,
	// Exact decimals.
	"NUMERIC": KindNumeric, "DECIMAL": KindNumeric, "DEC": KindNumeric, "NUMBER": KindNumeric,
	"FIXED": KindNumeric, "BIGNUMERIC": KindNumeric, "BIGDECIMAL": KindNumeric,
	"DECIMAL32": KindNumeric, "DECIMAL64": KindNumeric, "DECIMAL128": KindNumeric, "DECIMAL256": KindNumeric,
	"MONEY": KindNumeric, "SMALLMONEY": KindNumeric,
	// Floats.
	"FLOAT": KindFloat, "FLOAT4": KindFloat, "FLOAT8": KindFloat, "REAL": KindFloat,
	"DOUBLE": KindFloat, "DOUBLE PRECISION": KindFloat, "FLOAT32": KindFloat, "FLOAT64": KindFloat,
	"BINARY_FLOAT": KindFloat, "BINARY_DOUBLE": KindFloat, "IBFLOAT": KindFloat, "IBDOUBLE": KindFloat,
	// Booleans.
	"BOOL": KindBool, "BOOLEAN": KindBool,
	// Text.
	"TEXT": KindText, "VARCHAR": KindText, "CHAR": KindText, "NVARCHAR": KindText, "NCHAR": KindText,
	"STRING": KindText, "CHARACTER VARYING": KindText, "CHARACTER": KindText, "BPCHAR": KindText,
	"NTEXT": KindText, "VARCHAR2": KindText, "NVARCHAR2": KindText, "CLOB": KindText, "NCLOB": KindText,
	"FIXEDSTRING": KindText, "TINYTEXT": KindText, "MEDIUMTEXT": KindText, "LONGTEXT": KindText,
	"NAME": KindText, "CITEXT": KindText,
	// UUIDs.
	"UUID": KindUUID, "UNIQUEIDENTIFIER": KindUUID,
}

// SqlLiteral writes a value read from a column of the given kind back into
// SQL as a literal for dialect d, such that `column = literal` holds on the
// warehouse the value came from. The kind is QueryShapeColumn.Kind of the
// column the value was read from, as RunRawQuery and QueryShape report it.
//
// With KindUnknown the literal follows the Value alone: a TimeValue becomes a
// zone-less timestamp and a StringValue a string. That is right for text and
// numbers and for timestamps on the engines that coerce, and wrong for a
// DATE, UUID or exact NUMERIC column on those that do not (BigQuery, Trino).
//
// It fails for nil, IgnoredValue and JsonValue, and for a value that does not
// fit the kind (text in an integer column), rather than writing a literal
// that compares with something else.
func SqlLiteral(d sqldialect.Dialect, kind ValueKind, v Value) (string, error) {
	switch t := v.(type) {
	case nil:
		return "", errors.New("no literal for a nil value")
	case IgnoredValue:
		return "", errors.New("no literal for a value that was not read (IgnoredValue)")
	case JsonValue:
		return "", errors.New("no literal for a nested value (JsonValue)")
	case TimeValue:
		switch kind {
		case KindDate:
			return d.DateLiteral(time.Time(t)), nil
		case KindTimestampTz:
			return d.TimestampTzLiteral(time.Time(t)), nil
		case KindTimestamp, KindUnknown:
			return d.TimestampLiteral(time.Time(t)), nil
		}
	case IntValue:
		switch kind {
		case KindNumeric:
			return d.NumericLiteral(strconv.FormatInt(int64(t), 10))
		case KindInteger, KindFloat, KindBool, KindUnknown:
			return strconv.FormatInt(int64(t), 10), nil
		}
	case *BigIntValue:
		if t == nil {
			return "", errors.New("no literal for a nil value")
		}
		switch kind {
		case KindInteger, KindNumeric, KindUnknown:
			return d.NumericLiteral(t.String())
		}
	case DoubleValue:
		f := float64(t)
		if math.IsInf(f, 0) || math.IsNaN(f) {
			return "", errors.Errorf("no literal for %v", f)
		}
		switch kind {
		case KindFloat, KindUnknown:
			return strconv.FormatFloat(f, 'g', -1, 64), nil
		case KindNumeric:
			return d.NumericLiteral(strconv.FormatFloat(f, 'f', -1, 64))
		}
	case StringValue:
		switch kind {
		case KindNumeric, KindInteger:
			return d.NumericLiteral(string(t))
		case KindUUID:
			return d.UUIDLiteral(string(t)), nil
		case KindDate, KindTimestamp, KindTimestampTz:
			parsed, ok := ParseTimestamp(string(t))
			if !ok {
				return "", errors.Errorf("%q is not a timestamp", string(t))
			}
			return SqlLiteral(d, kind, TimeValue(parsed))
		case KindText, KindUnknown:
			return d.StringLiteral(string(t)), nil
		}
	default:
		return "", errors.Errorf("no literal for %T", v)
	}
	return "", errors.Errorf("no %s literal for %T", kind, v)
}
