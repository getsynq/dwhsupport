package scrappertest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"time"

	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/stretchr/testify/suite"
)

// ValueKind names a family of warehouse types the ValueRoundTripSuite pushes a
// value of through a real warehouse.
type ValueKind string

const (
	KindTimestamp   ValueKind = "timestamp"
	KindTimestampTz ValueKind = "timestamptz"
	KindDate        ValueKind = "date"
	KindNumeric     ValueKind = "numeric"
	KindBigInt      ValueKind = "bigint"
	KindDouble      ValueKind = "double"
	KindText        ValueKind = "text"
	KindUUID        ValueKind = "uuid"
)

// The values every warehouse is asked to produce. They are chosen to catch the
// ways a value is quietly lost on the way through:
//
//   - the timestamps carry microseconds, which a seconds-only literal drops;
//   - the timestamptz is written at +02:00, so a reader that ignores the offset
//     is two hours out;
//   - the numeric has 20 significant digits and the bigint is 2^53+1, neither
//     of which survives a float64;
//   - the text has a quote and a backslash, which the dialects escape
//     differently.
var (
	roundTripTimestamp = time.Date(2024, 3, 15, 10, 20, 30, 123456000, time.UTC)
	roundTripDate      = time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	roundTripNumeric   = "12345678901234.567890"
	roundTripBigInt    = int64(9007199254740993)
	roundTripDouble    = 12.5
	roundTripText      = `it's a back\slash`
	roundTripUUID      = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
)

// roundTripExprs is, per dialect, the native SQL that produces each value
// above. A kind a dialect has no type for is left out and its tests skip.
//
// The timestamptz expressions are written at +02:00 and must all denote
// roundTripTimestamp; the timestamp expressions are wall clock with no zone.
var roundTripExprs = map[string]map[ValueKind]string{
	"postgres": {
		KindTimestamp:   `TIMESTAMP '2024-03-15 10:20:30.123456'`,
		KindTimestampTz: `TIMESTAMPTZ '2024-03-15 12:20:30.123456+02:00'`,
		KindDate:        `DATE '2024-03-15'`,
		KindNumeric:     `CAST('12345678901234.567890' AS NUMERIC(20,6))`,
		KindBigInt:      `CAST(9007199254740993 AS BIGINT)`,
		KindDouble:      `CAST(12.5 AS DOUBLE PRECISION)`,
		KindText:        `'it''s a back\slash'`,
		KindUUID:        `CAST('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' AS UUID)`,
	},
	"redshift": {
		KindTimestamp:   `CAST('2024-03-15 10:20:30.123456' AS TIMESTAMP)`,
		KindTimestampTz: `CAST('2024-03-15 12:20:30.123456+02:00' AS TIMESTAMPTZ)`,
		KindDate:        `CAST('2024-03-15' AS DATE)`,
		KindNumeric:     `CAST('12345678901234.567890' AS NUMERIC(20,6))`,
		KindBigInt:      `CAST(9007199254740993 AS BIGINT)`,
		KindDouble:      `CAST(12.5 AS DOUBLE PRECISION)`,
		KindText:        `'it''s a back\\slash'`,
	},
	"snowflake": {
		KindTimestamp:   `TO_TIMESTAMP_NTZ('2024-03-15 10:20:30.123456')`,
		KindTimestampTz: `TO_TIMESTAMP_TZ('2024-03-15 12:20:30.123456 +02:00')`,
		KindDate:        `DATE '2024-03-15'`,
		KindNumeric:     `CAST('12345678901234.567890' AS NUMBER(20,6))`,
		KindBigInt:      `CAST(9007199254740993 AS BIGINT)`,
		KindDouble:      `CAST(12.5 AS DOUBLE)`,
		KindText:        `'it''s a back\\slash'`,
	},
	"bigquery": {
		KindTimestamp:   `DATETIME '2024-03-15 10:20:30.123456'`,
		KindTimestampTz: `TIMESTAMP '2024-03-15 12:20:30.123456+02:00'`,
		KindDate:        `DATE '2024-03-15'`,
		KindNumeric:     `NUMERIC '12345678901234.567890'`,
		KindBigInt:      `CAST(9007199254740993 AS INT64)`,
		KindDouble:      `CAST(12.5 AS FLOAT64)`,
		KindText:        `'it\'s a back\\slash'`,
	},
	"databricks": {
		KindTimestamp:   `TIMESTAMP_NTZ '2024-03-15 10:20:30.123456'`,
		KindTimestampTz: `TIMESTAMP '2024-03-15 12:20:30.123456+02:00'`,
		KindDate:        `DATE '2024-03-15'`,
		KindNumeric:     `CAST('12345678901234.567890' AS DECIMAL(20,6))`,
		KindBigInt:      `CAST(9007199254740993 AS BIGINT)`,
		KindDouble:      `CAST(12.5 AS DOUBLE)`,
		KindText:        `'it\'s a back\\slash'`,
	},
	"clickhouse": {
		KindTimestamp:   `toDateTime64('2024-03-15 10:20:30.123456', 6, 'UTC')`,
		KindTimestampTz: `parseDateTime64BestEffort('2024-03-15 12:20:30.123456+02:00', 6)`,
		KindDate:        `toDate('2024-03-15')`,
		KindNumeric:     `toDecimal128('12345678901234.567890', 6)`,
		KindBigInt:      `toInt64(9007199254740993)`,
		KindDouble:      `toFloat64(12.5)`,
		KindText:        `'it\'s a back\\slash'`,
		KindUUID:        `toUUID('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')`,
	},
	"duckdb": {
		KindTimestamp:   `TIMESTAMP '2024-03-15 10:20:30.123456'`,
		KindTimestampTz: `TIMESTAMPTZ '2024-03-15 12:20:30.123456+02:00'`,
		KindDate:        `DATE '2024-03-15'`,
		KindNumeric:     `CAST('12345678901234.567890' AS DECIMAL(20,6))`,
		KindBigInt:      `CAST(9007199254740993 AS BIGINT)`,
		KindDouble:      `CAST(12.5 AS DOUBLE)`,
		KindText:        `'it''s a back\slash'`,
		KindUUID:        `CAST('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' AS UUID)`,
	},
	"mysql": {
		KindTimestamp: `CAST('2024-03-15 10:20:30.123456' AS DATETIME(6))`,
		KindDate:      `DATE '2024-03-15'`,
		KindNumeric:   `CAST('12345678901234.567890' AS DECIMAL(20,6))`,
		KindBigInt:    `CAST(9007199254740993 AS SIGNED)`,
		KindDouble:    `CAST(12.5 AS DOUBLE)`,
		KindText:      `'it\'s a back\\slash'`,
	},
	"mssql": {
		KindTimestamp:   `CAST('2024-03-15 10:20:30.123456' AS DATETIME2(6))`,
		KindTimestampTz: `CAST('2024-03-15 12:20:30.123456 +02:00' AS DATETIMEOFFSET(6))`,
		KindDate:        `CAST('2024-03-15' AS DATE)`,
		KindNumeric:     `CAST('12345678901234.567890' AS DECIMAL(20,6))`,
		KindBigInt:      `CAST(9007199254740993 AS BIGINT)`,
		KindDouble:      `CAST(12.5 AS FLOAT)`,
		KindText:        `N'it''s a back\slash'`,
		KindUUID:        `CAST('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' AS UNIQUEIDENTIFIER)`,
	},
	"oracle": {
		KindTimestamp:   `TIMESTAMP '2024-03-15 10:20:30.123456'`,
		KindTimestampTz: `TIMESTAMP '2024-03-15 12:20:30.123456 +02:00'`,
		KindDate:        `DATE '2024-03-15'`,
		KindNumeric:     `CAST(12345678901234.567890 AS NUMBER(20,6))`,
		KindBigInt:      `CAST(9007199254740993 AS NUMBER(19))`,
		KindDouble:      `CAST(12.5 AS BINARY_DOUBLE)`,
		KindText:        `'it''s a back\slash'`,
	},
	"trino": {
		KindTimestamp:   `TIMESTAMP '2024-03-15 10:20:30.123456'`,
		KindTimestampTz: `TIMESTAMP '2024-03-15 12:20:30.123456 +02:00'`,
		KindDate:        `DATE '2024-03-15'`,
		KindNumeric:     `CAST('12345678901234.567890' AS DECIMAL(20,6))`,
		KindBigInt:      `CAST(9007199254740993 AS BIGINT)`,
		KindDouble:      `CAST(12.5 AS DOUBLE)`,
		KindText:        `'it''s a back\slash'`,
		KindUUID:        `CAST('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' AS UUID)`,
	},
}

func init() {
	roundTripExprs["fabric"] = roundTripExprs["mssql"]
	roundTripExprs["athena"] = roundTripExprs["trino"]
}

// ValueRoundTripSuite pushes one value of each common type through a real
// warehouse and checks every step a consumer depends on:
//
//   - raw: RunRawQuery decodes the native value to the scrapper.Value it
//     stands for, timestamps as instants in UTC, numerics without loss;
//   - metrics: QueryCustomMetrics decodes it to a number or a time;
//   - text: the warehouse's own text rendering of a timestamp (what
//     CAST(... AS VARCHAR) returns) is read back as the same instant;
//   - literal: the decoded value, written back into SQL as a literal for this
//     dialect, compares equal to the original in the warehouse.
//
// Nothing is written: every value comes from a literal in a SELECT, so the
// suite needs a connection and nothing else. Embed it in a warehouse's
// integration tests and set Scrapper in SetupSuite.
type ValueRoundTripSuite struct {
	suite.Suite
	Scrapper scrapper.Scrapper
}

// Ctx returns a background context for use in SetupSuite of embedding suites.
func (s *ValueRoundTripSuite) Ctx() context.Context {
	return context.Background()
}

func (s *ValueRoundTripSuite) ctx() context.Context {
	return querycontext.WithQueryContext(context.Background(), complianceQueryContext)
}

func (s *ValueRoundTripSuite) TestValueRoundTrip_Timestamp()   { s.runKind(KindTimestamp) }
func (s *ValueRoundTripSuite) TestValueRoundTrip_TimestampTz() { s.runKind(KindTimestampTz) }
func (s *ValueRoundTripSuite) TestValueRoundTrip_Date()        { s.runKind(KindDate) }
func (s *ValueRoundTripSuite) TestValueRoundTrip_Numeric()     { s.runKind(KindNumeric) }
func (s *ValueRoundTripSuite) TestValueRoundTrip_BigInt()      { s.runKind(KindBigInt) }
func (s *ValueRoundTripSuite) TestValueRoundTrip_Double()      { s.runKind(KindDouble) }
func (s *ValueRoundTripSuite) TestValueRoundTrip_Text()        { s.runKind(KindText) }
func (s *ValueRoundTripSuite) TestValueRoundTrip_UUID()        { s.runKind(KindUUID) }

func (s *ValueRoundTripSuite) runKind(kind ValueKind) {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}
	expr, ok := roundTripExprs[s.Scrapper.DialectType()][kind]
	if !ok {
		s.T().Skipf("%s has no %s type", s.Scrapper.DialectType(), kind)
	}

	s.Run("raw", func() {
		cv := s.rawValue(s.selectValue(expr))
		s.Require().False(cv.IsNull, "value should not be null")
		s.assertDecoded(kind, cv.Value)
	})

	s.Run("metrics", func() {
		switch kind {
		case KindText, KindUUID:
			s.T().Skip("QueryCustomMetrics does not carry text")
		}
		cv := s.metricsValue(s.selectValue(expr))
		s.Require().False(cv.IsNull, "value should not be null")
		s.assertMetricsDecoded(kind, cv.Value)
	})

	s.Run("text", func() {
		switch kind {
		case KindTimestamp, KindTimestampTz, KindDate:
		default:
			s.T().Skip("only timestamps have a text form worth parsing")
		}
		asText, err := s.Scrapper.SqlDialect().ToString(sqldialect.Sql("v")).ToSql(s.Scrapper.SqlDialect())
		s.Require().NoError(err)
		cv := s.metricsValue(s.selectFrom(expr, asText+" AS v"))
		s.Require().False(cv.IsNull, "value should not be null")
		got, ok := cv.Value.(scrapper.TimeValue)
		s.Require().Truef(ok, "the text form should decode to a TimeValue, got %T %v", cv.Value, cv.Value)
		// The warehouse picks the precision of its own text rendering
		// (Snowflake's default is milliseconds), so only the instant is
		// checked, to the millisecond.
		s.WithinDuration(s.wantTime(kind), time.Time(got), time.Millisecond)
	})

	s.Run("literal", func() {
		cv := s.rawValue(s.selectValue(expr))
		s.Require().False(cv.IsNull, "value should not be null")
		literal, err := roundTripLiteral(s.Scrapper.SqlDialect(), kind, cv.Value)
		s.Require().NoError(err)
		s.T().Logf("literal: %s", literal)
		eq := s.rawValue(s.selectFrom(expr, fmt.Sprintf("CASE WHEN v = %s THEN 1 ELSE 0 END AS v", literal)))
		s.Require().False(eq.IsNull)
		s.Equalf(scrapper.IntValue(1), eq.Value, "the literal should compare equal to the value it was decoded from")
	})
}

func (s *ValueRoundTripSuite) wantTime(kind ValueKind) time.Time {
	if kind == KindDate {
		return roundTripDate
	}
	return roundTripTimestamp
}

// assertDecoded checks what RunRawQuery made of the value.
func (s *ValueRoundTripSuite) assertDecoded(kind ValueKind, v scrapper.Value) {
	s.T().Helper()
	switch kind {
	case KindTimestamp, KindTimestampTz, KindDate:
		got, ok := v.(scrapper.TimeValue)
		s.Require().Truef(ok, "want TimeValue, got %T %v", v, v)
		s.Truef(time.Time(got).Equal(s.wantTime(kind)), "want %s, got %s", s.wantTime(kind), time.Time(got))
		s.Equal(time.UTC, time.Time(got).Location(), "timestamps are decoded in UTC")
	case KindNumeric:
		s.assertExactNumber(roundTripNumeric, v)
	case KindBigInt:
		s.assertExactNumber(strconv.FormatInt(roundTripBigInt, 10), v)
	case KindDouble:
		s.Equal(scrapper.DoubleValue(roundTripDouble), v)
	case KindText:
		s.Equal(scrapper.StringValue(roundTripText), v)
	case KindUUID:
		s.Equal(scrapper.StringValue(roundTripUUID), v)
	}
}

// assertMetricsDecoded checks what QueryCustomMetrics made of the value. The
// metrics path is float64 for anything fractional, so the numeric is only
// checked to float precision.
func (s *ValueRoundTripSuite) assertMetricsDecoded(kind ValueKind, v scrapper.Value) {
	s.T().Helper()
	switch kind {
	case KindTimestamp, KindTimestampTz, KindDate:
		got, ok := v.(scrapper.TimeValue)
		s.Require().Truef(ok, "want TimeValue, got %T %v", v, v)
		s.Truef(time.Time(got).Equal(s.wantTime(kind)), "want %s, got %s", s.wantTime(kind), time.Time(got))
	case KindNumeric:
		got, ok := v.(scrapper.DoubleValue)
		s.Require().Truef(ok, "want DoubleValue, got %T %v", v, v)
		want, _ := strconv.ParseFloat(roundTripNumeric, 64)
		s.InDelta(want, float64(got), 0.01)
	case KindBigInt:
		s.Equal(scrapper.IntValue(roundTripBigInt), v)
	case KindDouble:
		s.Equal(scrapper.DoubleValue(roundTripDouble), v)
	}
}

func (s *ValueRoundTripSuite) assertExactNumber(want string, v scrapper.Value) {
	s.T().Helper()
	wantRat, _ := new(big.Rat).SetString(want)
	var got *big.Rat
	switch t := v.(type) {
	case scrapper.IntValue:
		got = new(big.Rat).SetInt64(int64(t))
	case *scrapper.BigIntValue:
		got = new(big.Rat).SetInt(t.BigInt())
	case scrapper.StringValue:
		got, _ = new(big.Rat).SetString(string(t))
	}
	s.Require().NotNilf(got, "want an exact number, got %T %v", v, v)
	s.Truef(got.Cmp(wantRat) == 0, "want %s, got %s", want, got.FloatString(6))
}

func (s *ValueRoundTripSuite) selectValue(expr string) string {
	return s.selectFrom(expr, "v")
}

// selectFrom reads outer from a derived table whose only column, v, is expr.
func (s *ValueRoundTripSuite) selectFrom(expr, outer string) string {
	inner := "SELECT " + expr + " AS v"
	if s.Scrapper.DialectType() == "oracle" {
		inner += " FROM dual"
	}
	tableAlias := " AS t"
	if !s.Scrapper.SqlDialect().SupportsAsBeforeTableAlias() {
		tableAlias = " t"
	}
	return "SELECT " + outer + " FROM (" + inner + ")" + tableAlias
}

func (s *ValueRoundTripSuite) rawValue(sql string) *scrapper.ColumnValue {
	s.T().Helper()
	s.T().Logf("SQL: %s", sql)
	it, err := s.Scrapper.RunRawQuery(s.ctx(), sql)
	if errors.Is(err, scrapper.ErrUnsupported) {
		s.T().Skip("RunRawQuery unsupported")
	}
	s.Require().NoError(err)
	defer it.Close()

	var cells []*scrapper.ColumnValue
	for {
		row, err := it.Next(s.ctx())
		if errors.Is(err, io.EOF) {
			break
		}
		s.Require().NoError(err)
		cells = append(cells, row...)
	}
	s.Require().Len(cells, 1, "one row with one column")
	s.T().Logf("raw: %T %v (native %s)", cells[0].Value, cells[0].Value, it.Columns()[0].NativeType)
	return cells[0]
}

func (s *ValueRoundTripSuite) metricsValue(sql string) *scrapper.ColumnValue {
	s.T().Helper()
	s.T().Logf("SQL: %s", sql)
	rows, err := s.Scrapper.QueryCustomMetrics(s.ctx(), sql)
	if errors.Is(err, scrapper.ErrUnsupported) {
		s.T().Skip("QueryCustomMetrics unsupported")
	}
	s.Require().NoError(err)
	s.Require().Len(rows, 1)
	s.Require().Len(rows[0].ColumnValues, 1)
	cv := rows[0].ColumnValues[0]
	s.T().Logf("metrics: %T %v", cv.Value, cv.Value)
	return cv
}

// roundTripLiteral writes a decoded value back into SQL with what the dialect
// offers today.
func roundTripLiteral(d sqldialect.Dialect, _ ValueKind, v scrapper.Value) (string, error) {
	switch t := v.(type) {
	case scrapper.TimeValue:
		return d.ResolveTime(time.Time(t))
	case scrapper.IntValue:
		return strconv.FormatInt(int64(t), 10), nil
	case *scrapper.BigIntValue:
		return t.String(), nil
	case scrapper.DoubleValue:
		return strconv.FormatFloat(float64(t), 'g', -1, 64), nil
	case scrapper.StringValue:
		return d.StringLiteral(string(t)), nil
	}
	return "", fmt.Errorf("no literal for %T", v)
}
