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
var roundTripExprs = map[string]map[scrapper.ValueKind]string{
	"postgres": {
		scrapper.KindTimestamp:   `TIMESTAMP '2024-03-15 10:20:30.123456'`,
		scrapper.KindTimestampTz: `TIMESTAMPTZ '2024-03-15 12:20:30.123456+02:00'`,
		scrapper.KindDate:        `DATE '2024-03-15'`,
		scrapper.KindNumeric:     `CAST('12345678901234.567890' AS NUMERIC(20,6))`,
		scrapper.KindInteger:     `CAST(9007199254740993 AS BIGINT)`,
		scrapper.KindFloat:       `CAST(12.5 AS DOUBLE PRECISION)`,
		scrapper.KindText:        `'it''s a back\slash'`,
		scrapper.KindUUID:        `CAST('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' AS UUID)`,
	},
	"redshift": {
		scrapper.KindTimestamp:   `CAST('2024-03-15 10:20:30.123456' AS TIMESTAMP)`,
		scrapper.KindTimestampTz: `CAST('2024-03-15 12:20:30.123456+02:00' AS TIMESTAMPTZ)`,
		scrapper.KindDate:        `CAST('2024-03-15' AS DATE)`,
		scrapper.KindNumeric:     `CAST('12345678901234.567890' AS NUMERIC(20,6))`,
		scrapper.KindInteger:     `CAST(9007199254740993 AS BIGINT)`,
		scrapper.KindFloat:       `CAST(12.5 AS DOUBLE PRECISION)`,
		scrapper.KindText:        `'it''s a back\\slash'`,
	},
	"snowflake": {
		scrapper.KindTimestamp:   `TO_TIMESTAMP_NTZ('2024-03-15 10:20:30.123456')`,
		scrapper.KindTimestampTz: `TO_TIMESTAMP_TZ('2024-03-15 12:20:30.123456 +02:00')`,
		scrapper.KindDate:        `DATE '2024-03-15'`,
		scrapper.KindNumeric:     `CAST('12345678901234.567890' AS NUMBER(20,6))`,
		scrapper.KindInteger:     `CAST(9007199254740993 AS BIGINT)`,
		scrapper.KindFloat:       `CAST(12.5 AS DOUBLE)`,
		scrapper.KindText:        `'it''s a back\\slash'`,
	},
	"bigquery": {
		scrapper.KindTimestamp:   `DATETIME '2024-03-15 10:20:30.123456'`,
		scrapper.KindTimestampTz: `TIMESTAMP '2024-03-15 12:20:30.123456+02:00'`,
		scrapper.KindDate:        `DATE '2024-03-15'`,
		scrapper.KindNumeric:     `NUMERIC '12345678901234.567890'`,
		scrapper.KindInteger:     `CAST(9007199254740993 AS INT64)`,
		scrapper.KindFloat:       `CAST(12.5 AS FLOAT64)`,
		scrapper.KindText:        `'it\'s a back\\slash'`,
	},
	"databricks": {
		scrapper.KindTimestamp:   `TIMESTAMP_NTZ '2024-03-15 10:20:30.123456'`,
		scrapper.KindTimestampTz: `TIMESTAMP '2024-03-15 12:20:30.123456+02:00'`,
		scrapper.KindDate:        `DATE '2024-03-15'`,
		scrapper.KindNumeric:     `CAST('12345678901234.567890' AS DECIMAL(20,6))`,
		scrapper.KindInteger:     `CAST(9007199254740993 AS BIGINT)`,
		scrapper.KindFloat:       `CAST(12.5 AS DOUBLE)`,
		scrapper.KindText:        `'it\'s a back\\slash'`,
	},
	"clickhouse": {
		scrapper.KindTimestamp:   `toDateTime64('2024-03-15 10:20:30.123456', 6, 'UTC')`,
		scrapper.KindTimestampTz: `parseDateTime64BestEffort('2024-03-15 12:20:30.123456+02:00', 6)`,
		scrapper.KindDate:        `toDate('2024-03-15')`,
		scrapper.KindNumeric:     `toDecimal128('12345678901234.567890', 6)`,
		scrapper.KindInteger:     `toInt64(9007199254740993)`,
		scrapper.KindFloat:       `toFloat64(12.5)`,
		scrapper.KindText:        `'it\'s a back\\slash'`,
		scrapper.KindUUID:        `toUUID('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')`,
	},
	"duckdb": {
		scrapper.KindTimestamp:   `TIMESTAMP '2024-03-15 10:20:30.123456'`,
		scrapper.KindTimestampTz: `TIMESTAMPTZ '2024-03-15 12:20:30.123456+02:00'`,
		scrapper.KindDate:        `DATE '2024-03-15'`,
		scrapper.KindNumeric:     `CAST('12345678901234.567890' AS DECIMAL(20,6))`,
		scrapper.KindInteger:     `CAST(9007199254740993 AS BIGINT)`,
		scrapper.KindFloat:       `CAST(12.5 AS DOUBLE)`,
		scrapper.KindText:        `'it''s a back\slash'`,
		scrapper.KindUUID:        `CAST('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' AS UUID)`,
	},
	"mysql": {
		scrapper.KindTimestamp: `CAST('2024-03-15 10:20:30.123456' AS DATETIME(6))`,
		scrapper.KindDate:      `DATE '2024-03-15'`,
		scrapper.KindNumeric:   `CAST('12345678901234.567890' AS DECIMAL(20,6))`,
		scrapper.KindInteger:   `CAST(9007199254740993 AS SIGNED)`,
		scrapper.KindFloat:     `CAST(12.5 AS DOUBLE)`,
		scrapper.KindText:      `'it\'s a back\\slash'`,
	},
	"mssql": {
		scrapper.KindTimestamp:   `CAST('2024-03-15 10:20:30.123456' AS DATETIME2(6))`,
		scrapper.KindTimestampTz: `CAST('2024-03-15 12:20:30.123456 +02:00' AS DATETIMEOFFSET(6))`,
		scrapper.KindDate:        `CAST('2024-03-15' AS DATE)`,
		scrapper.KindNumeric:     `CAST('12345678901234.567890' AS DECIMAL(20,6))`,
		scrapper.KindInteger:     `CAST(9007199254740993 AS BIGINT)`,
		scrapper.KindFloat:       `CAST(12.5 AS FLOAT)`,
		scrapper.KindText:        `N'it''s a back\slash'`,
		scrapper.KindUUID:        `CAST('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' AS UNIQUEIDENTIFIER)`,
	},
	"oracle": {
		scrapper.KindTimestamp:   `TIMESTAMP '2024-03-15 10:20:30.123456'`,
		scrapper.KindTimestampTz: `TIMESTAMP '2024-03-15 12:20:30.123456 +02:00'`,
		scrapper.KindDate:        `DATE '2024-03-15'`,
		scrapper.KindNumeric:     `CAST(12345678901234.567890 AS NUMBER(20,6))`,
		scrapper.KindInteger:     `CAST(9007199254740993 AS NUMBER(19))`,
		scrapper.KindFloat:       `CAST(12.5 AS BINARY_DOUBLE)`,
		scrapper.KindText:        `'it''s a back\slash'`,
	},
	// Db2 LUW has neither TIMESTAMP WITH TIME ZONE nor a UUID type.
	"db2": {
		scrapper.KindTimestamp: `TIMESTAMP '2024-03-15 10:20:30.123456'`,
		scrapper.KindDate:      `DATE '2024-03-15'`,
		scrapper.KindNumeric:   `CAST(12345678901234.567890 AS DECIMAL(20,6))`,
		scrapper.KindInteger:   `CAST(9007199254740993 AS BIGINT)`,
		scrapper.KindFloat:     `CAST(12.5 AS DOUBLE)`,
		scrapper.KindText:      `'it''s a back\slash'`,
	},
	"trino": {
		scrapper.KindTimestamp:   `TIMESTAMP '2024-03-15 10:20:30.123456'`,
		scrapper.KindTimestampTz: `TIMESTAMP '2024-03-15 12:20:30.123456 +02:00'`,
		scrapper.KindDate:        `DATE '2024-03-15'`,
		scrapper.KindNumeric:     `CAST('12345678901234.567890' AS DECIMAL(20,6))`,
		scrapper.KindInteger:     `CAST(9007199254740993 AS BIGINT)`,
		scrapper.KindFloat:       `CAST(12.5 AS DOUBLE)`,
		scrapper.KindText:        `'it''s a back\slash'`,
		scrapper.KindUUID:        `CAST('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' AS UUID)`,
	},
}

// roundTripKinds is, per dialect, the kind a value's native type maps to where
// that is not the kind it was made as, because the engine has no such type:
// ClickHouse has no zone-less DateTime, an Oracle DATE carries a time of day,
// and NUMBER / FIXED is one type whatever its scale.
var roundTripKinds = map[string]map[scrapper.ValueKind]scrapper.ValueKind{
	"clickhouse": {scrapper.KindTimestamp: scrapper.KindTimestampTz},
	"oracle":     {scrapper.KindDate: scrapper.KindTimestamp, scrapper.KindInteger: scrapper.KindNumeric},
	"snowflake":  {scrapper.KindInteger: scrapper.KindNumeric},
}

func init() {
	roundTripExprs["fabric"] = roundTripExprs["mssql"]

	// Athena runs the Trino dialect, but its driver (influxdata/athenadriver)
	// fails the whole row on two of Trino's values, inside its own Rows.Next:
	// a timestamp with time zone rendered at an offset ("cannot load timezone
	// +02:00", it only resolves region names) and a uuid ("unknown type
	// uuid"). The timestamptz is written in UTC instead, and uuid is left out.
	athena := map[scrapper.ValueKind]string{}
	for kind, expr := range roundTripExprs["trino"] {
		athena[kind] = expr
	}
	athena[scrapper.KindTimestampTz] = `AT_TIMEZONE(TIMESTAMP '2024-03-15 12:20:30.123456 +02:00', 'UTC')`
	delete(athena, scrapper.KindUUID)
	roundTripExprs["athena"] = athena
}

// ValueRoundTripSuite pushes one value of each common type through a real
// warehouse and checks every step a consumer depends on:
//
//   - kind: RunRawQuery and QueryShape report the column's Kind as the kind
//     the value was made as;
//   - raw: RunRawQuery decodes the native value to the scrapper.Value it
//     stands for, timestamps as instants in UTC, numerics without loss;
//   - metrics: QueryCustomMetrics decodes it to a number or a time;
//   - text: the warehouse's own text rendering of a timestamp (what
//     CAST(... AS VARCHAR) returns) is read back as the same instant;
//   - literal: the decoded value, written back with scrapper.SqlLiteral for
//     its column's Kind, compares equal to the original in the warehouse.
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

func (s *ValueRoundTripSuite) TestValueRoundTrip_Timestamp()   { s.runKind(scrapper.KindTimestamp) }
func (s *ValueRoundTripSuite) TestValueRoundTrip_TimestampTz() { s.runKind(scrapper.KindTimestampTz) }
func (s *ValueRoundTripSuite) TestValueRoundTrip_Date()        { s.runKind(scrapper.KindDate) }
func (s *ValueRoundTripSuite) TestValueRoundTrip_Numeric()     { s.runKind(scrapper.KindNumeric) }
func (s *ValueRoundTripSuite) TestValueRoundTrip_BigInt()      { s.runKind(scrapper.KindInteger) }
func (s *ValueRoundTripSuite) TestValueRoundTrip_Double()      { s.runKind(scrapper.KindFloat) }
func (s *ValueRoundTripSuite) TestValueRoundTrip_Text()        { s.runKind(scrapper.KindText) }
func (s *ValueRoundTripSuite) TestValueRoundTrip_UUID()        { s.runKind(scrapper.KindUUID) }

// TestValueRoundTrip_SixteenCharacterText reads back text exactly 16 bytes
// long, which is also the length of a binary UUID. Drivers that hand text
// back as []byte (MySQL, MariaDB) must not have it decoded as one.
func (s *ValueRoundTripSuite) TestValueRoundTrip_SixteenCharacterText() {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}
	cv, _ := s.rawValue(s.selectValue(`'abcdefghijklmnop'`))
	s.Require().False(cv.IsNull)
	s.Equal(scrapper.StringValue("abcdefghijklmnop"), cv.Value)
}

func (s *ValueRoundTripSuite) runKind(kind scrapper.ValueKind) {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}
	expr, ok := roundTripExprs[s.Scrapper.DialectType()][kind]
	if !ok {
		s.T().Skipf("%s has no %s type", s.Scrapper.DialectType(), kind)
	}

	s.Run("kind", func() {
		dialect := s.Scrapper.DialectType()
		want := kind
		if mapped, ok := roundTripKinds[dialect][kind]; ok {
			want = mapped
		}
		_, col := s.rawValue(s.selectValue(expr))
		s.Equalf(want, col.Kind, "RunRawQuery column Kind for native type %q", col.NativeType)

		shape, err := s.Scrapper.QueryShape(s.ctx(), s.selectValue(expr))
		if errors.Is(err, scrapper.ErrUnsupported) {
			return
		}
		s.Require().NoError(err)
		s.Require().Len(shape, 1)
		s.T().Logf("shape native type: %s", shape[0].NativeType)
		s.Equalf(want, shape[0].Kind, "QueryShape column Kind for native type %q", shape[0].NativeType)
	})

	s.Run("raw", func() {
		cv, _ := s.rawValue(s.selectValue(expr))
		s.Require().False(cv.IsNull, "value should not be null")
		s.assertDecoded(kind, cv.Value)
	})

	s.Run("metrics", func() {
		switch kind {
		case scrapper.KindText, scrapper.KindUUID:
			s.T().Skip("QueryCustomMetrics does not carry text")
		}
		cv := s.metricsValue(s.selectValue(expr))
		s.Require().False(cv.IsNull, "value should not be null")
		s.assertMetricsDecoded(kind, cv.Value)
	})

	s.Run("text", func() {
		switch kind {
		case scrapper.KindTimestamp, scrapper.KindTimestampTz, scrapper.KindDate:
		default:
			s.T().Skip("only timestamps have a text form worth parsing")
		}
		asText, err := s.Scrapper.SqlDialect().ToString(sqldialect.Sql("v")).ToSql(s.Scrapper.SqlDialect())
		s.Require().NoError(err)
		sql := s.selectFrom(expr, asText+" AS v")

		// The warehouse picks the precision of its own text rendering
		// (Snowflake's default is milliseconds), so only the instant is
		// checked, to the millisecond. This reads the raw path only: the
		// metrics path keeps text out on purpose (scrapper.MetricValueFromText).
		raw, _ := s.rawValue(sql)
		s.Require().False(raw.IsNull, "value should not be null")
		parsed, ok := scrapper.AsTime(raw.Value)
		s.Require().Truef(ok, "scrapper.AsTime should read the text form, got %T %v", raw.Value, raw.Value)
		s.WithinDuration(s.wantTime(kind), parsed, time.Millisecond)
	})

	s.Run("literal", func() {
		cv, col := s.rawValue(s.selectValue(expr))
		s.Require().False(cv.IsNull, "value should not be null")
		literal, err := scrapper.SqlLiteral(s.Scrapper.SqlDialect(), col.Kind, cv.Value)
		s.Require().NoError(err)
		s.T().Logf("literal: %s", literal)
		eq, _ := s.rawValue(s.selectFrom(expr, fmt.Sprintf("CASE WHEN v = %s THEN 1 ELSE 0 END AS v", literal)))
		s.Require().False(eq.IsNull)
		s.Equalf(scrapper.IntValue(1), eq.Value, "the literal should compare equal to the value it was decoded from")
	})
}

func (s *ValueRoundTripSuite) wantTime(kind scrapper.ValueKind) time.Time {
	if kind == scrapper.KindDate {
		return roundTripDate
	}
	return roundTripTimestamp
}

// assertDecoded checks what RunRawQuery made of the value.
func (s *ValueRoundTripSuite) assertDecoded(kind scrapper.ValueKind, v scrapper.Value) {
	s.T().Helper()
	switch kind {
	case scrapper.KindTimestamp, scrapper.KindTimestampTz, scrapper.KindDate:
		got, ok := v.(scrapper.TimeValue)
		s.Require().Truef(ok, "want TimeValue, got %T %v", v, v)
		s.Truef(time.Time(got).Equal(s.wantTime(kind)), "want %s, got %s", s.wantTime(kind), time.Time(got))
		s.Equal(time.UTC, time.Time(got).Location(), "timestamps are decoded in UTC")
	case scrapper.KindNumeric:
		// A fractional exact numeric is text holding the warehouse's digits.
		s.Require().IsTypef(scrapper.StringValue(""), v, "want StringValue, got %T %v", v, v)
		got, ok := scrapper.AsBigRat(v)
		s.Require().True(ok)
		want, _ := new(big.Rat).SetString(roundTripNumeric)
		s.Truef(got.Cmp(want) == 0, "want %s, got %s", roundTripNumeric, v)
	case scrapper.KindInteger:
		s.Equal(scrapper.IntValue(roundTripBigInt), v)
	case scrapper.KindFloat:
		s.Equal(scrapper.DoubleValue(roundTripDouble), v)
	case scrapper.KindText:
		s.Equal(scrapper.StringValue(roundTripText), v)
	case scrapper.KindUUID:
		s.Equal(scrapper.StringValue(roundTripUUID), v)
	}
}

// assertMetricsDecoded checks what QueryCustomMetrics made of the value. The
// metrics path is float64 for anything fractional, so the numeric is only
// checked to float precision.
func (s *ValueRoundTripSuite) assertMetricsDecoded(kind scrapper.ValueKind, v scrapper.Value) {
	s.T().Helper()
	switch kind {
	case scrapper.KindTimestamp, scrapper.KindTimestampTz, scrapper.KindDate:
		got, ok := v.(scrapper.TimeValue)
		s.Require().Truef(ok, "want TimeValue, got %T %v", v, v)
		s.Truef(time.Time(got).Equal(s.wantTime(kind)), "want %s, got %s", s.wantTime(kind), time.Time(got))
	case scrapper.KindNumeric:
		got, ok := v.(scrapper.DoubleValue)
		s.Require().Truef(ok, "want DoubleValue, got %T %v", v, v)
		want, _ := strconv.ParseFloat(roundTripNumeric, 64)
		s.InDelta(want, float64(got), 0.01)
	case scrapper.KindInteger:
		s.Equal(scrapper.IntValue(roundTripBigInt), v)
	case scrapper.KindFloat:
		s.Equal(scrapper.DoubleValue(roundTripDouble), v)
	}
}

func (s *ValueRoundTripSuite) selectValue(expr string) string {
	return s.selectFrom(expr, "v")
}

// selectFrom reads outer from a derived table whose only column, v, is expr.
func (s *ValueRoundTripSuite) selectFrom(expr, outer string) string {
	inner := "SELECT " + expr + " AS v"
	switch s.Scrapper.DialectType() {
	case "oracle":
		inner += " FROM dual"
	case "db2":
		inner += " FROM SYSIBM.SYSDUMMY1"
	}
	tableAlias := " AS t"
	if !s.Scrapper.SqlDialect().SupportsAsBeforeTableAlias() {
		tableAlias = " t"
	}
	return "SELECT " + outer + " FROM (" + inner + ")" + tableAlias
}

// rawValue runs sql through RunRawQuery and returns its only cell and the
// column it was read from.
func (s *ValueRoundTripSuite) rawValue(sql string) (*scrapper.ColumnValue, *scrapper.QueryShapeColumn) {
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
	col := it.Columns()[0]
	s.T().Logf("raw: %T %v (native %s, kind %s)", cells[0].Value, cells[0].Value, col.NativeType, col.Kind)
	return cells[0], col
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
