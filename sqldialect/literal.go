package sqldialect

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// Typed literals: writing a value read from a warehouse back into SQL for that
// warehouse. Every dialect's implementations sit together here so they can be
// compared side by side; scrappertest.ValueRoundTripSuite is what checks them,
// by comparing each literal with the value it was decoded from on a real
// engine.
//
// Every time is written in UTC with all the fractional digits the engine can
// hold. A zone-less timestamp is decoded as its wall clock in UTC
// (scrapper.NormalizeTime), so writing its UTC wall clock back gives the same
// value, and a zoned one is written with an explicit +00:00 so neither the
// engine nor the session's time zone gets to reinterpret it.

// wallClock renders t's UTC wall clock as "2006-01-02 15:04:05" followed by
// exactly digits fractional digits.
func wallClock(t time.Time, digits int) string {
	t = t.UTC()
	s := t.Format(time.DateTime)
	if digits > 0 {
		frac := fmt.Sprintf("%09d", t.Nanosecond())
		s += "." + frac[:digits]
	}
	return s
}

func dateOnly(t time.Time) string {
	return t.UTC().Format(time.DateOnly)
}

var decimalText = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)$`)

// checkDecimal accepts plain decimal text (digits, an optional point and
// sign, no exponent) and returns it without a leading '+'. Anything else
// could not be written as a numeric literal without being evaluated first.
func checkDecimal(s string) (string, error) {
	s = strings.TrimSpace(s)
	if !decimalText.MatchString(s) {
		return "", errors.Errorf("%q is not a decimal number", s)
	}
	return strings.TrimPrefix(s, "+"), nil
}

// decimalScale is the number of digits after the point.
func decimalScale(s string) int {
	if i := strings.IndexByte(s, '.'); i >= 0 {
		return len(s) - i - 1
	}
	return 0
}

// backslashStringLiteral escapes for the dialects whose string literals treat
// a backslash as an escape character. quote is how a single quote is written
// inside the literal.
func backslashStringLiteral(s string, quote string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, quote)
	return "'" + s + "'"
}

// Postgres

func (d *PostgresDialect) TimestampLiteral(t time.Time) string {
	return fmt.Sprintf("TIMESTAMP '%s'", wallClock(t, 6))
}

func (d *PostgresDialect) TimestampTzLiteral(t time.Time) string {
	return fmt.Sprintf("TIMESTAMPTZ '%s+00:00'", wallClock(t, 6))
}

func (d *PostgresDialect) DateLiteral(t time.Time) string {
	return fmt.Sprintf("DATE '%s'", dateOnly(t))
}

func (d *PostgresDialect) NumericLiteral(s string) (string, error) {
	return checkDecimal(s)
}

func (d *PostgresDialect) UUIDLiteral(s string) string {
	return fmt.Sprintf("CAST(%s AS UUID)", d.StringLiteral(s))
}

// Redshift

func (d *RedshiftDialect) TimestampLiteral(t time.Time) string {
	return fmt.Sprintf("CAST('%s' AS TIMESTAMP)", wallClock(t, 6))
}

func (d *RedshiftDialect) TimestampTzLiteral(t time.Time) string {
	return fmt.Sprintf("CAST('%s+00:00' AS TIMESTAMPTZ)", wallClock(t, 6))
}

func (d *RedshiftDialect) DateLiteral(t time.Time) string {
	return fmt.Sprintf("CAST('%s' AS DATE)", dateOnly(t))
}

func (d *RedshiftDialect) NumericLiteral(s string) (string, error) {
	return checkDecimal(s)
}

// UUIDLiteral is text: Redshift has no UUID type.
func (d *RedshiftDialect) UUIDLiteral(s string) string {
	return d.StringLiteral(s)
}

// Snowflake

func (d *SnowflakeDialect) TimestampLiteral(t time.Time) string {
	return fmt.Sprintf("TO_TIMESTAMP_NTZ('%s')", wallClock(t, 9))
}

func (d *SnowflakeDialect) TimestampTzLiteral(t time.Time) string {
	return fmt.Sprintf("TO_TIMESTAMP_TZ('%s +00:00')", wallClock(t, 9))
}

func (d *SnowflakeDialect) DateLiteral(t time.Time) string {
	return fmt.Sprintf("DATE '%s'", dateOnly(t))
}

func (d *SnowflakeDialect) NumericLiteral(s string) (string, error) {
	return checkDecimal(s)
}

// UUIDLiteral is text: Snowflake has no UUID type.
func (d *SnowflakeDialect) UUIDLiteral(s string) string {
	return d.StringLiteral(s)
}

// BigQuery

func (d *BigQueryDialect) TimestampLiteral(t time.Time) string {
	return fmt.Sprintf("DATETIME '%s'", wallClock(t, 6))
}

func (d *BigQueryDialect) TimestampTzLiteral(t time.Time) string {
	return fmt.Sprintf("TIMESTAMP '%s+00:00'", wallClock(t, 6))
}

func (d *BigQueryDialect) DateLiteral(t time.Time) string {
	return fmt.Sprintf("DATE '%s'", dateOnly(t))
}

// NumericLiteral is a BIGNUMERIC: an unadorned decimal is FLOAT64 on BigQuery,
// and comparing a NUMERIC column with one goes through float64. BIGNUMERIC
// holds every NUMERIC and INT64 value, so either compares with it exactly.
func (d *BigQueryDialect) NumericLiteral(s string) (string, error) {
	s, err := checkDecimal(s)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("BIGNUMERIC '%s'", s), nil
}

// UUIDLiteral is text: BigQuery has no UUID type.
func (d *BigQueryDialect) UUIDLiteral(s string) string {
	return d.StringLiteral(s)
}

// Databricks

func (d *DatabricksDialect) TimestampLiteral(t time.Time) string {
	return fmt.Sprintf("TIMESTAMP_NTZ '%s'", wallClock(t, 6))
}

func (d *DatabricksDialect) TimestampTzLiteral(t time.Time) string {
	return fmt.Sprintf("TIMESTAMP '%s+00:00'", wallClock(t, 6))
}

func (d *DatabricksDialect) DateLiteral(t time.Time) string {
	return fmt.Sprintf("DATE '%s'", dateOnly(t))
}

// NumericLiteral uses the BD suffix, which makes the literal a DECIMAL at any
// length; without it a literal past 38 digits would become a DOUBLE.
func (d *DatabricksDialect) NumericLiteral(s string) (string, error) {
	s, err := checkDecimal(s)
	if err != nil {
		return "", err
	}
	return s + "BD", nil
}

// UUIDLiteral is text: Databricks has no UUID type.
func (d *DatabricksDialect) UUIDLiteral(s string) string {
	return d.StringLiteral(s)
}

// ClickHouse

// TimestampLiteral is a DateTime64 in UTC, like TimestampTzLiteral: every
// ClickHouse DateTime is an instant, a column without a zone of its own is
// read in the server's.
func (d *ClickHouseDialect) TimestampLiteral(t time.Time) string {
	return fmt.Sprintf("toDateTime64('%s', 9, 'UTC')", wallClock(t, 9))
}

func (d *ClickHouseDialect) TimestampTzLiteral(t time.Time) string {
	return d.TimestampLiteral(t)
}

// DateLiteral is a Date32, whose range covers both Date and Date32 columns.
func (d *ClickHouseDialect) DateLiteral(t time.Time) string {
	return fmt.Sprintf("toDate32('%s')", dateOnly(t))
}

// NumericLiteral is a Decimal256 at the text's own scale: an unadorned
// decimal is Float64 on ClickHouse.
func (d *ClickHouseDialect) NumericLiteral(s string) (string, error) {
	s, err := checkDecimal(s)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("toDecimal256('%s', %d)", s, decimalScale(s)), nil
}

func (d *ClickHouseDialect) UUIDLiteral(s string) string {
	return fmt.Sprintf("toUUID(%s)", d.StringLiteral(s))
}

// DuckDB

func (d *DuckDBDialect) TimestampLiteral(t time.Time) string {
	return fmt.Sprintf("TIMESTAMP '%s'", wallClock(t, 6))
}

func (d *DuckDBDialect) TimestampTzLiteral(t time.Time) string {
	return fmt.Sprintf("TIMESTAMPTZ '%s+00:00'", wallClock(t, 6))
}

func (d *DuckDBDialect) DateLiteral(t time.Time) string {
	return fmt.Sprintf("DATE '%s'", dateOnly(t))
}

func (d *DuckDBDialect) NumericLiteral(s string) (string, error) {
	return checkDecimal(s)
}

func (d *DuckDBDialect) UUIDLiteral(s string) string {
	return fmt.Sprintf("CAST(%s AS UUID)", d.StringLiteral(s))
}

// MySQL / MariaDB

func (d *MySQLDialect) TimestampLiteral(t time.Time) string {
	return fmt.Sprintf("CAST('%s' AS DATETIME(6))", wallClock(t, 6))
}

// TimestampTzLiteral is a DATETIME too: MySQL has no zoned type. A TIMESTAMP
// column is read in the session's time zone, and a DATETIME literal is
// compared in it, so the round trip holds.
func (d *MySQLDialect) TimestampTzLiteral(t time.Time) string {
	return d.TimestampLiteral(t)
}

func (d *MySQLDialect) DateLiteral(t time.Time) string {
	return fmt.Sprintf("DATE '%s'", dateOnly(t))
}

func (d *MySQLDialect) NumericLiteral(s string) (string, error) {
	return checkDecimal(s)
}

// UUIDLiteral is text: MySQL has no UUID type.
func (d *MySQLDialect) UUIDLiteral(s string) string {
	return d.StringLiteral(s)
}

// MSSQL

func (d *MSSQLDialect) TimestampLiteral(t time.Time) string {
	return fmt.Sprintf("CAST('%s' AS DATETIME2(7))", wallClock(t, 7))
}

func (d *MSSQLDialect) TimestampTzLiteral(t time.Time) string {
	return fmt.Sprintf("CAST('%s +00:00' AS DATETIMEOFFSET(7))", wallClock(t, 7))
}

func (d *MSSQLDialect) DateLiteral(t time.Time) string {
	return fmt.Sprintf("CAST('%s' AS DATE)", dateOnly(t))
}

func (d *MSSQLDialect) NumericLiteral(s string) (string, error) {
	return checkDecimal(s)
}

func (d *MSSQLDialect) UUIDLiteral(s string) string {
	return fmt.Sprintf("CAST(%s AS UNIQUEIDENTIFIER)", d.StringLiteral(s))
}

// Fabric

// TimestampLiteral is DATETIME2(6): Fabric Warehouse caps the precision at 6.
func (d *FabricDialect) TimestampLiteral(t time.Time) string {
	return fmt.Sprintf("CAST('%s' AS DATETIME2(6))", wallClock(t, 6))
}

// TimestampTzLiteral is a DATETIME2 too: Fabric Warehouse has no
// DATETIMEOFFSET.
func (d *FabricDialect) TimestampTzLiteral(t time.Time) string {
	return d.TimestampLiteral(t)
}

// Oracle

func (d *OracleDialect) TimestampLiteral(t time.Time) string {
	return fmt.Sprintf("TIMESTAMP '%s'", wallClock(t, 9))
}

func (d *OracleDialect) TimestampTzLiteral(t time.Time) string {
	return fmt.Sprintf("TIMESTAMP '%s +00:00'", wallClock(t, 9))
}

func (d *OracleDialect) DateLiteral(t time.Time) string {
	return fmt.Sprintf("DATE '%s'", dateOnly(t))
}

func (d *OracleDialect) NumericLiteral(s string) (string, error) {
	return checkDecimal(s)
}

// UUIDLiteral is text: Oracle has no UUID type.
func (d *OracleDialect) UUIDLiteral(s string) string {
	return d.StringLiteral(s)
}

// Trino (and Athena, which uses this dialect)

func (d *TrinoDialect) TimestampLiteral(t time.Time) string {
	return fmt.Sprintf("TIMESTAMP '%s'", wallClock(t, 9))
}

func (d *TrinoDialect) TimestampTzLiteral(t time.Time) string {
	return fmt.Sprintf("TIMESTAMP '%s UTC'", wallClock(t, 9))
}

func (d *TrinoDialect) DateLiteral(t time.Time) string {
	return fmt.Sprintf("DATE '%s'", dateOnly(t))
}

// NumericLiteral is a DECIMAL literal: Trino will not compare a decimal with
// text.
func (d *TrinoDialect) NumericLiteral(s string) (string, error) {
	s, err := checkDecimal(s)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("DECIMAL '%s'", s), nil
}

// UUIDLiteral is a UUID literal: Trino will not compare a uuid with text.
func (d *TrinoDialect) UUIDLiteral(s string) string {
	return "UUID " + d.StringLiteral(s)
}
