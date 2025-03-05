package sqldialect

import (
	"fmt"
	"time"
)

//
// Dialect
//

type Dialect interface {
	ResolveFqn(fqn *TableFqnExpr) (string, error)

	Count(expr Expr) Expr
	CountIf(Expr) Expr
	Median(Expr) Expr
	Stddev(Expr) Expr
	RoundTime(Expr, time.Duration) Expr
	CeilTime(Expr, time.Duration) Expr
	SubTime(Expr, time.Duration) Expr
	AddTime(Expr, time.Duration) Expr

	Identifier(string) string
	ToString(Expr) Expr
	Coalesce(exprs ...Expr) Expr
	ToFloat64(Expr) Expr

	ResolveTime(time.Time) (string, error)
	ResolveTimeColumn(col *TimeColExpr) (string, error)
	AggregationColumnReference(expression Expr, alias string) Expr
}

//
// ClickHouseDialect
//

var _ Dialect = (*ClickHouseDialect)(nil)

type ClickHouseDialect struct{}

func NewClickHouseDialect() *ClickHouseDialect {
	return &ClickHouseDialect{}
}

func (d *ClickHouseDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	return fmt.Sprintf("%s.%s", fqn.datasetId, fqn.tableId), nil
}

func (d *ClickHouseDialect) CountIf(expr Expr) Expr {
	return Fn("toInt64", Fn("countIf", expr))
}

func (d *ClickHouseDialect) Count(expr Expr) Expr {
	return Fn("toInt64", Fn("count", expr))
}

func (d *ClickHouseDialect) Median(expr Expr) Expr {
	return Fn("median", expr)
}

func (d *ClickHouseDialect) Stddev(expr Expr) Expr {
	return Fn("stddevSamp", expr)
}

func (d *ClickHouseDialect) ResolveTime(t time.Time) (string, error) {
	return fmt.Sprintf("parseDateTimeBestEffort('%s')", t.Format("2006-01-02 15:04:05")), nil
}

func (d *ClickHouseDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return expr.name, nil
}

func (d *ClickHouseDialect) RoundTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("toStartOfInterval(%s, interval %s %s)", expr, Int64(interval), timeUnitSql(unit))
}

func (d *ClickHouseDialect) CeilTime(expr Expr, interval time.Duration) Expr {
	return d.AddTime(d.RoundTime(expr, interval), interval)
}

func (d *ClickHouseDialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("timestamp_sub(%s, interval %s %s)", expr, Int64(interval), timeUnitSql(unit))
}

func (d *ClickHouseDialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("timestamp_add(%s, interval %s %s)", expr, Int64(interval), timeUnitSql(unit))
}

func (d *ClickHouseDialect) Identifier(identifier string) string {
	return identifier
}

func (d *ClickHouseDialect) ToString(expr Expr) Expr {
	return Fn("toString", expr)
}

func (d *ClickHouseDialect) ToFloat64(expr Expr) Expr {
	return Fn("toFloat64", expr)
}

func (d *ClickHouseDialect) Coalesce(exprs ...Expr) Expr {
	return Fn("coalesce", exprs...)
}

func (d *ClickHouseDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return Identifier(alias)
}

//
// BigQueryDialect
//

var _ Dialect = (*BigQueryDialect)(nil)

type BigQueryDialect struct{}

func NewBigQueryDialect() *BigQueryDialect {
	return &BigQueryDialect{}
}

func (d *BigQueryDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	return fmt.Sprintf("`%s.%s.%s`", fqn.projectId, fqn.datasetId, fqn.tableId), nil
}

func (d *BigQueryDialect) CountIf(expr Expr) Expr {
	return Fn("countif", expr)
}

func (d *BigQueryDialect) Count(expr Expr) Expr {
	return Fn("count", expr)
}

func (d *BigQueryDialect) Median(expr Expr) Expr {
	return WrapSql("approx_quantiles(%s, 2)[offset(1)]", expr)
}

func (d *BigQueryDialect) Stddev(expr Expr) Expr {
	return Fn("stddev_samp", expr)
}

func (d *BigQueryDialect) ResolveTime(t time.Time) (string, error) {
	return fmt.Sprintf("timestamp '%s'", t.Format(time.RFC3339)), nil
}

func (d *BigQueryDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return fmt.Sprintf("timestamp(%s)", expr.name), nil
}

func (d *BigQueryDialect) RoundTime(expr Expr, duration time.Duration) Expr {
	unit, _ := getTimeUnitWithInterval(duration)

	return Fn("timestamp_trunc", expr, timeUnitSql(unit))
}

func (d *BigQueryDialect) CeilTime(expr Expr, interval time.Duration) Expr {
	return d.AddTime(d.RoundTime(expr, interval), interval)
}

func (d *BigQueryDialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("TIMESTAMP_ADD(%s, INTERVAL %s %s)", expr, Int64(-1*interval), timeUnitSql(unit))
}

func (d *BigQueryDialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("TIMESTAMP_ADD(%s, INTERVAL %s %s)", expr, Int64(interval), timeUnitSql(unit))
}

func (d *BigQueryDialect) Identifier(identifier string) string {
	return identifier
}

func (d *BigQueryDialect) ToString(expr Expr) Expr {
	return WrapSql("SAFE_CAST(%s AS STRING)", expr)
}

func (d *BigQueryDialect) ToFloat64(expr Expr) Expr {
	return WrapSql("CAST(%s AS FLOAT64)", expr)
}

func (d *BigQueryDialect) Coalesce(exprs ...Expr) Expr {
	return Fn("COALESCE", exprs...)
}

func (d *BigQueryDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return expression
}

//
// SnowflakeDialect
//

var _ Dialect = (*SnowflakeDialect)(nil)

type SnowflakeDialect struct{}

func NewSnowflakeDialect() *SnowflakeDialect {
	return &SnowflakeDialect{}
}

func (d *SnowflakeDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	return fmt.Sprintf("%s.%s.%s", fqn.projectId, fqn.datasetId, fqn.tableId), nil
}

func (d *SnowflakeDialect) CountIf(expr Expr) Expr {
	return Fn("count_if", expr)
}

func (d *SnowflakeDialect) Count(expr Expr) Expr {
	return Fn("count", expr)
}

func (d *SnowflakeDialect) Median(expr Expr) Expr {
	return Fn("median", expr)
}

func (d *SnowflakeDialect) Stddev(expr Expr) Expr {
	return Fn("stddev", expr)
}

func (d *SnowflakeDialect) ResolveTime(t time.Time) (string, error) {
	return fmt.Sprintf("'%s'", t.Format(time.RFC3339)), nil
}

func (d *SnowflakeDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return expr.name, nil
}

func (d *SnowflakeDialect) RoundTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return Fn("time_slice", Fn("to_timestamp_ntz", expr), Int64(interval), timeUnitString(unit))
}

func (d *SnowflakeDialect) CeilTime(expr Expr, interval time.Duration) Expr {
	return d.AddTime(d.RoundTime(expr, interval), interval)
}

func (d *SnowflakeDialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("TIMESTAMPADD(%s, %s, %s)", timeUnitSql(unit), Int64(-1*interval), expr)
}

func (d *SnowflakeDialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("TIMESTAMPADD(%s, %s, %s)", timeUnitSql(unit), Int64(interval), expr)
}

func (d *SnowflakeDialect) Identifier(identifier string) string {
	return fmt.Sprintf("%q", identifier)
}

func (d *SnowflakeDialect) ToString(expr Expr) Expr {
	return Fn("to_varchar", expr)
}

func (d *SnowflakeDialect) ToFloat64(expr Expr) Expr {
	return WrapSql("CAST(%s AS FLOAT)", expr)
}

func (d *SnowflakeDialect) Coalesce(exprs ...Expr) Expr {
	return Fn("COALESCE", exprs...)
}

func (d *SnowflakeDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return Identifier(alias)
}

//
// RedshiftDialect
//

var _ Dialect = (*RedshiftDialect)(nil)

type RedshiftDialect struct{}

func NewRedshiftDialect() *RedshiftDialect {
	return &RedshiftDialect{}
}

func (d *RedshiftDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	return fmt.Sprintf("%q.%q.%q", fqn.projectId, fqn.datasetId, fqn.tableId), nil
}

func (d *RedshiftDialect) CountIf(expr Expr) Expr {
	return Fn("SUM(CASE WHEN %s THEN 1 ELSE 0 END)", expr)
}

func (d *RedshiftDialect) Count(expr Expr) Expr {
	return Fn("count", expr)
}

func (d *RedshiftDialect) Median(expr Expr) Expr {
	return Fn("MEDIAN", expr)
}

func (d *RedshiftDialect) Stddev(expr Expr) Expr {
	return Fn("STDDEV", expr)
}

func (d *RedshiftDialect) ResolveTime(t time.Time) (string, error) {
	return fmt.Sprintf("'%s'", t.Format(time.RFC3339)), nil
}

func (d *RedshiftDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return expr.name, nil
}

func (d *RedshiftDialect) RoundTime(expr Expr, interval time.Duration) Expr {
	unit, _ := getTimeUnitWithInterval(interval)

	return Fn("DATE_TRUNC", timeUnitString(unit), expr)
}

func (d *RedshiftDialect) CeilTime(expr Expr, interval time.Duration) Expr {
	return d.AddTime(d.RoundTime(expr, interval), interval)
}

func (d *RedshiftDialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("DATEADD(%s, %s, %s)", timeUnitSql(unit), Int64(-1*interval), expr)
}

func (d *RedshiftDialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("DATEADD(%s, %s, %s)", timeUnitSql(unit), Int64(interval), expr)
}

func (d *RedshiftDialect) Identifier(identifier string) string {
	return identifier
}

func (d *RedshiftDialect) ToString(expr Expr) Expr {
	return WrapSql("CAST(%s AS VARCHAR)", expr)
}

func (d *RedshiftDialect) ToFloat64(expr Expr) Expr {
	return WrapSql("CAST(%s AS FLOAT)", expr)
}

func (d *RedshiftDialect) Coalesce(exprs ...Expr) Expr {
	return Fn("COALESCE", exprs...)
}

func (d *RedshiftDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return Identifier(alias)
}

//
// PostgresDialect
//

var _ Dialect = (*PostgresDialect)(nil)

type PostgresDialect struct{}

func NewPostgresDialect() *PostgresDialect {
	return &PostgresDialect{}
}

func (d *PostgresDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	return fmt.Sprintf("%s.%s", fqn.datasetId, fqn.tableId), nil
}

func (d *PostgresDialect) CountIf(expr Expr) Expr {
	return Fn("SUM(CASE WHEN %s THEN 1 ELSE 0 END)", expr)
}

func (d *PostgresDialect) Count(expr Expr) Expr {
	return Fn("count", expr)
}

func (d *PostgresDialect) Median(expr Expr) Expr {
	return Fn("MEDIAN", expr)
}

func (d *PostgresDialect) Stddev(expr Expr) Expr {
	return Fn("STDDEV", expr)
}

func (d *PostgresDialect) ResolveTime(t time.Time) (string, error) {
	return fmt.Sprintf("'%s'", t.Format(time.RFC3339)), nil
}

func (d *PostgresDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return expr.name, nil
}

func (d *PostgresDialect) RoundTime(expr Expr, interval time.Duration) Expr {
	unit, _ := getTimeUnitWithInterval(interval)

	return Fn("DATE_TRUNC", timeUnitString(unit), expr)
}

func (d *PostgresDialect) CeilTime(expr Expr, interval time.Duration) Expr {
	return d.AddTime(d.RoundTime(expr, interval), interval)
}

func (d *PostgresDialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("%s + '%s %s'", expr, Int64(-1*interval), timeUnitSql(unit))
}

func (d *PostgresDialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("%s + '%s %s'", expr, Int64(interval), timeUnitSql(unit))
}

func (d *PostgresDialect) Identifier(identifier string) string {
	return identifier
}

func (d *PostgresDialect) ToString(expr Expr) Expr {
	return WrapSql("CAST(%s AS VARCHAR)", expr)
}

func (d *PostgresDialect) ToFloat64(expr Expr) Expr {
	return WrapSql("CAST(%s AS FLOAT)", expr)
}

func (d *PostgresDialect) Coalesce(exprs ...Expr) Expr {
	return Fn("COALESCE", exprs...)
}

func (d *PostgresDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return expression
}

//
// PostgresDialect
//

var _ Dialect = (*DuckDBDialect)(nil)

type DuckDBDialect struct{}

func NewDuckDBDialect() *DuckDBDialect {
	return &DuckDBDialect{}
}

func (d *DuckDBDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	return fmt.Sprintf("%s.%s", fqn.datasetId, fqn.tableId), nil
}

func (d *DuckDBDialect) CountIf(expr Expr) Expr {
	return Fn("SUM(CASE WHEN %s THEN 1 ELSE 0 END)", expr)
}

func (d *DuckDBDialect) Count(expr Expr) Expr {
	return Fn("count", expr)
}

func (d *DuckDBDialect) Median(expr Expr) Expr {
	return Fn("MEDIAN", expr)
}

func (d *DuckDBDialect) Stddev(expr Expr) Expr {
	return Fn("STDDEV", expr)
}

func (d *DuckDBDialect) ResolveTime(t time.Time) (string, error) {
	return fmt.Sprintf("'%s'", t.Format(time.RFC3339)), nil
}

func (d *DuckDBDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return expr.name, nil
}

func (d *DuckDBDialect) RoundTime(expr Expr, interval time.Duration) Expr {
	unit, _ := getTimeUnitWithInterval(interval)

	return Fn("DATE_TRUNC", timeUnitString(unit), expr)
}

func (d *DuckDBDialect) CeilTime(expr Expr, interval time.Duration) Expr {
	return d.AddTime(d.RoundTime(expr, interval), interval)
}

func (d *DuckDBDialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("%s + '%s %s'", expr, Int64(-1*interval), timeUnitSql(unit))
}

func (d *DuckDBDialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("%s + '%s %s'", expr, Int64(interval), timeUnitSql(unit))
}

func (d *DuckDBDialect) Identifier(identifier string) string {
	return identifier
}

func (d *DuckDBDialect) ToString(expr Expr) Expr {
	return WrapSql("CAST(%s AS VARCHAR)", expr)
}

func (d *DuckDBDialect) ToFloat64(expr Expr) Expr {
	return WrapSql("CAST(%s AS FLOAT)", expr)
}

func (d *DuckDBDialect) Coalesce(exprs ...Expr) Expr {
	return Fn("COALESCE", exprs...)
}

func (d *DuckDBDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return expression
}

//
// MySQLDialect
//

var _ Dialect = (*MySQLDialect)(nil)

type MySQLDialect struct{}

func NewMySQLDialect() *MySQLDialect {
	return &MySQLDialect{}
}

func (d *MySQLDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	return fmt.Sprintf("%s.%s", fqn.datasetId, fqn.tableId), nil
}

func (d *MySQLDialect) CountIf(expr Expr) Expr {
	return Fn("SUM(CASE WHEN %s THEN 1 ELSE 0 END)", expr)
}

func (d *MySQLDialect) Count(expr Expr) Expr {
	return Fn("count", expr)
}

func (d *MySQLDialect) Median(expr Expr) Expr {
	return Fn("MEDIAN", expr)
}

func (d *MySQLDialect) Stddev(expr Expr) Expr {
	return Fn("STDDEV", expr)
}

func (d *MySQLDialect) ResolveTime(t time.Time) (string, error) {
	return fmt.Sprintf("'%s'", t.Format(time.RFC3339)), nil
}

func (d *MySQLDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return expr.name, nil
}

func (d *MySQLDialect) RoundTime(expr Expr, interval time.Duration) Expr {
	unit, _ := getTimeUnitWithInterval(interval)

	switch unit {
	case TimeUnitSecond:
		return expr
	case TimeUnitHour:
		return Fn("STR_TO_DATE", Fn("DATE_FORMAT", expr, String("%Y-%m-%d %H:00:00")), String("%Y-%m-%d %H:%i:%s"))
	case TimeUnitDay:
		return Fn("STR_TO_DATE", Fn("DATE_FORMAT", expr, String("%Y-%m-%d 00:00:00")), String("%Y-%m-%d %H:%i:%s"))
	case TimeUnitMinute:
		return Fn("STR_TO_DATE", Fn("DATE_FORMAT", expr, String("%Y-%m-%d %H:%i:00")), String("%Y-%m-%d %H:%i:%s"))
	}

	return Fn("ROUND_TIME_NOT_SUPPORTED", timeUnitString(unit), expr)
}

func (d *MySQLDialect) CeilTime(expr Expr, interval time.Duration) Expr {
	return d.AddTime(d.RoundTime(expr, interval), interval)
}

func (d *MySQLDialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("DATEADD(%s, %s, %s)", timeUnitSql(unit), Int64(-1*interval), expr)
}

func (d *MySQLDialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("DATEADD(%s, %s, %s)", timeUnitSql(unit), Int64(interval), expr)
}

func (d *MySQLDialect) Identifier(identifier string) string {
	return identifier
}

func (d *MySQLDialect) ToString(expr Expr) Expr {
	return WrapSql("CAST(%s AS CHAR)", expr)
}

func (d *MySQLDialect) ToFloat64(expr Expr) Expr {
	return WrapSql("CAST(%s AS FLOAT)", expr)
}

func (d *MySQLDialect) Coalesce(exprs ...Expr) Expr {
	return Fn("COALESCE", exprs...)
}

func (d *MySQLDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return expression
}

//
// DatabricksDialect
//

var _ Dialect = (*DatabricksDialect)(nil)

type DatabricksDialect struct{}

func NewDatabricksDialect() *DatabricksDialect {
	return &DatabricksDialect{}
}

func (d *DatabricksDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	return fmt.Sprintf("%s.%s.%s", fqn.projectId, fqn.datasetId, fqn.tableId), nil
}

func (d *DatabricksDialect) CountIf(expr Expr) Expr {
	return Fn("count_if", expr)
}

func (d *DatabricksDialect) Count(expr Expr) Expr {
	return Fn("count", expr)
}

func (d *DatabricksDialect) Median(expr Expr) Expr {
	return Fn("median", expr)
}

func (d *DatabricksDialect) Stddev(expr Expr) Expr {
	return Fn("stddev", expr)
}

func (d *DatabricksDialect) ResolveTime(t time.Time) (string, error) {
	return fmt.Sprintf("'%s'", t.Format(time.RFC3339)), nil
}

func (d *DatabricksDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return expr.name, nil
}

func (d *DatabricksDialect) RoundTime(expr Expr, duration time.Duration) Expr {
	unit, _ := getTimeUnitWithInterval(duration)

	return Fn("DATE_TRUNC", timeUnitString(unit), expr)
}

func (d *DatabricksDialect) CeilTime(expr Expr, interval time.Duration) Expr {
	return d.AddTime(d.RoundTime(expr, interval), interval)
}

func (d *DatabricksDialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("TIMESTAMPADD(%s, %s, %s)", timeUnitSql(unit), Int64(-1*interval), expr)
}

func (d *DatabricksDialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("TIMESTAMPADD(%s, %s, %s)", timeUnitSql(unit), Int64(interval), expr)
}

func (d *DatabricksDialect) Identifier(identifier string) string {
	return fmt.Sprintf("`%s`", identifier)
}

func (d *DatabricksDialect) ToString(expr Expr) Expr {
	return WrapSql("CAST(%s AS STRING)", expr)
}

func (d *DatabricksDialect) ToFloat64(expr Expr) Expr {
	return WrapSql("CAST(%s AS FLOAT)", expr)
}

func (d *DatabricksDialect) Coalesce(exprs ...Expr) Expr {
	return Fn("COALESCE", exprs...)
}

func (d *DatabricksDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return Identifier(alias)
}

// utils

type TimeUnit string

const TimeUnitSecond TimeUnit = "SECOND"
const TimeUnitMinute TimeUnit = "MINUTE"
const TimeUnitHour TimeUnit = "HOUR"
const TimeUnitDay TimeUnit = "DAY"

func getTimeUnitWithInterval(duration time.Duration) (unit TimeUnit, interval int64) {
	switch duration {
	case time.Minute:
		unit = "MINUTE"
		interval = int64(duration.Minutes())

	case time.Hour:
		unit = "HOUR"
		interval = int64(duration.Hours())

	case 24 * time.Hour:
		unit = "DAY"
		interval = int64(duration.Hours() / 24)

	default:
		unit = "SECOND"
		interval = int64(duration.Seconds())
	}

	return
}

func timeUnitSql(unit TimeUnit) Expr {
	return Sql(string(unit))
}

func timeUnitString(unit TimeUnit) Expr {
	return String(string(unit))
}
