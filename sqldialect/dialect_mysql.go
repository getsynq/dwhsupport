package sqldialect

import (
	"fmt"
	"time"
)

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
	return WrapSql("SUM(CASE WHEN %s THEN 1 ELSE 0 END)", expr)
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

func (d *MySQLDialect) SubString(expr Expr, start int64, length int64) Expr {
	return Fn("SUBSTRING", expr, Int64(start), Int64(length))
}
