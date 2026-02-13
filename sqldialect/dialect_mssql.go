package sqldialect

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

var _ Dialect = (*MSSQLDialect)(nil)

type MSSQLDialect struct{}

func NewMSSQLDialect() *MSSQLDialect {
	return &MSSQLDialect{}
}

func (d *MSSQLDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	if fqn == nil {
		return "", errors.New("fqn is nil")
	}
	return fmt.Sprintf("[%s].[%s]", fqn.datasetId, fqn.tableId), nil
}

func (d *MSSQLDialect) ResolveTableFunction(t *TableFnExpr) (string, error) {
	if t == nil {
		return "", errors.New("table_fn is nil")
	}
	return Fn(t.name, t.ops...).ToSql(d)
}

func (d *MSSQLDialect) CountIf(expr Expr) Expr {
	return WrapSql("SUM(CASE WHEN %s THEN 1 ELSE 0 END)", expr)
}

func (d *MSSQLDialect) Count(expr Expr) Expr {
	return Fn("COUNT", expr)
}

func (d *MSSQLDialect) Median(expr Expr) Expr {
	return Fn("PERCENTILE_CONT", Sql("0.5"))
}

func (d *MSSQLDialect) Stddev(expr Expr) Expr {
	return Fn("STDEV", expr)
}

func (d *MSSQLDialect) ResolveTime(t time.Time) (string, error) {
	return fmt.Sprintf("CAST('%s' AS DATETIME2)", t.Format("2006-01-02 15:04:05")), nil
}

func (d *MSSQLDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return MSSQLQuoteIdentifier(expr.name), nil
}

func (d *MSSQLDialect) RoundTime(expr Expr, interval time.Duration) Expr {
	return Fn("DATETRUNC", timeUnitSql(mssqlTimeUnit(interval)), expr)
}

func (d *MSSQLDialect) CeilTime(expr Expr, interval time.Duration) Expr {
	return d.AddTime(d.RoundTime(expr, interval), interval)
}

func (d *MSSQLDialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)
	return Fn("DATEADD", timeUnitSql(unit), Int64(-1*interval), expr)
}

func (d *MSSQLDialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)
	return Fn("DATEADD", timeUnitSql(unit), Int64(interval), expr)
}

func (d *MSSQLDialect) CurrentTimestamp() Expr {
	return Sql("GETUTCDATE()")
}

func (d *MSSQLDialect) Identifier(identifier string) string {
	return MSSQLQuoteIdentifier(identifier)
}

func (d *MSSQLDialect) StringLiteral(s string) string {
	return StandardSQLStringLiteral(s)
}

func (d *MSSQLDialect) ToString(expr Expr) Expr {
	return WrapSql("CAST(%s AS NVARCHAR(MAX))", expr)
}

func (d *MSSQLDialect) ToFloat64(expr Expr) Expr {
	return WrapSql("CAST(%s AS FLOAT)", expr)
}

func (d *MSSQLDialect) Coalesce(exprs ...Expr) Expr {
	return Fn("COALESCE", exprs...)
}

func (d *MSSQLDialect) ConcatWithSeparator(separator string, exprs ...Expr) Expr {
	args := make([]Expr, 0, len(exprs)+1)
	args = append(args, String(separator))
	args = append(args, exprs...)
	return Fn("CONCAT_WS", args...)
}

func (d *MSSQLDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return expression
}

func (d *MSSQLDialect) SubString(expr Expr, start int64, length int64) Expr {
	return Fn("SUBSTRING", expr, Int64(start), Int64(length))
}

func (d *MSSQLDialect) FormatLimit(rowsSql string) string {
	return fmt.Sprintf("OFFSET 0 ROWS FETCH NEXT %s ROWS ONLY", rowsSql)
}

func MSSQLQuoteIdentifier(identifier string) string {
	return fmt.Sprintf("[%s]", identifier)
}

func mssqlTimeUnit(duration time.Duration) TimeUnit {
	switch {
	case duration >= 24*time.Hour:
		return TimeUnitDay
	case duration >= time.Hour:
		return TimeUnitHour
	case duration >= time.Minute:
		return TimeUnitMinute
	default:
		return TimeUnitSecond
	}
}
