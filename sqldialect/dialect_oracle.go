package sqldialect

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

var _ Dialect = (*OracleDialect)(nil)

type OracleDialect struct{}

func NewOracleDialect() *OracleDialect {
	return &OracleDialect{}
}

func (d *OracleDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	if fqn == nil {
		return "", errors.New("fqn is nil")
	}
	return fmt.Sprintf("%s.%s", OracleQuoteIdentifier(fqn.datasetId), OracleQuoteIdentifier(fqn.tableId)), nil
}

func (d *OracleDialect) ResolveTableFunction(t *TableFnExpr) (string, error) {
	if t == nil {
		return "", errors.New("table_fn is nil")
	}
	return Fn(t.name, t.ops...).ToSql(d)
}

func (d *OracleDialect) CountIf(expr Expr) Expr {
	return WrapSql("SUM(CASE WHEN %s THEN 1 ELSE 0 END)", expr)
}

func (d *OracleDialect) Count(expr Expr) Expr {
	return Fn("COUNT", expr)
}

func (d *OracleDialect) Median(expr Expr) Expr {
	return Fn("MEDIAN", expr)
}

func (d *OracleDialect) Stddev(expr Expr) Expr {
	return Fn("STDDEV", expr)
}

func (d *OracleDialect) ResolveTime(t time.Time) (string, error) {
	return fmt.Sprintf("TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS')", t.Format("2006-01-02 15:04:05")), nil
}

func (d *OracleDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return OracleQuoteIdentifier(expr.name), nil
}

func (d *OracleDialect) RoundTime(expr Expr, interval time.Duration) Expr {
	unit := oracleTimeTruncUnit(interval)
	return Fn("TRUNC", expr, String(unit))
}

func (d *OracleDialect) CeilTime(expr Expr, interval time.Duration) Expr {
	return d.AddTime(d.RoundTime(expr, interval), interval)
}

func (d *OracleDialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)
	return WrapSql("(%s - INTERVAL '%s' %s)", expr, Int64(interval), timeUnitSql(unit))
}

func (d *OracleDialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)
	return WrapSql("(%s + INTERVAL '%s' %s)", expr, Int64(interval), timeUnitSql(unit))
}

func (d *OracleDialect) CurrentTimestamp() Expr {
	return Sql("CURRENT_TIMESTAMP")
}

func (d *OracleDialect) Identifier(identifier string) string {
	return OracleQuoteIdentifier(identifier)
}

func (d *OracleDialect) StringLiteral(s string) string {
	return StandardSQLStringLiteral(s)
}

func (d *OracleDialect) ToString(expr Expr) Expr {
	return Fn("TO_CHAR", expr)
}

func (d *OracleDialect) ToFloat64(expr Expr) Expr {
	return WrapSql("CAST(%s AS BINARY_DOUBLE)", expr)
}

func (d *OracleDialect) Coalesce(exprs ...Expr) Expr {
	return Fn("COALESCE", exprs...)
}

func (d *OracleDialect) ConcatWithSeparator(separator string, exprs ...Expr) Expr {
	if len(exprs) == 0 {
		return String("")
	}
	if len(exprs) == 1 {
		return exprs[0]
	}
	result := exprs[0]
	sep := String(separator)
	for i := 1; i < len(exprs); i++ {
		result = WrapSql("%s || %s || %s", result, sep, exprs[i])
	}
	return result
}

func (d *OracleDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return expression
}

func (d *OracleDialect) SubString(expr Expr, start int64, length int64) Expr {
	return Fn("SUBSTR", expr, Int64(start), Int64(length))
}

func (d *OracleDialect) FormatLimit(rowsSql string) string {
	return fmt.Sprintf("FETCH FIRST %s ROWS ONLY", rowsSql)
}

func OracleQuoteIdentifier(identifier string) string {
	return fmt.Sprintf("\"%s\"", identifier)
}

func oracleTimeTruncUnit(duration time.Duration) string {
	switch {
	case duration >= 24*time.Hour:
		return "DD"
	case duration >= time.Hour:
		return "HH24"
	case duration >= time.Minute:
		return "MI"
	default:
		return "SS"
	}
}
