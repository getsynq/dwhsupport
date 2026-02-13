package sqldialect

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

//
// RedshiftDialect
//

var _ Dialect = (*RedshiftDialect)(nil)

type RedshiftDialect struct{}

func NewRedshiftDialect() *RedshiftDialect {
	return &RedshiftDialect{}
}

func (d *RedshiftDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	if fqn == nil {
		return "", errors.New("fqn is nil")
	}
	return fmt.Sprintf("%q.%q.%q", fqn.projectId, PqQuoteIdentifierIfUpper(fqn.datasetId), PqQuoteIdentifierIfUpper(fqn.tableId)), nil
}

func (d *RedshiftDialect) ResolveTableFunction(t *TableFnExpr) (string, error) {
	if t == nil {
		return "", errors.New("table_fn is nil")
	}
	return Fn(t.name, t.ops...).ToSql(d)
}

func (d *RedshiftDialect) CountIf(expr Expr) Expr {
	return WrapSql("SUM(CASE WHEN %s THEN 1 ELSE 0 END)", expr)
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
	return PqQuoteIdentifierIfUpper(expr.name), nil
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

func (d *RedshiftDialect) CurrentTimestamp() Expr {
	return Sql("CURRENT_TIMESTAMP::timestamp")
}

func (d *RedshiftDialect) Identifier(identifier string) string {
	return identifier
}

func (d *RedshiftDialect) StringLiteral(s string) string {
	return StandardSQLStringLiteral(s)
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

func (d *RedshiftDialect) ConcatWithSeparator(separator string, exprs ...Expr) Expr {
	args := make([]Expr, 0, len(exprs)+1)
	args = append(args, String(separator))
	args = append(args, exprs...)
	return Fn("concat_ws", args...)
}

func (d *RedshiftDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return expression
}

func (d *RedshiftDialect) SubString(expr Expr, start int64, length int64) Expr {
	return Fn("SUBSTRING", expr, Int64(start), Int64(length))
}
