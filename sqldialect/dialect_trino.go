package sqldialect

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

//
// TrinoDialect
//

var _ Dialect = (*TrinoDialect)(nil)

type TrinoDialect struct{}

func NewTrinoDialect() *TrinoDialect {
	return &TrinoDialect{}
}

func (d *TrinoDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	if fqn == nil {
		return "", errors.New("fqn is nil")
	}
	return fmt.Sprintf("%s.%s.%s", fqn.projectId, fqn.datasetId, fqn.tableId), nil
}

func (d *TrinoDialect) ResolveTableFunction(t *TableFnExpr) (string, error) {
	if t == nil {
		return "", errors.New("table_fn is nil")
	}
	return Fn("TABLE", Fn(t.name, t.ops...)).ToSql(d)
}

func (d *TrinoDialect) CountIf(expr Expr) Expr {
	return Fn("count_if", expr)
}

func (d *TrinoDialect) Count(expr Expr) Expr {
	return Fn("count", expr)
}

func (d *TrinoDialect) Median(expr Expr) Expr {
	return Fn("approx_percentile", expr, Sql("0.5"))
}

func (d *TrinoDialect) Stddev(expr Expr) Expr {
	return Fn("STDDEV", expr)
}

func (d *TrinoDialect) ResolveTime(t time.Time) (string, error) {
	return Fn("from_iso8601_timestamp", String(t.Format(time.RFC3339))).ToSql(d)
}

func (d *TrinoDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return expr.name, nil
}

func (d *TrinoDialect) RoundTime(expr Expr, interval time.Duration) Expr {
	unit, _ := getTimeUnitWithInterval(interval)

	return Fn("DATE_TRUNC", timeUnitString(unit), expr)
}

func (d *TrinoDialect) CeilTime(expr Expr, interval time.Duration) Expr {
	return d.AddTime(d.RoundTime(expr, interval), interval)
}

func (d *TrinoDialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("DATE_ADD(%s, %s, %s)", timeUnitString(unit), Int64(-1*interval), expr)
}

func (d *TrinoDialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("DATE_ADD(%s, %s, %s)", timeUnitString(unit), Int64(interval), expr)
}

func (d *TrinoDialect) CurrentTimestamp() Expr {
	return Sql("CURRENT_TIMESTAMP")
}

func (d *TrinoDialect) Identifier(identifier string) string {
	return identifier
}

func (d *TrinoDialect) StringLiteral(s string) string {
	return StandardSQLStringLiteral(s)
}

func (d *TrinoDialect) ToString(expr Expr) Expr {
	return WrapSql("CAST(%s AS VARCHAR)", expr)
}

func (d *TrinoDialect) ToFloat64(expr Expr) Expr {
	return WrapSql("CAST(%s AS DOUBLE)", expr)
}

func (d *TrinoDialect) Coalesce(exprs ...Expr) Expr {
	return Fn("COALESCE", exprs...)
}

func (d *TrinoDialect) ConcatWithSeparator(separator string, exprs ...Expr) Expr {
	args := make([]Expr, 0, len(exprs)+1)
	args = append(args, String(separator))
	args = append(args, exprs...)
	return Fn("concat_ws", args...)
}

func (d *TrinoDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return expression
}

func (d *TrinoDialect) SubString(expr Expr, start int64, length int64) Expr {
	return Fn("SUBSTRING", expr, Int64(start), Int64(length))
}
