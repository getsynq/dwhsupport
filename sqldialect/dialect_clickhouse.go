package sqldialect

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

//
// ClickHouseDialect
//

var _ Dialect = (*ClickHouseDialect)(nil)

type ClickHouseDialect struct{}

func NewClickHouseDialect() *ClickHouseDialect {
	return &ClickHouseDialect{}
}

func (d *ClickHouseDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	if fqn == nil {
		return "", errors.New("fqn is nil")
	}
	return fmt.Sprintf("%s.%s", fqn.datasetId, fqn.tableId), nil
}

func (d *ClickHouseDialect) ResolveTableFunction(t *TableFnExpr) (string, error) {
	if t == nil {
		return "", errors.New("table_fn is nil")
	}
	return Fn(t.name, t.ops...).ToSql(d)
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

func (d *ClickHouseDialect) SubString(expr Expr, start int64, length int64) Expr {
	return Fn("substring", expr, Int64(start), Int64(length))
}
