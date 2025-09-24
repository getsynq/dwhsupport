package sqldialect

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

//
// DuckDBDialect
//

var _ Dialect = (*DuckDBDialect)(nil)

type DuckDBDialect struct{}

func NewDuckDBDialect() *DuckDBDialect {
	return &DuckDBDialect{}
}

func (d *DuckDBDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	if fqn == nil {
		return "", errors.New("fqn is nil")
	}
	return fmt.Sprintf("%s.%s", fqn.datasetId, fqn.tableId), nil
}

func (d *DuckDBDialect) ResolveTableFunction(t *TableFnExpr) (string, error) {
	if t == nil {
		return "", errors.New("table_fn is nil")
	}
	return Fn(t.name, t.ops...).ToSql(d)
}

func (d *DuckDBDialect) CountIf(expr Expr) Expr {
	return WrapSql("SUM(CASE WHEN %s THEN 1 ELSE 0 END)", expr)
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

func (d *DuckDBDialect) CurrentTimestamp() Expr {
	return Fn("now")
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
	return Identifier(alias)
}

func (d *DuckDBDialect) SubString(expr Expr, start int64, length int64) Expr {
	return Fn("SUBSTRING", expr, Int64(start), Int64(length))
}
