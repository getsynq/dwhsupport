package sqldialect

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

//
// DatabricksDialect
//

var _ Dialect = (*DatabricksDialect)(nil)

type DatabricksDialect struct{}

func NewDatabricksDialect() *DatabricksDialect {
	return &DatabricksDialect{}
}

func (d *DatabricksDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	if fqn == nil {
		return "", errors.New("fqn is nil")
	}
	return fmt.Sprintf("%s.%s.%s", fqn.projectId, fqn.datasetId, fqn.tableId), nil
}

func (d *DatabricksDialect) ResolveTableFunction(t *TableFnExpr) (string, error) {
	if t == nil {
		return "", errors.New("table_fn is nil")
	}
	return Fn(t.name, t.ops...).ToSql(d)
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

func (d *DatabricksDialect) CurrentTimestamp() Expr {
	return Fn("CURRENT_TIMESTAMP")
}

func (d *DatabricksDialect) Identifier(identifier string) string {
	return fmt.Sprintf("`%s`", identifier)
}

func (d *DatabricksDialect) StringLiteral(s string) string {
	return StandardSQLStringLiteral(s)
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

func (d *DatabricksDialect) SubString(expr Expr, start int64, length int64) Expr {
	return Fn("SUBSTRING", expr, Int64(start), Int64(length))
}
