package sqldialect

import (
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
	CurrentTimestamp() Expr

	Identifier(string) string
	ToString(Expr) Expr
	Coalesce(exprs ...Expr) Expr
	ToFloat64(Expr) Expr
	SubString(expr Expr, start int64, length int64) Expr

	ResolveTime(time.Time) (string, error)
	ResolveTimeColumn(col *TimeColExpr) (string, error)
	AggregationColumnReference(expression Expr, alias string) Expr
	ResolveTableFunction(t *TableFnExpr) (string, error)
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
