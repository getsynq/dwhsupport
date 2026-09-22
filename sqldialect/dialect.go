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
	ResolveFieldRef(string) string

	// QuoteIdent wraps a name in this dialect's identifier delimiters,
	// always. Identifier quotes only when a character demands it, which is a
	// character-class test and therefore blind to reserved words: a column
	// called `group` comes back bare and the statement fails to parse. Reach
	// for it through Ident rather than directly — that type also settles
	// whether the name still has to be folded.
	QuoteIdent(name string) string

	// UnquoteIdent strips one layer of identifier delimiters from text and
	// decodes the escape inside, reporting whether the text carried any. It is
	// the inverse of QuoteIdent, and dialect-specific for the same reason:
	// BigQuery escapes its backtick as `\``, everyone else doubles the
	// delimiter.
	UnquoteIdent(text string) (string, bool)

	// FoldIdent returns the name an unquoted reference resolves to on this
	// engine — upper case on Snowflake and Oracle, lower on Postgres and the
	// Presto family, unchanged where the comparison ignores case. Quoting an
	// identifier pins its case, so a written name is folded first to keep it
	// addressing the object it addressed unquoted.
	FoldIdent(name string) string
	StringLiteral(string) string
	ToString(Expr) Expr
	Coalesce(exprs ...Expr) Expr
	ConcatWithSeparator(separator string, exprs ...Expr) Expr
	ToFloat64(Expr) Expr
	SubString(expr Expr, start int64, length int64) Expr
	StringLength(expr Expr) Expr

	ResolveTime(time.Time) (string, error)
	ResolveTimeColumn(col *TimeColExpr) (string, error)
	AggregationColumnReference(expression Expr, alias string) Expr
	ResolveTableFunction(t *TableFnExpr) (string, error)
	FormatLimit(rowsSql string) string

	// SupportsCrossDatabaseQueries returns true if the dialect supports referencing
	// the database/catalog as part of the SQL FQN (e.g. database.schema.table).
	SupportsCrossDatabaseQueries() bool

	// SupportsAsBeforeTableAlias reports whether the optional AS keyword may sit
	// between a derived table and its alias — `(select ...) AS t` rather than
	// `(select ...) t`.
	//
	// The keyword is optional in the SQL standard and most dialects document it
	// as accepted, but Oracle rejects it outright (ORA-03048: SQL reserved word
	// 'AS' is not syntactically valid), which used to fail every query that
	// wrapped a caller's SQL as a subquery. Dropping it everywhere would be the
	// smaller change but leaves the generated SQL harder to read on the
	// dialects that do take it, so the keyword is a per-dialect answer.
	SupportsAsBeforeTableAlias() bool
}

// utils

type TimeUnit string

const (
	TimeUnitSecond TimeUnit = "SECOND"
	TimeUnitMinute TimeUnit = "MINUTE"
	TimeUnitHour   TimeUnit = "HOUR"
	TimeUnitDay    TimeUnit = "DAY"
)

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
