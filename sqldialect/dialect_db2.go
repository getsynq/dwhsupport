package sqldialect

import (
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
)

var _ Dialect = (*Db2Dialect)(nil)

// Db2Dialect is IBM Db2 for Linux, UNIX and Windows (LUW).
type Db2Dialect struct{}

func NewDb2Dialect() *Db2Dialect {
	return &Db2Dialect{}
}

// ResolveFqn renders schema.table: a connection is bound to one database, so
// the database part of the name is never written.
func (d *Db2Dialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	if fqn == nil {
		return "", errors.New("fqn is nil")
	}
	return fmt.Sprintf("%s.%s", Db2QuoteIdentifier(fqn.datasetId), Db2QuoteIdentifier(fqn.tableId)), nil
}

func (d *Db2Dialect) ResolveTableFunction(t *TableFnExpr) (string, error) {
	if t == nil {
		return "", errors.New("table_fn is nil")
	}
	return Fn(t.name, t.ops...).ToSql(d)
}

func (d *Db2Dialect) CountIf(expr Expr) Expr {
	return WrapSql("SUM(CASE WHEN %s THEN 1 ELSE 0 END)", expr)
}

func (d *Db2Dialect) Count(expr Expr) Expr {
	return Fn("COUNT", expr)
}

func (d *Db2Dialect) Median(expr Expr) Expr {
	return Fn("MEDIAN", expr)
}

func (d *Db2Dialect) Stddev(expr Expr) Expr {
	return Fn("STDDEV", expr)
}

func (d *Db2Dialect) ResolveTime(t time.Time) (string, error) {
	return fmt.Sprintf("TIMESTAMP '%s'", t.UTC().Format("2006-01-02 15:04:05")), nil
}

func (d *Db2Dialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return d.ResolveFieldRef(expr.name), nil
}

// RoundTime truncates with TRUNC(ts, format), which takes the same format
// elements as Oracle's.
func (d *Db2Dialect) RoundTime(expr Expr, interval time.Duration) Expr {
	return Fn("TRUNC", expr, String(oracleTimeTruncUnit(interval)))
}

func (d *Db2Dialect) CeilTime(expr Expr, interval time.Duration) Expr {
	return d.AddTime(d.RoundTime(expr, interval), interval)
}

// SubTime and AddTime use labeled durations (`ts - 3 HOUR`): Db2 has no
// INTERVAL type.
func (d *Db2Dialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)
	return WrapSql("(%s - %s %s)", expr, Int64(interval), timeUnitSql(unit))
}

func (d *Db2Dialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)
	return WrapSql("(%s + %s %s)", expr, Int64(interval), timeUnitSql(unit))
}

func (d *Db2Dialect) CurrentTimestamp() Expr {
	return Sql("CURRENT TIMESTAMP")
}

// QuoteIdent renders name wrapped in this dialect's identifier delimiters,
// always — unlike Identifier, which quotes only when a character demands it
// and so lets a reserved word through bare.
func (d *Db2Dialect) QuoteIdent(name string) string {
	return identQuotingDoubleQuotes.quote(name)
}

// UnquoteIdent is the inverse: it strips one layer of delimiters and decodes
// the escape inside, reporting whether the text carried any.
func (d *Db2Dialect) UnquoteIdent(text string) (string, bool) {
	return identQuotingDoubleQuotes.unquote(text)
}

// FoldIdent returns the name an unquoted reference resolves to.
// An unquoted reference folds to upper case.
func (d *Db2Dialect) FoldIdent(name string) string {
	return identFoldingUpperDb2.fold(name)
}

func (d *Db2Dialect) Identifier(identifier string) string {
	return Db2QuoteIdentifier(identifier)
}

func (d *Db2Dialect) ResolveFieldRef(name string) string {
	if isLikelyExpression(name) || isQuotedWith(name, '"', '"') {
		return name
	}
	return Db2QuoteIfNeeded(name)
}

func (d *Db2Dialect) StringLiteral(s string) string {
	return StandardSQLStringLiteral(s)
}

func (d *Db2Dialect) ToString(expr Expr) Expr {
	return Fn("VARCHAR", expr)
}

func (d *Db2Dialect) ToFloat64(expr Expr) Expr {
	return WrapSql("CAST(%s AS DOUBLE)", expr)
}

func (d *Db2Dialect) Coalesce(exprs ...Expr) Expr {
	return Fn("COALESCE", exprs...)
}

func (d *Db2Dialect) ConcatWithSeparator(separator string, exprs ...Expr) Expr {
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

func (d *Db2Dialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return expression
}

// SubString uses SUBSTRING with CODEUNITS32: SUBSTR fails with SQLCODE -138
// when start+length runs past the string, where every other engine returns
// what is there, and both SUBSTR and LENGTH count bytes by default.
func (d *Db2Dialect) SubString(expr Expr, start int64, length int64) Expr {
	return WrapSql("SUBSTRING(%s, %s, %s, CODEUNITS32)", expr, Int64(start), Int64(length))
}

// StringLength counts characters: LENGTH counts bytes of the database's
// encoding (11 for 'zamówienia').
func (d *Db2Dialect) StringLength(expr Expr) Expr {
	return Fn("CHARACTER_LENGTH", expr)
}

func (d *Db2Dialect) FormatLimit(rowsSql string) string {
	return fmt.Sprintf("FETCH FIRST %s ROWS ONLY", rowsSql)
}

func (d *Db2Dialect) SupportsCrossDatabaseQueries() bool { return false }

func (d *Db2Dialect) SupportsAsBeforeTableAlias() bool { return true }

func Db2QuoteIdentifier(identifier string) string {
	return fmt.Sprintf("\"%s\"", identifier)
}

// Db2QuoteIfNeeded quotes an identifier the way QuoteForFoldUpper does, and
// additionally quotes one that does not begin with a letter: like Oracle, Db2
// rejects an unquoted identifier starting with `_` (SQLCODE -20521).
func Db2QuoteIfNeeded(identifier string) string {
	if !startsWithLetter(identifier) {
		return Db2QuoteIdentifier(strings.ReplaceAll(identifier, `"`, `""`))
	}
	return QuoteForFoldUpper(identifier)
}
