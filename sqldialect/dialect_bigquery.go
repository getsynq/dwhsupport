package sqldialect

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

//
// BigQueryDialect
//

var _ Dialect = (*BigQueryDialect)(nil)

type BigQueryDialect struct{}

func NewBigQueryDialect() *BigQueryDialect {
	return &BigQueryDialect{}
}

func (d *BigQueryDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	if fqn == nil {
		return "", errors.New("fqn is nil")
	}
	return fmt.Sprintf("`%s.%s.%s`", fqn.projectId, fqn.datasetId, fqn.tableId), nil
}

func (d *BigQueryDialect) ResolveTableFunction(t *TableFnExpr) (string, error) {
	if t == nil {
		return "", errors.New("table_fn is nil")
	}
	return Fn(t.name, t.ops...).ToSql(d)
}

func (d *BigQueryDialect) CountIf(expr Expr) Expr {
	return Fn("countif", expr)
}

func (d *BigQueryDialect) Count(expr Expr) Expr {
	return Fn("count", expr)
}

func (d *BigQueryDialect) Median(expr Expr) Expr {
	return WrapSql("approx_quantiles(%s, 2)[offset(1)]", expr)
}

func (d *BigQueryDialect) Stddev(expr Expr) Expr {
	return Fn("stddev_samp", expr)
}

func (d *BigQueryDialect) ResolveTime(t time.Time) (string, error) {
	return fmt.Sprintf("timestamp '%s'", t.Format(time.RFC3339)), nil
}

func (d *BigQueryDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return fmt.Sprintf("timestamp(%s)", d.ResolveFieldRef(expr.name)), nil
}

func (d *BigQueryDialect) RoundTime(expr Expr, duration time.Duration) Expr {
	unit, _ := getTimeUnitWithInterval(duration)

	return Fn("timestamp_trunc", expr, timeUnitSql(unit))
}

func (d *BigQueryDialect) CeilTime(expr Expr, interval time.Duration) Expr {
	return d.AddTime(d.RoundTime(expr, interval), interval)
}

func (d *BigQueryDialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("TIMESTAMP_ADD(%s, INTERVAL %s %s)", expr, Int64(-1*interval), timeUnitSql(unit))
}

func (d *BigQueryDialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("TIMESTAMP_ADD(%s, INTERVAL %s %s)", expr, Int64(interval), timeUnitSql(unit))
}

func (d *BigQueryDialect) CurrentTimestamp() Expr {
	return Fn("CURRENT_TIMESTAMP")
}

// QuoteIdent renders name wrapped in this dialect's identifier delimiters,
// always — unlike Identifier, which quotes only when a character demands it
// and so lets a reserved word through bare.
func (d *BigQueryDialect) QuoteIdent(name string) string {
	return identQuotingBackticksEscape.quote(name)
}

// UnquoteIdent is the inverse: it strips one layer of delimiters and decodes
// the escape inside, reporting whether the text carried any.
func (d *BigQueryDialect) UnquoteIdent(text string) (string, bool) {
	return identQuotingBackticksEscape.unquote(text)
}

// FoldIdent returns the name an unquoted reference resolves to.
// Dataset and table names are case-sensitive and column names are matched
// case-insensitively, so an unquoted reference resolves to the name as
// written and there is nothing to fold.
func (d *BigQueryDialect) FoldIdent(name string) string {
	return name
}

func (d *BigQueryDialect) Identifier(identifier string) string {
	return QuoteWithBackticksIfNeeded(identifier)
}

// ResolveFieldRef returns the SQL reference for a user-supplied field name.
// Expressions (containing function calls, casts, JSON operators, etc.) are
// returned as-is; plain identifiers are backtick-quoted only when needed.
func (d *BigQueryDialect) ResolveFieldRef(name string) string {
	if isLikelyExpression(name) || isQuotedWith(name, '`', '`') {
		return name
	}
	return QuoteWithBackticksIfNeeded(name)
}

func (d *BigQueryDialect) StringLiteral(s string) string {
	return StandardSQLStringLiteral(s)
}

func (d *BigQueryDialect) ToString(expr Expr) Expr {
	return WrapSql("SAFE_CAST(%s AS STRING)", expr)
}

func (d *BigQueryDialect) ToFloat64(expr Expr) Expr {
	return WrapSql("CAST(%s AS FLOAT64)", expr)
}

func (d *BigQueryDialect) Coalesce(exprs ...Expr) Expr {
	return Fn("COALESCE", exprs...)
}

func (d *BigQueryDialect) ConcatWithSeparator(separator string, exprs ...Expr) Expr {
	// BigQuery doesn't have CONCAT_WS, so we build CONCAT(expr1, '|', expr2, '|', expr3)
	if len(exprs) == 0 {
		return String("")
	}
	if len(exprs) == 1 {
		return exprs[0]
	}
	parts := make([]Expr, 0, len(exprs)*2-1)
	for i, expr := range exprs {
		parts = append(parts, expr)
		if i < len(exprs)-1 {
			parts = append(parts, String(separator))
		}
	}
	return Fn("CONCAT", parts...)
}

func (d *BigQueryDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return expression
}

func (d *BigQueryDialect) SubString(expr Expr, start int64, length int64) Expr {
	return Fn("SUBSTR", expr, Int64(start), Int64(length))
}

func (d *BigQueryDialect) StringLength(expr Expr) Expr {
	return Fn("LENGTH", expr)
}

func (d *BigQueryDialect) FormatLimit(rowsSql string) string {
	return fmt.Sprintf("limit %s", rowsSql)
}

func (d *BigQueryDialect) SupportsCrossDatabaseQueries() bool { return true }

func (d *BigQueryDialect) SupportsAsBeforeTableAlias() bool { return true }
