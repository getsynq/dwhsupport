package sqldialect

//
// AliasExpr - a column alias the builder mints and later reads back
//

// AliasExpr is the name a builder gives a result column, spelled so that
// reading the value back and referring to the alias one level up both resolve
// to the same thing.
//
// Identifier is the wrong tool for this. It quotes unconditionally on the
// dialects whose Identifier is a quoting function (Oracle, ClickHouse,
// BigQuery), which pins the alias to the exact case it was written in, while
// every reference to it — a QualifiedCol, an outer WHERE, a GROUP BY — goes
// through ResolveFieldRef and folds. `as "key_val"` next to a reference to
// `key_val` is two different columns on Oracle, and the query fails or, worse,
// resolves to a same-named column of the inner table.
//
// Alias therefore resolves through ResolveFieldRef, the same path every
// reference takes, so the two agree by construction. What the warehouse then
// reports the column as is still its own choice — a folding account spells
// `key_val` back as KEY_VAL — so reads must stay case-insensitive
// (exec.QueryMapResult.Get).
type AliasExpr struct {
	name string
}

var _ Expr = (*AliasExpr)(nil)
var _ TextExpr = (*AliasExpr)(nil)

// Alias builds the alias half of an As expression.
func Alias(name string) *AliasExpr {
	return &AliasExpr{name: name}
}

func (e *AliasExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.ResolveFieldRef(e.name), nil
}

func (e *AliasExpr) IsTextExpr() {}

// Name returns the alias as it was asked for, before the dialect spelled it.
// Callers reading a result column back use this together with a
// case-insensitive lookup.
func (e *AliasExpr) Name() string {
	return e.name
}

//
// QualifiedColExpr - a column reference qualified by a table alias
//

// QualifiedColExpr renders `qualifier.name`, both halves resolved by the
// dialect.
//
// Formatting `alias.col` into a string does not survive case folding: Oracle
// and Snowflake fold an unquoted identifier to upper case, Postgres, Redshift
// and Trino fold it to lower, and a quoted one keeps the case it was written
// in. The qualifier therefore has to be spelled the same way the alias it
// refers to was emitted — which is what SubqueryTableExpr and this expression
// both do by running the identifier through ResolveFieldRef.
//
// Passing the whole `alias.col` string to TextCol instead would not work:
// ResolveFieldRef treats a `.` as the marker of an expression and passes the
// string through raw, so neither half gets quoted.
type QualifiedColExpr struct {
	qualifier string
	name      string
}

var _ Expr = (*QualifiedColExpr)(nil)
var _ TextExpr = (*QualifiedColExpr)(nil)
var _ NumericExpr = (*QualifiedColExpr)(nil)

// QualifiedCol builds a column reference qualified by a table alias.
func QualifiedCol(qualifier string, name string) *QualifiedColExpr {
	return &QualifiedColExpr{qualifier: qualifier, name: name}
}

func (e *QualifiedColExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.ResolveFieldRef(e.qualifier) + "." + dialect.ResolveFieldRef(e.name), nil
}

func (e *QualifiedColExpr) IsTextExpr()    {}
func (e *QualifiedColExpr) IsNumericExpr() {}
