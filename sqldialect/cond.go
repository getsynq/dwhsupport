package sqldialect

import (
	"fmt"
	"strings"
)

//
// CompareExpr
//

type CompareFn string

const (
	COMPARE_EQ     CompareFn = "="
	COMPARE_LT     CompareFn = "<"
	COMPARE_GT     CompareFn = ">"
	COMPARE_LTE    CompareFn = "<="
	COMPARE_GTE    CompareFn = ">="
	COMPARE_IS     CompareFn = "is"
	COMPARE_IS_NOT CompareFn = "is not"
)

type CompareExpr struct {
	a  Expr
	b  Expr
	fn CompareFn
}

func compare(a Expr, fn CompareFn, b Expr) *CompareExpr {
	return &CompareExpr{a: a, b: b, fn: fn}
}

func (e *CompareExpr) ToSql(dialect Dialect) (string, error) {
	aSql, err := e.a.ToSql(dialect)
	if err != nil {
		return "", err
	}

	bSql, err := e.b.ToSql(dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s %s %s", aSql, e.fn, bSql), nil
}

func (e *CompareExpr) IsCondExpr() {}

//
// BetweenExpr
//

type BetweenExpr struct {
	col  Expr
	from Expr
	to   Expr
}

func Between(col, from, to Expr) *BetweenExpr {
	return &BetweenExpr{col: col, from: from, to: to}
}

func (e *BetweenExpr) ToSql(dialect Dialect) (string, error) {
	colSql, err := e.col.ToSql(dialect)
	if err != nil {
		return "", err
	}

	fromSql, err := e.from.ToSql(dialect)
	if err != nil {
		return "", err
	}

	toSql, err := e.to.ToSql(dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s between %s and %s", colSql, fromSql, toSql), nil
}

func (e *BetweenExpr) IsCondExpr() {}

//
// InExpr
//

type InExpr struct {
	col  Expr
	list []Expr
}

func In(col Expr, list ...Expr) *InExpr {
	return &InExpr{col: col, list: list}
}

func (e *InExpr) ToSql(dialect Dialect) (string, error) {
	colSql, err := e.col.ToSql(dialect)
	if err != nil {
		return "", err
	}

	inSql := []string{}
	for _, expr := range e.list {
		exprSql, err := expr.ToSql(dialect)
		if err != nil {
			return "", err
		}

		inSql = append(inSql, exprSql)
	}

	return fmt.Sprintf("%s in (%s)", colSql, strings.Join(inSql, ", ")), nil
}

func (e *InExpr) IsCondExpr() {}

//
// NotInExpr
//

type NotInExpr struct {
	col  Expr
	list []Expr
}

func NotIn(col Expr, list ...Expr) *NotInExpr {
	return &NotInExpr{col: col, list: list}
}

func (e *NotInExpr) ToSql(dialect Dialect) (string, error) {
	colSql, err := e.col.ToSql(dialect)
	if err != nil {
		return "", err
	}

	inSql := []string{}
	for _, expr := range e.list {
		exprSql, err := expr.ToSql(dialect)
		if err != nil {
			return "", err
		}

		inSql = append(inSql, exprSql)
	}

	return fmt.Sprintf("%s not in (%s)", colSql, strings.Join(inSql, ", ")), nil
}

func (e *NotInExpr) IsCondExpr() {}

//
// Shortcuts
//

func Eq(a, b Expr) *CompareExpr {
	return compare(a, COMPARE_EQ, b)
}

func Gt(a, b Expr) *CompareExpr {
	return compare(a, COMPARE_GT, b)
}

func Lt(a, b Expr) *CompareExpr {
	return compare(a, COMPARE_LT, b)
}

func Gte(a, b Expr) *CompareExpr {
	return compare(a, COMPARE_GTE, b)
}

func Lte(a, b Expr) *CompareExpr {
	return compare(a, COMPARE_LTE, b)
}

func IsNull(a Expr) *CompareExpr {
	return compare(a, COMPARE_IS, Null())
}

func IsNotNull(a Expr) *CompareExpr {
	return compare(a, COMPARE_IS_NOT, Null())
}

type OrExpr struct {
	conds []CondExpr
}

func Or(conds ...CondExpr) *OrExpr {
	return &OrExpr{conds: conds}
}

func (e *OrExpr) ToSql(dialect Dialect) (string, error) {
	if len(e.conds) == 0 {
		return "", nil
	}

	condsSql, err := exprsToSql(e.conds, dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("(%s)", strings.Join(condsSql, " or ")), nil
}

func (e *OrExpr) IsCondExpr() {}
