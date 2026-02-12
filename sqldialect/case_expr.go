package sqldialect

import (
	"fmt"
	"strings"
)

// CaseExpr represents a SQL CASE expression
type CaseExpr struct {
	whens    []caseWhen
	elseExpr Expr
}

type caseWhen struct {
	cond CondExpr
	then Expr
}

var _ Expr = (*CaseExpr)(nil)
var _ NumericExpr = (*CaseExpr)(nil)

// Case creates a new CASE expression builder
func Case() *CaseExpr {
	return &CaseExpr{
		whens: []caseWhen{},
	}
}

// When adds a WHEN condition to the CASE expression
func (e *CaseExpr) When(cond CondExpr, then Expr) *CaseExpr {
	e.whens = append(e.whens, caseWhen{cond: cond, then: then})
	return e
}

// Else sets the ELSE value for the CASE expression
func (e *CaseExpr) Else(expr Expr) *CaseExpr {
	e.elseExpr = expr
	return e
}

// ToSql renders the CASE expression to SQL
func (e *CaseExpr) ToSql(dialect Dialect) (string, error) {
	var parts []string
	parts = append(parts, "CASE")

	for _, when := range e.whens {
		condSql, err := when.cond.ToSql(dialect)
		if err != nil {
			return "", err
		}
		thenSql, err := when.then.ToSql(dialect)
		if err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf("WHEN %s THEN %s", condSql, thenSql))
	}

	if e.elseExpr != nil {
		elseSql, err := e.elseExpr.ToSql(dialect)
		if err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf("ELSE %s", elseSql))
	}

	parts = append(parts, "END")
	return strings.Join(parts, " "), nil
}

func (e *CaseExpr) IsNumericExpr() {}
