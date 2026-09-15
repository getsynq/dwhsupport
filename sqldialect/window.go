package sqldialect

import (
	"fmt"
	"strings"
)

//
// WindowExpr - `<fn> over (partition by ... order by ...)`
//

// WindowExpr renders a window function call: the function, then an OVER clause
// carrying an optional PARTITION BY and ORDER BY.
//
// Frame clauses (ROWS/RANGE BETWEEN) are deliberately not modelled — nothing
// needs them yet, and they are the part of the window syntax the dialects
// disagree most about.
type WindowExpr struct {
	fn          Expr
	partitionBy []Expr
	orderBy     []OrderByExpr
}

var _ Expr = (*WindowExpr)(nil)
var _ NumericExpr = (*WindowExpr)(nil)

// Over wraps a function call in an OVER clause. Call PartitionBy and OrderBy
// to fill the clause in.
func Over(fn Expr) *WindowExpr {
	return &WindowExpr{fn: fn}
}

// PartitionBy appends PARTITION BY terms.
func (e *WindowExpr) PartitionBy(exprs ...Expr) *WindowExpr {
	e.partitionBy = append(e.partitionBy, exprs...)
	return e
}

// OrderBy appends ORDER BY terms to the window, using the same Asc/Desc terms
// as a Select's ORDER BY.
func (e *WindowExpr) OrderBy(orderBy ...OrderByExpr) *WindowExpr {
	e.orderBy = append(e.orderBy, orderBy...)
	return e
}

func (e *WindowExpr) ToSql(dialect Dialect) (string, error) {
	fnSql, err := e.fn.ToSql(dialect)
	if err != nil {
		return "", err
	}

	var clauses []string

	if len(e.partitionBy) > 0 {
		sqls, err := exprsToSql(e.partitionBy, dialect)
		if err != nil {
			return "", err
		}
		clauses = append(clauses, "partition by "+strings.Join(sqls, ", "))
	}

	if len(e.orderBy) > 0 {
		sqls, err := exprsToSql(e.orderBy, dialect)
		if err != nil {
			return "", err
		}
		clauses = append(clauses, "order by "+strings.Join(sqls, ", "))
	}

	return fmt.Sprintf("%s over (%s)", fnSql, strings.Join(clauses, " ")), nil
}

func (e *WindowExpr) IsNumericExpr() {}

// RowNumber builds a ROW_NUMBER() call. It is only valid inside an Over.
func RowNumber() *FnExpr {
	return Fn("ROW_NUMBER")
}

// Ntile builds an NTILE(buckets) call, which splits the ordered partition into
// that many groups. It is only valid inside an Over, and every dialect we
// support requires that Over to carry an ORDER BY.
func Ntile(buckets Expr) *FnExpr {
	return Fn("NTILE", buckets)
}
