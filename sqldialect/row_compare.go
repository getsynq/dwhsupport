package sqldialect

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

// RowCompare builds a lexicographic comparison of a column tuple against a
// value tuple: `(cols...) <fn> (vals...)`, where fn is one of <, <=, >, >=
// (equality should use per-column Eq). It is the dialect-aware way to express a
// half-open key-range scan `(a, b) >= (lo0, lo1) AND (a, b) < (hi0, hi1)` for a
// composite key.
//
// The two equivalent renderings differ wildly in how engines optimise them, and
// the better one is engine-specific, so RowCompare lets each dialect pick:
//
//   - Native row-value constructor `(a, b) >= (x, y)` — Postgres and MySQL
//     decompose this into a leading-column index range scan (sargable). The
//     lexicographic OR-expansion, by contrast, confuses their planners into a
//     bitmap union that over-scans.
//   - Lexicographic OR-expansion `(a > x) OR (a = x AND b >= y)` — ClickHouse
//     prunes its primary-key granules from this form, but does NOT prune from a
//     native tuple comparison (it scans every granule). BigQuery / Snowflake /
//     Redshift / MSSQL don't support `<`/`>` on row constructors at all, so the
//     expansion is also their only valid form.
//
// Both renderings are semantically identical (same total order, NULLs aside).
// A dialect opts into the native form by implementing rowValueComparer; every
// other dialect falls back to the portable expansion, which is valid SQL
// everywhere.
func RowCompare(cols []Expr, fn CompareFn, vals []Expr) CondExpr {
	return &RowCompareExpr{cols: cols, vals: vals, fn: fn}
}

type RowCompareExpr struct {
	cols []Expr
	vals []Expr
	fn   CompareFn
}

var _ CondExpr = (*RowCompareExpr)(nil)

// rowValueComparer is the optional interface a dialect implements to declare
// that a native row-value constructor comparison is both supported and the
// index-friendly form for it. Dialects that don't implement it get the
// lexicographic expansion. Kept unexported: opting in is a property of the
// concrete dialect, not something callers toggle.
type rowValueComparer interface {
	prefersRowValueComparison() bool
}

func (e *RowCompareExpr) ToSql(dialect Dialect) (string, error) {
	if len(e.cols) != len(e.vals) {
		// A dropped component would silently emit a different, over-broad
		// predicate (wrong rows, no failure) — so reject the mismatch rather
		// than truncate to the shorter tuple.
		return "", errors.Errorf("RowCompare: mismatched tuple lengths (%d columns, %d values)", len(e.cols), len(e.vals))
	}
	n := len(e.cols)
	if n == 0 {
		return "", errors.New("RowCompare: empty column or value tuple")
	}
	// A single component is a plain scalar comparison in every dialect — no
	// tuple machinery, no divergent planning, so emit it directly.
	if n == 1 {
		return compare(e.cols[0], e.fn, e.vals[0]).ToSql(dialect)
	}

	if rv, ok := dialect.(rowValueComparer); ok && rv.prefersRowValueComparison() {
		return e.nativeRowValue(dialect, n)
	}
	return e.expand(n).ToSql(dialect)
}

func (e *RowCompareExpr) IsCondExpr() {}

// nativeRowValue renders `(c0, c1, ...) fn (v0, v1, ...)`.
func (e *RowCompareExpr) nativeRowValue(dialect Dialect, n int) (string, error) {
	colsSql := make([]string, n)
	valsSql := make([]string, n)
	for i := 0; i < n; i++ {
		c, err := e.cols[i].ToSql(dialect)
		if err != nil {
			return "", err
		}
		v, err := e.vals[i].ToSql(dialect)
		if err != nil {
			return "", err
		}
		colsSql[i] = c
		valsSql[i] = v
	}
	return fmt.Sprintf("(%s) %s (%s)",
		strings.Join(colsSql, ", "), e.fn, strings.Join(valsSql, ", ")), nil
}

// expand builds the portable lexicographic OR-expansion. For `(c) fn (v)` with
// fn ∈ {<, <=, >, >=} it emits, for each level i:
//
//	(c0 = v0 AND ... AND c_{i-1} = v_{i-1} AND c_i <step> v_i)
//
// OR-joined across levels, where <step> is the strict operator (< or >) at every
// non-final level and the requested (possibly non-strict) operator at the final
// level. That is exactly the standard total-order definition of a tuple
// comparison.
func (e *RowCompareExpr) expand(n int) CondExpr {
	nonFinal, final := stepOps(e.fn)

	groups := make([]CondExpr, 0, n)
	for i := 0; i < n; i++ {
		level := make([]CondExpr, 0, i+1)
		for j := 0; j < i; j++ {
			level = append(level, Eq(e.cols[j], e.vals[j]))
		}
		if i == n-1 {
			level = append(level, compare(e.cols[i], final, e.vals[i]))
		} else {
			level = append(level, compare(e.cols[i], nonFinal, e.vals[i]))
		}
		if len(level) == 1 {
			groups = append(groups, level[0])
		} else {
			groups = append(groups, AndGroups(level...))
		}
	}
	if len(groups) == 1 {
		return groups[0]
	}
	return Or(groups...)
}

// stepOps maps a tuple comparison operator to (non-final-level op, final-level
// op): non-final levels are always strict in the comparison direction; only the
// final level carries the inclusive/exclusive distinction.
func stepOps(fn CompareFn) (nonFinal, final CompareFn) {
	switch fn {
	case COMPARE_LT:
		return COMPARE_LT, COMPARE_LT
	case COMPARE_LTE:
		return COMPARE_LT, COMPARE_LTE
	case COMPARE_GT:
		return COMPARE_GT, COMPARE_GT
	case COMPARE_GTE:
		return COMPARE_GT, COMPARE_GTE
	default:
		// Defensive: only ordering operators define a lexicographic expansion.
		return fn, fn
	}
}
