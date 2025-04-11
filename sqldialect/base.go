package sqldialect

import (
	"fmt"
	"strings"
	"time"
)

//
// Base
//

type Expr interface {
	ToSql(dialect Dialect) (string, error)
}

type TextExpr interface {
	Expr
	IsTextExpr()
}

type NumericExpr interface {
	Expr
	IsNumericExpr()
}

type TimeExpr interface {
	Expr
	IsTimeExpr()
}

type ToCondExpr interface {
	ToCondExpr() CondExpr
}

type CondExpr interface {
	Expr
	IsCondExpr()
}

type TableExpr interface {
	Expr
	IsTableExpr()
}

type LimitExpr struct {
	rows *IntLitExpr
}

var _ Expr = (*LimitExpr)(nil)

func Limit(rows *IntLitExpr) *LimitExpr {
	return &LimitExpr{rows}
}

func (e *LimitExpr) ToSql(dialect Dialect) (string, error) {
	rowsSql, err := e.rows.ToSql(dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("limit %s", rowsSql), nil
}

type NotImplementedExpr struct {
	msg string
}

var _ Expr = (*NotImplementedExpr)(nil)

func NotImplementedWithExplanation(msg string) *NotImplementedExpr {
	return &NotImplementedExpr{msg: msg}
}

func NotImplemented() *NotImplementedExpr {
	return &NotImplementedExpr{}
}

func (e *NotImplementedExpr) ToSql(dialect Dialect) (string, error) {
	return "", fmt.Errorf("not implemented %s", e.msg)
}

// IdentifierExpr
type IdentifierExpr struct {
	identifier string
}

var _ Expr = (*IdentifierExpr)(nil)
var _ TextExpr = (*IdentifierExpr)(nil)

func Identifier(identifier string) *IdentifierExpr {
	return &IdentifierExpr{identifier: identifier}
}

func (i *IdentifierExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.Identifier(i.identifier), nil
}

func (s *IdentifierExpr) IsTextExpr() {}

//
// OrderExpr
//

type OrderExpr struct {
	expr Expr
	desc bool
}

var _ Expr = (*OrderExpr)(nil)

func Asc(expr Expr) *OrderExpr {
	return &OrderExpr{expr: expr, desc: false}
}

func Desc(expr Expr) *OrderExpr {
	return &OrderExpr{expr: expr, desc: true}
}

func (e *OrderExpr) ToSql(dialect Dialect) (string, error) {
	exprSql, err := e.expr.ToSql(dialect)
	if err != nil {
		return "", err
	}

	if e.desc {
		return fmt.Sprintf("%s desc", exprSql), nil
	}

	return exprSql, nil
}

//
// SelectExpr
//

type Select struct {
	cols    []Expr
	table   TableExpr
	joins   []*JoinExpr
	where   []CondExpr
	groupBy []Expr
	orderBy []*OrderExpr
	limit   *LimitExpr
}

func NewSelect() *Select {
	return &Select{}
}

func (s *Select) From(table TableExpr) *Select {
	s.table = table
	return s
}

func (s *Select) Where(conds ...CondExpr) *Select {
	s.where = append(s.where, conds...)
	return s
}

func (s *Select) Cols(cols ...Expr) *Select {
	s.cols = append(s.cols, cols...)
	return s
}

func (s *Select) Join(other TableExpr, how JoinDefExpr) *Select {
	s.joins = append(s.joins, Join(other, how))
	return s
}

func (s *Select) GroupBy(groupBy ...Expr) *Select {
	s.groupBy = append(s.groupBy, groupBy...)
	return s
}

func (s *Select) OrderBy(orderBy ...*OrderExpr) *Select {
	s.orderBy = append(s.orderBy, orderBy...)
	return s
}

func (s *Select) WithLimit(limit *LimitExpr) *Select {
	s.limit = limit
	return s
}

func (s *Select) ToSql(dialect Dialect) (string, error) {
	colsSql, err := exprsToSql(s.cols, dialect)
	if err != nil {
		return "", err
	}

	tableSql, err := s.table.ToSql(dialect)
	if err != nil {
		return "", err
	}

	whereSql, err := exprsToSql(s.where, dialect)
	if err != nil {
		return "", err
	}

	groupBySql, err := exprsToSql(s.groupBy, dialect)
	if err != nil {
		return "", err
	}

	orderBySql, err := exprsToSql(s.orderBy, dialect)
	if err != nil {
		return "", err
	}

	joinsSql, err := exprsToSql(s.joins, dialect)
	if err != nil {
		return "", err
	}

	limitSql := ""
	if s.limit != nil {
		limitSql, err = s.limit.ToSql(dialect)
		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf(`%s
from %s %s
%s
%s
%s %s`,
		buildListSegment("select", ", ", colsSql),
		tableSql,
		strings.Join(joinsSql, " "),
		buildListSegment("where", " and ", whereSql),
		buildListSegment("group by", ", ", groupBySql),
		buildListSegment("order by", ", ", orderBySql),
		limitSql,
	), nil
}

func buildListSegment(segmentId string, separator string, sqls []string) string {
	if len(sqls) == 0 {
		return ""
	}
	if len(sqls) == 1 {
		return fmt.Sprintf(`%s %s`, segmentId, strings.Join(sqls, separator))
	}

	separator = fmt.Sprintf("%s\n  ", separator)
	return fmt.Sprintf("%s\n  %s", segmentId, strings.Join(sqls, separator))
}

func exprsToSql[T Expr](exprs []T, dialect Dialect) ([]string, error) {
	sqls := []string{}
	for _, expr := range exprs {
		exprSql, err := expr.ToSql(dialect)
		if err != nil {
			return nil, err
		}

		sqls = append(sqls, exprSql)
	}

	return sqls, nil
}

//
// Expr
//

type StarExpr struct {
	except []Expr
}

var _ Expr = (*StarExpr)(nil)

func Star(except ...Expr) *StarExpr {
	return &StarExpr{except: except}
}

func (s *StarExpr) ToSql(dialect Dialect) (string, error) {
	return "*", nil
}

//
// SqlExpr
//

type SqlExpr struct {
	sql string
}

var _ Expr = (*SqlExpr)(nil)

func Sql(sql string) *SqlExpr {
	return &SqlExpr{sql: sql}
}

func (s *SqlExpr) ToSql(dialect Dialect) (string, error) {
	return s.sql, nil
}

func (s *SqlExpr) IsTextExpr()    {}
func (s *SqlExpr) IsNumericExpr() {}
func (s *SqlExpr) IsTimeExpr()    {}
func (s *SqlExpr) IsCondExpr()    {}
func (s *SqlExpr) IsTableExpr()   {}
func (s *SqlExpr) IsJoinExpr()    {}

//
// Col
//

type ColBaseExpr struct {
	sql string
}

func (s *ColBaseExpr) ToSql(dialect Dialect) (string, error) {
	return s.sql, nil
}

type TextColExpr struct {
	ColBaseExpr
}

var _ Expr = (*TextColExpr)(nil)
var _ TextExpr = (*TextColExpr)(nil)

func TextCol(name string) *TextColExpr {
	return &TextColExpr{ColBaseExpr: ColBaseExpr{sql: name}}
}

func (s *TextColExpr) IsTextExpr() {}

type TimeColExpr struct {
	name string
}

var _ Expr = (*TimeColExpr)(nil)
var _ TimeExpr = (*TimeColExpr)(nil)

func TimeCol(name string) *TimeColExpr {
	return &TimeColExpr{name: name}
}

func (t *TimeColExpr) IsTimeExpr() {}

func (t *TimeColExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.ResolveTimeColumn(t)
}

type NumericColExpr struct {
	ColBaseExpr
}

var _ Expr = (*NumericColExpr)(nil)
var _ NumericExpr = (*NumericColExpr)(nil)

func NumericCol(name string) *NumericColExpr {
	return &NumericColExpr{ColBaseExpr: ColBaseExpr{sql: name}}
}

func (s *NumericColExpr) IsNumericExpr() {}

//
// TableFqn
//

type TableFqnExpr struct {
	projectId string
	datasetId string
	tableId   string
}

var _ Expr = (*TableFqnExpr)(nil)
var _ TableExpr = (*TableFqnExpr)(nil)

func TableFqn(projectId, datasetId, tableId string) *TableFqnExpr {
	return &TableFqnExpr{
		projectId: projectId,
		datasetId: datasetId,
		tableId:   tableId,
	}
}

func (t *TableFqnExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.ResolveFqn(t)
}

func (t *TableFqnExpr) IsTableExpr() {}

//
// JoinExpr
//

type JoinExpr struct {
	other TableExpr
	how   JoinDefExpr
}

var _ Expr = (*JoinExpr)(nil)
var _ TableExpr = (*JoinExpr)(nil)

func Join(other TableExpr, how JoinDefExpr) *JoinExpr {
	return &JoinExpr{
		other: other,
		how:   how,
	}
}

func (t *JoinExpr) ToSql(dialect Dialect) (string, error) {
	tableSql, err := t.other.ToSql(dialect)
	if err != nil {
		return "", err
	}

	howSql, err := t.how.ToSql(dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("join %s %s", tableSql, howSql), nil
}

func (t *JoinExpr) IsJoinExpr()  {}
func (t *JoinExpr) IsTableExpr() {}

type JoinDefExpr interface {
	Expr
	IsJoinDefExpr()
}

type JoinOnExpr struct {
	conds []CondExpr
}

var _ Expr = (*JoinOnExpr)(nil)
var _ JoinDefExpr = (*JoinOnExpr)(nil)

func On(conds ...CondExpr) *JoinOnExpr {
	return &JoinOnExpr{conds: conds}
}

func (t *JoinOnExpr) ToSql(dialect Dialect) (string, error) {
	sqls, err := exprsToSql(t.conds, dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("on %s", strings.Join(sqls, " and ")), nil
}

func (t *JoinOnExpr) IsJoinDefExpr() {}

type JoinUsingExpr struct {
	exprs []Expr
}

var _ Expr = (*JoinUsingExpr)(nil)
var _ JoinDefExpr = (*JoinUsingExpr)(nil)

func Using(exprs ...Expr) *JoinUsingExpr {
	return &JoinUsingExpr{exprs: exprs}
}

func (t *JoinUsingExpr) ToSql(dialect Dialect) (string, error) {
	sqls, err := exprsToSql(t.exprs, dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("using (%s)", strings.Join(sqls, ", ")), nil
}

func (t *JoinUsingExpr) IsJoinDefExpr() {}

//
// Placeholder
//

type ArgExpr struct {
	pos int
}

var _ Expr = (*ArgExpr)(nil)

func Arg(pos int) *ArgExpr {
	return &ArgExpr{pos: pos}
}

func (e *ArgExpr) ToSql(dialect Dialect) (string, error) {
	return fmt.Sprintf("$%d", e.pos), nil
}

//
// LitExpr
//

type StringLitExpr struct {
	val string
}

var _ Expr = (*StringLitExpr)(nil)
var _ TextExpr = (*StringLitExpr)(nil)

func String(val string) *StringLitExpr {
	return &StringLitExpr{val: val}
}

func Stringf(format string, val ...any) *StringLitExpr {
	return &StringLitExpr{val: fmt.Sprintf(format, val...)}
}

func (e *StringLitExpr) ToSql(dialect Dialect) (string, error) {
	return fmt.Sprintf("'%v'", e.val), nil
}

func (e *StringLitExpr) IsTextExpr() {}

//
// IntLitExpr
//

var _ Expr = (*IntLitExpr)(nil)

type IntLitExpr struct {
	val int64
}

func Int64(val int64) *IntLitExpr {
	return &IntLitExpr{val: val}
}

func (e *IntLitExpr) ToSql(dialect Dialect) (string, error) {
	return fmt.Sprintf("%d", e.val), nil
}

func (e *IntLitExpr) IsNumericExpr() {}

//
// TimeLitExpr
//

var _ Expr = (*TimeLitExpr)(nil)

type TimeLitExpr struct {
	val *time.Time
}

func Time(val time.Time) *TimeLitExpr {
	return &TimeLitExpr{val: &val}
}

func (e *TimeLitExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.ResolveTime(*e.val)
}

func (e *TimeLitExpr) IsTimeExpr() {}

//
// AsExpr
//

var _ Expr = (*AsExpr)(nil)

type AsExpr struct {
	expr  Expr
	alias TextExpr
}

func As(expr Expr, alias TextExpr) *AsExpr {
	return &AsExpr{expr: expr, alias: alias}
}

func (e *AsExpr) ToSql(dialect Dialect) (string, error) {
	exprSql, err := e.expr.ToSql(dialect)
	if err != nil {
		return "", err
	}

	aliasSql, err := e.alias.ToSql(dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s as %s", exprSql, aliasSql), nil
}

//
// DistinctExpr
//

var _ Expr = (*DistinctExpr)(nil)

type DistinctExpr struct {
	expr Expr
}

func Distinct(expr Expr) *DistinctExpr {
	return &DistinctExpr{expr: expr}
}

func (e *DistinctExpr) ToSql(dialect Dialect) (string, error) {
	exprSql, err := e.expr.ToSql(dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("distinct %s", exprSql), nil
}

//
// CoalesceExpr
//

var _ Expr = (*CoalesceExpr)(nil)

type CoalesceExpr struct {
	exprs []Expr
}

func Coalesce(exprs ...Expr) *CoalesceExpr {
	return &CoalesceExpr{exprs: exprs}
}

func (e *CoalesceExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.Coalesce(e.exprs...).ToSql(dialect)
}

//
// ToStringExpr
//

var _ Expr = (*CountAllExpr)(nil)
var _ NumericExpr = (*CountAllExpr)(nil)

type CountAllExpr struct {
}

func (c CountAllExpr) IsNumericExpr() {}

func (c CountAllExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.Count(Star()).ToSql(dialect)
}

func CountAll() *CountAllExpr {
	return &CountAllExpr{}
}

var _ Expr = (*ToStringExpr)(nil)

type ToStringExpr struct {
	expr Expr
}

func (e *ToStringExpr) IsTextExpr() {}

func ToString(expr Expr) *ToStringExpr {
	return &ToStringExpr{expr: expr}
}

func (e *ToStringExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.ToString(e.expr).ToSql(dialect)
}

var _ Expr = (*SubStringExpr)(nil)

type SubStringExpr struct {
	expr   Expr
	start  int64
	length int64
}

func (s SubStringExpr) IsTextExpr() {}

func SubString(expr Expr, start int64, length int64) *SubStringExpr {
	return &SubStringExpr{
		expr:   expr,
		start:  start,
		length: length,
	}
}

func (s SubStringExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.SubString(s.expr, s.start, s.length).ToSql(dialect)
}

type ToFloat64Expr struct {
	expr Expr
}

var _ Expr = (*ToFloat64Expr)(nil)
var _ NumericExpr = (*ToFloat64Expr)(nil)

func ToFloat64(expr Expr) *ToFloat64Expr {
	return &ToFloat64Expr{expr: expr}
}

func (e *ToFloat64Expr) ToSql(dialect Dialect) (string, error) {
	return dialect.ToFloat64(e.expr).ToSql(dialect)
}

func (e *ToFloat64Expr) IsNumericExpr() {}

//
// FnExpr
//

type FnExpr struct {
	name string
	ops  []Expr
}

var _ Expr = (*FnExpr)(nil)

func Fn(name string, ops ...Expr) *FnExpr {
	return &FnExpr{name: name, ops: ops}
}

func (e *FnExpr) ToSql(dialect Dialect) (string, error) {
	ops := []string{}
	for _, op := range e.ops {
		opSql, err := op.ToSql(dialect)
		if err != nil {
			return "", err
		}

		ops = append(ops, opSql)
	}

	return fmt.Sprintf("%s(%s)", e.name, strings.Join(ops, ", ")), nil
}

//
// WrapSqlExpr
//

type WrapSqlExpr struct {
	sql     string
	wrapped []Expr
}

var _ Expr = (*WrapSqlExpr)(nil)

func WrapSql(sql string, wrapped ...Expr) *WrapSqlExpr {
	return &WrapSqlExpr{sql: sql, wrapped: wrapped}
}

func (e *WrapSqlExpr) ToSql(dialect Dialect) (string, error) {
	anySql := []interface{}{}
	for _, expr := range e.wrapped {
		exprSql, err := expr.ToSql(dialect)
		if err != nil {
			return "", err
		}

		anySql = append(anySql, exprSql)
	}

	return fmt.Sprintf(e.sql, anySql...), nil
}

//
// AggregationColumnReferenceExpr
// Not all databases support referencing aggregation columns by alias in WHERE.
// E.g. postgresql does not support it.
//

type AggregationColumnReferenceExpr struct {
	expression Expr
	alias      string
}

var _ Expr = (*AggregationColumnReferenceExpr)(nil)

func AggregationColumnReference(expression Expr, alias string) *AggregationColumnReferenceExpr {
	return &AggregationColumnReferenceExpr{expression: expression, alias: alias}
}

func (e *AggregationColumnReferenceExpr) ToSql(dialect Dialect) (string, error) {
	refExpr := dialect.AggregationColumnReference(e.expression, e.alias)

	return refExpr.ToSql(dialect)
}

func ToExprSlice[T Expr](expr []T) []Expr {
	ret := make([]Expr, len(expr))
	for i, t := range expr {
		ret[i] = t
	}
	return ret
}

func ToExpr[T Expr](expr T) Expr {
	return expr
}
