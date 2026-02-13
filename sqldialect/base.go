package sqldialect

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

var emptyLinesRegex = regexp.MustCompile(`(?m)^\s*\n`)

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

type CteExpr interface {
	Expr
	IsCteExpr()
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

	if _, ok := dialect.(*OracleDialect); ok {
		return fmt.Sprintf("FETCH FIRST %s ROWS ONLY", rowsSql), nil
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
	ctes    []*Cte
	cols    []Expr
	table   TableExpr
	joins   []*JoinExpr
	where   []CondExpr
	groupBy []Expr
	orderBy []*OrderExpr
	having  []CondExpr
	limit   *LimitExpr
}

func NewSelect() *Select {
	return &Select{}
}

func (s *Select) IsCteExpr() {}

func (s *Select) Cte(alias *CteAliasExpr, sql CteExpr) *Select {
	s.ctes = append(
		s.ctes, &Cte{
			Alias:     alias,
			Select:    sql,
			Recursive: []string{},
		},
	)

	return s
}

func (s *Select) RecursiveCte(alias *CteAliasExpr, sql *Select, recursive ...string) *Select {
	s.ctes = append(
		s.ctes, &Cte{
			Alias:     alias,
			Select:    sql,
			Recursive: recursive,
		},
	)

	return s
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

func (s *Select) Having(having ...CondExpr) *Select {
	s.having = append(s.having, having...)
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

	havingSql, err := exprsToSql(s.having, dialect)
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

	// build cte sql
	cteSqls := []string{}
	for _, cte := range s.ctes {
		expr, err := cte.ToSql(dialect)
		if err != nil {
			return "", err
		}
		cteSqls = append(cteSqls, expr)
	}

	selectSql := fmt.Sprintf(
		`%s
%s
from %s %s
%s
%s
%s
%s %s`,
		buildListSegment("with", ",\n", cteSqls),
		buildListSegment("select", ", ", colsSql),
		tableSql,
		strings.Join(joinsSql, " "),
		buildListSegment("where", " and ", whereSql),
		buildListSegment("group by", ", ", groupBySql),
		buildListSegment("having", " and ", havingSql),
		buildListSegment("order by", ", ", orderBySql),
		limitSql,
	)

	selectSql = emptyLinesRegex.ReplaceAllString(selectSql, "")

	return selectSql, nil
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

// CteExpr
type Cte struct {
	Alias     *CteAliasExpr
	Select    CteExpr
	Recursive []string
}

func (s *Cte) ToSql(dialect Dialect) (string, error) {
	sql, err := s.Select.ToSql(dialect)
	if err != nil {
		return "", err
	}

	recursive := ""
	recursiveParams := ""
	if len(s.Recursive) > 0 {
		recursive = "RECURSIVE "
		recursiveParams = "(" + strings.Join(s.Recursive, ", ") + ")"
	}

	aliasSql, err := s.Alias.ToSql(dialect)
	if err != nil {
		return "", err
	}

	sql = fmt.Sprintf("%s%s%s AS (%s)", recursive, aliasSql, recursiveParams, sql)
	return strings.TrimSpace(sql), nil
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

type NullExpr struct {
}

var _ Expr = (*NullExpr)(nil)

func Null() *NullExpr {
	return &NullExpr{}
}

func (e *NullExpr) ToSql(dialect Dialect) (string, error) {
	return "null", nil
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
func (s *SqlExpr) IsCteExpr()     {}

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

func (t *TableFqnExpr) ProjectId() string {
	return t.projectId
}

func (t *TableFqnExpr) DatasetId() string {
	return t.datasetId
}

func (t *TableFqnExpr) TableId() string {
	return t.tableId
}

func (t *TableFqnExpr) IsTableExpr() {}

var _ TableExpr = (*CteAliasExpr)(nil)

type TableFnExpr struct {
	name string
	ops  []Expr
}

func (t *TableFnExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.ResolveTableFunction(t)
}

func (t *TableFnExpr) IsTableExpr() {}

var _ TableExpr = (*TableFnExpr)(nil)

func TableFn(name string, ops ...Expr) *TableFnExpr {
	return &TableFnExpr{
		name: name,
		ops:  ops,
	}
}

type CteAliasExpr struct {
	alias string
}

func CteFqn(alias string) *CteAliasExpr {
	return &CteAliasExpr{alias: alias}
}

func (t *CteAliasExpr) ToSql(dialect Dialect) (string, error) {
	return t.alias, nil
}

func (t *CteAliasExpr) IsTableExpr() {}

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
	return dialect.StringLiteral(e.val), nil
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
	exprs []Expr
}

func Distinct(exprs ...Expr) *DistinctExpr {
	return &DistinctExpr{exprs: exprs}
}

func (e *DistinctExpr) ToSql(dialect Dialect) (string, error) {
	exprs, err := exprsToSql(e.exprs, dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("distinct %s", strings.Join(exprs, ", ")), nil
}

//
// TrimExpr
//

// var _ Expr = (*TrimExpr)(nil)

type TrimExpr struct {
	expr Expr
}

func Trim(expr Expr) *TrimExpr {
	return &TrimExpr{expr: expr}
}

func (e *TrimExpr) ToSql(dialect Dialect) (string, error) {
	exprSql, err := e.expr.ToSql(dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("trim(%s)", exprSql), nil
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
	var ops []string
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
// FnCondExpr
//

type FnCondExpr struct {
	name string
	ops  []Expr
}

var _ CondExpr = (*FnCondExpr)(nil)

func (e *FnCondExpr) IsCondExpr() {}

func FnCond(name string, ops ...Expr) *FnCondExpr {
	return &FnCondExpr{name: name, ops: ops}
}

func (e *FnCondExpr) ToSql(dialect Dialect) (string, error) {
	var ops []string
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
	var anySql []interface{}
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

// Add after other CondExpr types

type AndGroupsExpr struct {
	conds []CondExpr
}

func AndGroups(conds ...CondExpr) *AndGroupsExpr {
	return &AndGroupsExpr{conds: conds}
}

func (e *AndGroupsExpr) ToSql(dialect Dialect) (string, error) {
	if len(e.conds) == 0 {
		return "", nil
	}
	sqls, err := exprsToSql(e.conds, dialect)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("(%s)", strings.Join(sqls, ") and (")), nil
}

func (e *AndGroupsExpr) IsCondExpr() {}
