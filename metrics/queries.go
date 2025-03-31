package metrics

import (
	"fmt"
	"time"

	"github.com/getsynq/dwhsupport/querybuilder"
	. "github.com/getsynq/dwhsupport/sqldialect"
)

type MonitorPartitioning struct {
	Field             string
	Interval          time.Duration
	ScheduleTimeShift time.Duration
}

type MonitorArgs struct {
	Conditions   []CondExpr
	Segmentation []*Segmentation
}

type Segmentation struct {
	Field string // FIXME: make it expression
	Rule  SegmentationRule
}

type SegmentationRule interface {
	isSegmentationRule()
}

type SegmentationRuleAll struct{}

func (s *SegmentationRuleAll) isSegmentationRule() {}

func AllSegments() *SegmentationRuleAll {
	return &SegmentationRuleAll{}
}

type SegmentationRuleAcceptList struct {
	Values []string
}

func AcceptSegments(values ...string) *SegmentationRuleAcceptList {
	return &SegmentationRuleAcceptList{
		Values: values,
	}
}

func (s *SegmentationRuleAcceptList) isSegmentationRule() {}

type SegmentationRuleExcludeList struct {
	Values []string
}

func ExcludeSegments(values ...string) *SegmentationRuleExcludeList {
	return &SegmentationRuleExcludeList{
		Values: values,
	}
}

func (s *SegmentationRuleExcludeList) isSegmentationRule() {}

func ApplyMonitorDefArgs(
	qb *querybuilder.QueryBuilder,
	args *MonitorArgs,
	partitioning *MonitorPartitioning,
) *querybuilder.QueryBuilder {
	if partitioning != nil {
		if partitioning.ScheduleTimeShift == 0 {
			qb = qb.WithTimeSegment(TimeCol(partitioning.Field), partitioning.Interval)
		} else {
			qb = qb.WithShiftedTimeSegment(TimeCol(partitioning.Field), partitioning.Interval, partitioning.ScheduleTimeShift)
		}
	}

	if args == nil {
		return qb
	}

	for _, condition := range args.Conditions {
		qb = qb.WithFilter(condition)
	}

	for _, segmentation := range args.Segmentation {
		if segmentation.Field != "" {

			var useValues = false
			var values []string
			var isExcluding bool

			switch t := segmentation.Rule.(type) {
			case *SegmentationRuleAcceptList:
				useValues = true
				values = t.Values
			case *SegmentationRuleExcludeList:
				useValues = true
				isExcluding = true
				values = t.Values
			case *SegmentationRuleAll:
				// No filtration
			}

			if useValues {
				qb = qb.WithSegmentFiltered(
					Sql(segmentation.Field),
					values,
					isExcluding)
			} else {
				qb = qb.WithSegment(Sql(segmentation.Field))
			}
		}
	}
	return qb
}

func TableVolumeMetricsCols() []Expr {
	return []Expr{
		TableMetric(METRIC_NUM_ROWS),
	}
}

func TableLastLoadedAtMetricsCols(timeCol TimeExpr) []Expr {
	return []Expr{
		TimeMetric(timeCol, METRIC_LAST_LOADED_AT),
	}
}

type MetricConf struct {
	AliasPrefix string
}

func (c *MetricConf) PrefixedAliasForMetric(metricId MetricId) TextExpr {
	if c.AliasPrefix == "" {
		return Identifier(string(metricId))
	}
	return Identifier(fmt.Sprintf("%s$%s", c.AliasPrefix, string(metricId)))
}

type MetricConfOption func(*MetricConf)

func DefaultMetricConf() *MetricConf {
	return &MetricConf{}
}

func WithPrefixForColumn(prefix string) MetricConfOption {
	return func(conf *MetricConf) {
		conf.AliasPrefix = prefix
	}
}

// Expr

type TableMetricExpr struct {
	MetricConf
	MetricId MetricId
}

func TableMetric(metricId MetricId) *TableMetricExpr {
	return &TableMetricExpr{
		MetricId: metricId,
	}
}

func (m *TableMetricExpr) ToSql(dialect Dialect) (string, error) {
	switch m.MetricId {
	case METRIC_NUM_ROWS:
		return As(dialect.Count(Star()), m.OutColumnAlias()).ToSql(dialect)
	default:
		return "", fmt.Errorf("unknown TABLE metric type: %s", m.MetricId)
	}
}

func (m *TableMetricExpr) OutColumnAlias() TextExpr {
	return m.PrefixedAliasForMetric(m.MetricId)
}

//
// Numeric Metric
//

// Groupings

var NumericMetrics = []MetricId{
	METRIC_NUM_NOT_NULL,
	METRIC_NUM_UNIQUE,
	METRIC_NUM_EMPTY,
	METRIC_MEAN,
	METRIC_MIN,
	METRIC_MAX,
	METRIC_MEDIAN,
	METRIC_STDDEV,
}

func NumericMetricsValuesCols(field string, opts ...MetricConfOption) []Expr {
	metricFieldCol := NumericCol(field)

	var cols []Expr
	for _, metricId := range NumericMetrics {
		metricExpr := NumericMetric(metricFieldCol, metricId)
		for _, opt := range opts {
			opt(&metricExpr.MetricConf)
		}
		cols = append(cols, metricExpr)
	}

	return cols
}

func NumericMetricsCols(field string, opts ...MetricConfOption) []Expr {
	cols := []Expr{As(String(field), Identifier("field"))}
	cols = append(cols, CountStar(METRIC_NUM_ROWS))
	cols = append(cols, NumericMetricsValuesCols(field, opts...)...)

	return cols
}

// Expr

type NumericMetricExpr struct {
	MetricConf
	MetricId MetricId
	Column   NumericExpr
}

func NumericMetric(col NumericExpr, metricId MetricId) *NumericMetricExpr {
	return &NumericMetricExpr{
		MetricId: metricId,
		Column:   col,
	}
}

func (m *NumericMetricExpr) ToSql(dialect Dialect) (string, error) {
	switch m.MetricId {
	case METRIC_NUM_ROWS:
		return As(dialect.Count(Star()), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_NUM_NOT_NULL:
		return As(dialect.Count(m.Column), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_NUM_UNIQUE:
		return As(dialect.Count(Distinct(m.Column)), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_NUM_EMPTY:
		return As(dialect.CountIf(Eq(m.Column, Int64(0))), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_MEAN:
		return As(dialect.ToFloat64(Fn("avg", m.Column)), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_MEDIAN:
		return As(dialect.ToFloat64(dialect.Median(m.Column)), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_MIN:
		return As(dialect.ToFloat64(Fn("min", m.Column)), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_MAX:
		return As(dialect.ToFloat64(Fn("max", m.Column)), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_STDDEV:
		return As(dialect.ToFloat64(dialect.Stddev(m.Column)), m.OutColumnAlias()).ToSql(dialect)

	default:
		return "", fmt.Errorf("unknown NUMERIC metric type for : %s", m.MetricId)
	}
}

func (m *NumericMetricExpr) OutColumnAlias() TextExpr {
	return m.PrefixedAliasForMetric(m.MetricId)
}

//
// Time Metric
//

// Grouping

var TimeMetrics = []MetricId{
	METRIC_NUM_NOT_NULL,
	METRIC_MIN,
	METRIC_MAX,
}

func TimeMetricsValuesCols(field string, opts ...MetricConfOption) []Expr {
	timeFieldCol := TimeCol(field)

	var cols []Expr
	for _, metricId := range TimeMetrics {
		metricExpr := TimeMetric(timeFieldCol, metricId)
		for _, opt := range opts {
			opt(&metricExpr.MetricConf)
		}
		cols = append(cols, metricExpr)
	}

	return cols
}

func TimeMetricsCols(field string, opts ...MetricConfOption) []Expr {
	cols := []Expr{As(String(field), Identifier("field"))}
	cols = append(cols, CountStar(METRIC_NUM_ROWS))
	cols = append(cols, TimeMetricsValuesCols(field, opts...)...)

	return cols
}

// Expr

type TimeMetricExpr struct {
	MetricConf
	MetricId MetricId
	TimeExpr TimeExpr
}

func TimeMetric(col TimeExpr, metricId MetricId) *TimeMetricExpr {
	return &TimeMetricExpr{
		MetricId: metricId,
		TimeExpr: col,
	}
}

func (m *TimeMetricExpr) ToSql(dialect Dialect) (string, error) {
	switch m.MetricId {
	case METRIC_NUM_ROWS:
		return As(dialect.Count(Star()), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_NUM_NOT_NULL:
		return As(dialect.Count(m.TimeExpr), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_MIN:
		return As(Fn("min", m.TimeExpr), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_MAX:
		return As(Fn("max", m.TimeExpr), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_FRESHNESS:
		return As(Fn("max", m.TimeExpr), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_LAST_LOADED_AT:
		return As(Fn("max", m.TimeExpr), m.OutColumnAlias()).ToSql(dialect)

	default:
		return "", fmt.Errorf("unknown TIME metric type for : %s", m.MetricId)
	}
}

func (m *TimeMetricExpr) OutColumnAlias() TextExpr {
	return m.PrefixedAliasForMetric(m.MetricId)
}

//
// Text Metric
//

var TextMetrics = []MetricId{
	METRIC_NUM_NOT_NULL,
	METRIC_NUM_UNIQUE,
	METRIC_NUM_EMPTY,
}

var TextLengthMetrics = []MetricId{
	METRIC_MIN_LENGTH,
	METRIC_MAX_LENGTH,
	METRIC_MEAN_LENGTH,
}

func TextMetricsLengthCols(field string, opts ...MetricConfOption) []Expr {
	textFieldCol := TextCol(field)

	var cols []Expr
	for _, metricId := range TextLengthMetrics {
		metricExpr := TextMetric(textFieldCol, metricId)
		for _, opt := range opts {
			opt(&metricExpr.MetricConf)
		}
		cols = append(cols, metricExpr)

	}

	return cols
}

func TextMetricsValuesCols(field string, opts ...MetricConfOption) []Expr {
	textFieldCol := TextCol(field)

	var cols []Expr
	for _, metricId := range TextMetrics {
		metricExpr := TextMetric(textFieldCol, metricId)
		for _, opt := range opts {
			opt(&metricExpr.MetricConf)
		}
		cols = append(cols, metricExpr)

	}

	return cols
}

func TextMetricsCols(field string, opts ...MetricConfOption) []Expr {
	cols := []Expr{As(String(field), Identifier("field"))}
	cols = append(cols, CountStar(METRIC_NUM_ROWS))
	cols = append(cols, TextMetricsValuesCols(field, opts...)...)

	return cols
}

type TextMetricExpr struct {
	MetricConf
	MetricId MetricId
	Column   TextExpr
}

func TextMetric(col TextExpr, metricId MetricId) *TextMetricExpr {
	return &TextMetricExpr{
		MetricId: metricId,
		Column:   col,
	}
}

func (m *TextMetricExpr) As(alias string) *AsExpr {
	return As(m, Identifier(alias))
}

func (m *TextMetricExpr) ToSql(dialect Dialect) (string, error) {
	switch m.MetricId {
	case METRIC_NUM_ROWS:
		return As(dialect.Count(Star()), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_NUM_NOT_NULL:
		return As(dialect.Count(m.Column), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_NUM_UNIQUE:
		return As(dialect.Count(Distinct(m.Column)), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_NUM_EMPTY:
		return As(dialect.CountIf(Eq(m.Column, String(""))), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_MEAN_LENGTH:
		return As(dialect.ToFloat64(Fn("avg", Fn("length", m.Column))), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_MIN_LENGTH:
		return As(dialect.ToFloat64(Fn("min", Fn("length", m.Column))), m.OutColumnAlias()).ToSql(dialect)

	case METRIC_MAX_LENGTH:
		return As(dialect.ToFloat64(Fn("max", Fn("length", m.Column))), m.OutColumnAlias()).ToSql(dialect)

	default:
		return "", fmt.Errorf("unknown TEXT metric type for : %s", m.MetricId)
	}
}

func (m *TextMetricExpr) OutColumnAlias() TextExpr {
	return m.PrefixedAliasForMetric(m.MetricId)
}

type CustomNumericMetricExpr struct {
	MetricConf
	MetricId MetricId
	Sql      NumericExpr
}

func (m *CustomNumericMetricExpr) ToSql(dialect Dialect) (string, error) {
	return As(ToFloat64(m.Sql), m.OutColumnAlias()).ToSql(dialect)
}

func (m *CustomNumericMetricExpr) OutColumnAlias() TextExpr {
	return m.PrefixedAliasForMetric(m.MetricId)
}

func CustomNumericMetric(sql NumericExpr, metricId MetricId) *CustomNumericMetricExpr {
	return &CustomNumericMetricExpr{
		MetricId: metricId,
		Sql:      sql,
	}
}

func CustomNumericMetricsCols(sql NumericExpr, metricId MetricId) []Expr {
	return []Expr{CustomNumericMetric(sql, metricId)}
}

type CountExpr struct {
	MetricConf
	Expresion Expr
	MetricId  MetricId
}

func (m CountExpr) ToSql(dialect Dialect) (string, error) {
	return As(dialect.Count(m.Expresion), m.OutColumnAlias()).ToSql(dialect)
}

func (m CountExpr) IsNumericExpr() {
}

func (m CountExpr) OutColumnAlias() TextExpr {
	return m.PrefixedAliasForMetric(m.MetricId)
}

func CountStar(metricId MetricId) NumericExpr {
	return &CountExpr{
		Expresion: Star(),
		MetricId:  metricId,
	}
}
