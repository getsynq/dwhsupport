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
	Filter       string
	Segmentation *Segmentation
}

type Segmentation struct {
	Field string
	Rule  SegmentationRule
}

type SegmentationRule interface {
	isSegmentationRule()
}

type SegmentationRuleAll struct{}

func (s *SegmentationRuleAll) isSegmentationRule() {}

type SegmentationRuleAcceptList struct {
	Values []string
}

func (s *SegmentationRuleAcceptList) isSegmentationRule() {}

type SegmentationRuleExcludeList struct {
	Values []string
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

	if args.Filter != "" {
		qb = qb.WithFilter(Sql(args.Filter))
	}

	if args.Segmentation != nil && args.Segmentation.Field != "" {
		switch t := args.Segmentation.Rule.(type) {
		case *SegmentationRuleAcceptList:
			qb = qb.WithSegmentValues(t.Values, false)
		case *SegmentationRuleExcludeList:
			qb = qb.WithSegmentValues(t.Values, true)
		case *SegmentationRuleAll:
			// No filtration
		}

		qb = qb.WithSegment(Sql(args.Segmentation.Field)).OrderBy(Asc(Identifier("segment")))
	}
	return qb
}

var TableVolumeMetricsCols = []Expr{
	TableMetric(METRIC_NUM_ROWS),
}

func TableLastLoadedAtMetricsCols(timeCol TimeExpr) []Expr {
	return []Expr{
		TimeMetric(timeCol, METRIC_LAST_LOADED_AT),
	}
}

// Expr

type TableMetricExpr struct {
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
		return As(dialect.Count(Star()), Identifier(string(METRIC_NUM_ROWS))).ToSql(dialect)
	default:
		return "", fmt.Errorf("unknown TABLE metric type: %s", m.MetricId)
	}
}

//
// Numeric Metric
//

// Groupings

var NumericMetrics = []MetricId{
	METRIC_NUM_ROWS,
	METRIC_NUM_NOT_NULL,
	METRIC_NUM_UNIQUE,
	METRIC_NUM_EMPTY,
	METRIC_MEAN,
	METRIC_MIN,
	METRIC_MAX,
	METRIC_MEDIAN,
	METRIC_STDDEV,
}

func NumericMetricsCols(field string) []Expr {
	metricFieldCol := NumericCol(field)

	cols := []Expr{As(String(field), Identifier("field"))}
	for _, metricId := range NumericMetrics {
		cols = append(cols, NumericMetric(metricFieldCol, metricId))
	}

	return cols
}

// Expr

type NumericMetricExpr struct {
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
		return As(dialect.Count(Star()), Identifier(string(METRIC_NUM_ROWS))).ToSql(dialect)

	case METRIC_NUM_NOT_NULL:
		return As(dialect.Count(m.Column), Identifier(string(METRIC_NUM_NOT_NULL))).ToSql(dialect)

	case METRIC_NUM_UNIQUE:
		return As(dialect.Count(Distinct(m.Column)), Identifier(string(METRIC_NUM_UNIQUE))).ToSql(dialect)

	case METRIC_NUM_EMPTY:
		return As(dialect.CountIf(Eq(m.Column, Int64(0))), Identifier(string(METRIC_NUM_EMPTY))).ToSql(dialect)

	case METRIC_MEAN:
		return As(dialect.ToFloat64(Fn("avg", m.Column)), Identifier(string(METRIC_MEAN))).ToSql(dialect)

	case METRIC_MEDIAN:
		return As(dialect.ToFloat64(dialect.Median(m.Column)), Identifier(string(METRIC_MEDIAN))).ToSql(dialect)

	case METRIC_MIN:
		return As(dialect.ToFloat64(Fn("min", m.Column)), Identifier(string(METRIC_MIN))).ToSql(dialect)

	case METRIC_MAX:
		return As(dialect.ToFloat64(Fn("max", m.Column)), Identifier(string(METRIC_MAX))).ToSql(dialect)

	case METRIC_STDDEV:
		return As(dialect.ToFloat64(dialect.Stddev(m.Column)), Identifier(string(METRIC_STDDEV))).ToSql(dialect)

	default:
		return "", fmt.Errorf("unknown NUMERIC metric type for : %s", m.MetricId)
	}
}

//
// Time Metric
//

// Grouping

var TimeMetrics = []MetricId{
	METRIC_NUM_ROWS,
	METRIC_NUM_NOT_NULL,
	METRIC_MIN,
	METRIC_MAX,
}

func TimeMetricsCols(field string) []Expr {
	timeFieldCol := TimeCol(field)

	cols := []Expr{As(String(field), Identifier("field"))}
	for _, metricId := range TimeMetrics {
		cols = append(cols, TimeMetric(timeFieldCol, metricId))
	}

	return cols
}

// Expr

type TimeMetricExpr struct {
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
		return As(dialect.Count(Star()), Identifier(string(METRIC_NUM_ROWS))).ToSql(dialect)

	case METRIC_NUM_NOT_NULL:
		return As(dialect.Count(m.TimeExpr), Identifier(string(METRIC_NUM_NOT_NULL))).ToSql(dialect)

	case METRIC_MIN:
		return As(Fn("min", m.TimeExpr), Identifier(string(METRIC_MIN))).ToSql(dialect)

	case METRIC_MAX:
		return As(Fn("max", m.TimeExpr), Identifier(string(METRIC_MAX))).ToSql(dialect)

	case METRIC_FRESHNESS:
		return As(Fn("max", m.TimeExpr), Identifier(string(METRIC_FRESHNESS))).ToSql(dialect)

	case METRIC_LAST_LOADED_AT:
		return As(Fn("max", m.TimeExpr), Identifier(string(METRIC_LAST_LOADED_AT))).ToSql(dialect)

	default:
		return "", fmt.Errorf("unknown TIME metric type for : %s", m.MetricId)
	}
}

//
// Text Metric
//

var TextMetrics = []MetricId{
	METRIC_NUM_ROWS,
	METRIC_NUM_NOT_NULL,
	METRIC_NUM_UNIQUE,
	METRIC_NUM_EMPTY,
}

func TextMetricsCols(field string) []Expr {
	textFieldCol := TextCol(field)

	cols := []Expr{As(String(field), Identifier("field"))}
	for _, metricId := range TextMetrics {
		cols = append(cols, TextMetric(textFieldCol, metricId))
	}

	return cols
}

type TextMetricExpr struct {
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
		return As(dialect.Count(Star()), Identifier(string(METRIC_NUM_ROWS))).ToSql(dialect)

	case METRIC_NUM_NOT_NULL:
		return As(dialect.Count(m.Column), Identifier(string(METRIC_NUM_NOT_NULL))).ToSql(dialect)

	case METRIC_NUM_UNIQUE:
		return As(dialect.Count(Distinct(m.Column)), Identifier(string(METRIC_NUM_UNIQUE))).ToSql(dialect)

	case METRIC_NUM_EMPTY:
		return As(dialect.CountIf(Eq(m.Column, String(""))), Identifier(string(METRIC_NUM_EMPTY))).ToSql(dialect)

	default:
		return "", fmt.Errorf("unknown TEXT metric type for : %s", m.MetricId)
	}
}
