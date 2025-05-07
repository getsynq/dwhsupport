package metrics

import (
	"slices"

	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/samber/lo"
)

type FieldMetrics struct {
	Field   string
	Metrics []MetricId
}

var TimePredictionMetricsGroup = []MetricId{
	METRIC_NUM_ROWS,
	METRIC_NUM_NULL,
	METRIC_NUM_UNIQUE,
	METRIC_MIN,
	METRIC_MAX,
	METRIC_PCT_NULL,
	METRIC_PCT_UNIQUE,
}

var NumericPredictionMetricsGroup = []MetricId{
	METRIC_NUM_ROWS,
	METRIC_NUM_NOT_NULL,
	METRIC_NUM_UNIQUE,
	METRIC_NUM_EMPTY,
	METRIC_MEAN,
	METRIC_MIN,
	METRIC_MAX,
	METRIC_MEDIAN,
	METRIC_STDDEV,

	METRIC_NUM_NULL,
	METRIC_PCT_NULL,
	METRIC_PCT_UNIQUE,
	METRIC_PCT_EMPTY,
}

var TextPredictionMetricsGroup = []MetricId{
	METRIC_NUM_ROWS,
	METRIC_NUM_UNIQUE,
	METRIC_NUM_EMPTY,
	METRIC_NUM_NULL,
	METRIC_PCT_NULL,
	METRIC_PCT_UNIQUE,
	METRIC_PCT_EMPTY,
}

func GetNumericPredictionMetricsGroup(dialect sqldialect.Dialect) []MetricId {
	if _, ok := (dialect).(*sqldialect.RedshiftDialect); ok {
		metrics := lo.Filter(NumericPredictionMetricsGroup, func(metricId MetricId, _ int) bool {
			return !slices.Contains([]MetricId{METRIC_NUM_UNIQUE, METRIC_PCT_UNIQUE}, metricId)
		})
		return metrics
	}

	return NumericPredictionMetricsGroup
}
