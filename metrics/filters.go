package metrics

import (
	"time"

	"github.com/samber/lo"
)

func FilterMetricVals[T MetricType](metricVals []*MetricVal[T], path, segment string, from, to time.Time) []*MetricVal[T] {
	return lo.Filter(metricVals, func(m *MetricVal[T], _ int) bool {
		return m.MonitorPath == path &&
			!m.At.Before(from) && !m.At.After(to)
	})
}

func GetMetricValsBetween[T MetricType](metricVals []*MetricVal[T], from, to time.Time) []*MetricVal[T] {
	return lo.Filter(metricVals, func(m *MetricVal[T], _ int) bool {
		return !m.At.Before(from) && !m.At.After(to)
	})
}

func GetMetricValsIngestedFrom[T MetricType](metricVals []*MetricVal[T], from time.Time) []*MetricVal[T] {
	return lo.Filter(metricVals, func(m *MetricVal[T], _ int) bool {
		return !m.IngestedAt.Before(from)
	})
}

func HasMetricValWithoutPrediction[T MetricType](metricVals []*MetricVal[T]) bool {
	_, ok := lo.Find(metricVals, func(m *MetricVal[T]) bool {
		return m.HasPredictionItem == false
	})

	return ok
}
