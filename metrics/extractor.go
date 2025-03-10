package metrics

import (
	"slices"
	"time"

	"github.com/pkg/errors"
	"github.com/samber/lo"
)

func IsTimeType[T any]() bool {
	var value T
	var valueI interface{} = value
	switch valueI.(type) {
	case time.Time:
		return true
	}
	return false
}

func GetMetricValueFromNumber[T MetricType, V int64 | float64](
	metricId MetricId,
	value V,
) (T, error) {
	var expectedTimeType = IsTimeType[T]()

	switch metricId {
	case METRIC_FRESHNESS:
		val := time.Unix(int64(value), 0).UTC()
		if expectedTimeType {
			return ConvertToMetricType[T](val)
		} else {
			return ConvertToMetricType[T](val.Unix())
		}
	case METRIC_LAST_LOADED_AT,
		METRIC_VOLUME,
		METRIC_NUM_ROWS,
		METRIC_NUM_NULL,
		METRIC_NUM_NOT_NULL,
		METRIC_NUM_UNIQUE,
		METRIC_NUM_EMPTY,
		METRIC_ROW_GROWTH,
		METRIC_DELAY,
		METRIC_VOLUME_CHANGE_DELAY,
		METRIC_SIZE_BYTES:
		val := int64(value)
		return ConvertToMetricType[T](val)
	case METRIC_CUSTOM_NUMERIC,
		METRIC_MEAN,
		METRIC_MIN,
		METRIC_MAX,
		METRIC_MEDIAN,
		METRIC_STDDEV,
		METRIC_PCT_NULL,
		METRIC_PCT_UNIQUE,
		METRIC_PCT_EMPTY:
		val := float64(value)
		return ConvertToMetricType[T](val)
	}

	var zeroRes T
	return zeroRes, errors.Errorf("metric %s not supported for %v", metricId, value)
}

func ConvertToMetricType[T MetricType](val interface{}) (T, error) {
	resT, ok := val.(T)
	if !ok {
		return resT, errors.Errorf("cannot convert %v (%T) into MetricType %T", val, val, resT)
	}

	return resT, nil
}

func ExtractGrowthsFromMetricsSeries[T int64 | float64](
	volumeSeries []*MetricVal[T],
	loadSeries []*MetricVal[time.Time],
) []*MetricVal[T] {
	if len(volumeSeries) < 2 || len(loadSeries) < 2 {
		return []*MetricVal[T]{}
	}

	dedupLoadData := []*MetricVal[time.Time]{}
	for _, timeMetric := range loadSeries {
		if len(dedupLoadData) == 0 {
			dedupLoadData = append(dedupLoadData, timeMetric)
			continue
		}

		if timeMetric.Value == dedupLoadData[len(dedupLoadData)-1].Value {
			continue
		}

		dedupLoadData = append(dedupLoadData, timeMetric)
	}

	changingTimestamps := lo.Map(
		dedupLoadData,
		func(i *MetricVal[time.Time], _ int) time.Time { return i.At },
	)

	data := lo.Filter(
		volumeSeries,
		func(i *MetricVal[T], _ int) bool { return slices.Contains(changingTimestamps, i.At) },
	)

	if len(data) < 2 {
		return []*MetricVal[T]{}
	}

	growths := make([]*MetricVal[T], len(data)-1)
	for i := 1; i < len(data); i++ {
		val := *data[i]
		val.MetricId = METRIC_ROW_GROWTH
		val.Value = data[i].Value - data[i-1].Value
		growths[i-1] = &val
	}

	return growths
}
