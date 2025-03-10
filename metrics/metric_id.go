package metrics

import "github.com/pkg/errors"

type MetricId string

const (
	METRIC_FRESHNESS           MetricId = "updated_at"
	METRIC_VOLUME              MetricId = "row_count"
	METRIC_ROW_GROWTH          MetricId = "row_growth"
	METRIC_LAST_LOADED_AT      MetricId = "last_loaded_at"
	METRIC_NUM_ROWS            MetricId = "num_rows"
	METRIC_CUSTOM_NUMERIC      MetricId = "custom_numeric"
	METRIC_NUM_UNIQUE          MetricId = "num_unique"
	METRIC_NUM_NOT_NULL        MetricId = "num_not_null"
	METRIC_NUM_NULL            MetricId = "num_null"
	METRIC_NUM_EMPTY           MetricId = "num_empty"
	METRIC_MEAN                MetricId = "mean"
	METRIC_MIN                 MetricId = "min"
	METRIC_MAX                 MetricId = "max"
	METRIC_MEDIAN              MetricId = "median"
	METRIC_STDDEV              MetricId = "stddev"
	METRIC_DELAY               MetricId = "delay"
	METRIC_VOLUME_CHANGE_DELAY MetricId = "volume_change_delay"
	METRIC_SIZE_BYTES          MetricId = "size_bytes"
	METRIC_PCT_UNIQUE          MetricId = "pct_unique"
	METRIC_PCT_NULL            MetricId = "pct_null"
	METRIC_PCT_EMPTY           MetricId = "pct_empty"
)

// Implement the sql.Scanner interface
func (m *MetricId) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	*m = MetricId(b)
	return nil
}

func MetricIdDescription(id MetricId) string {
	switch id {
	case METRIC_FRESHNESS:
		return "Updated at"
	case METRIC_VOLUME:
		return "Row count"
	case METRIC_ROW_GROWTH:
		return "Row growth"
	case METRIC_LAST_LOADED_AT:
		return "Last loaded at"
	case METRIC_NUM_ROWS:
		return "Number of rows"
	case METRIC_CUSTOM_NUMERIC:
		return "Custom numeric value"
	case METRIC_NUM_UNIQUE:
		return "Number of unique"
	case METRIC_NUM_NOT_NULL:
		return "Number of not null"
	case METRIC_NUM_NULL:
		return "Number of null"
	case METRIC_NUM_EMPTY:
		return "Number of empty"
	case METRIC_MEAN:
		return "Mean"
	case METRIC_MIN:
		return "Min"
	case METRIC_MAX:
		return "Max"
	case METRIC_MEDIAN:
		return "Median"
	case METRIC_STDDEV:
		return "Standard deviation"
	case METRIC_DELAY:
		return "Delay"
	case METRIC_VOLUME_CHANGE_DELAY:
		return "Change Delay"
	case METRIC_SIZE_BYTES:
		return "Size Bytes"
	case METRIC_PCT_UNIQUE:
		return "Percentage of Unique"
	case METRIC_PCT_NULL:
		return "Percentage of Null"
	case METRIC_PCT_EMPTY:
		return "Percentage of Empty"
	default:
		return string(id)
	}
}
