package metrics

import (
	"time"
)

type MetricType interface {
	int64 | float64 | time.Time
}

type MetricVal[T MetricType] struct {
	MonitorPath          string
	MonitoredAssetPath   string
	Segment              string
	Field                string
	MetricId             MetricId
	At                   time.Time
	IngestedAt           time.Time
	Value                T
	MetricsVersion       int32
	IsAnomaly            bool
	IsCorrection         bool
	HasPredictionItem    bool
	IsShiftedTimeSegment bool
}

func (m *MetricVal[T]) ToFloat() float64 {
	switch t := any(m).(type) {
	case *MetricVal[float64]:
		return t.Value

	case *MetricVal[time.Time]:
		return float64(t.At.Sub(t.Value).Seconds())

	case *MetricVal[int64]:
		return float64(t.Value)
	}

	return 0
}
