package metrics

import (
	"fmt"
	"time"
)

type StaleMetricsError struct {
	At time.Time
}

func (e *StaleMetricsError) Error() string {
	return fmt.Sprintf("Latest metric value is stale for %s", e.At)
}

func NewStaleMetricsError(at time.Time) *StaleMetricsError {
	return &StaleMetricsError{At: at}
}
