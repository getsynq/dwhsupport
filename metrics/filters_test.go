package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type MetricFiltersSuite struct {
	suite.Suite
}

func TestMetricFiltersSuite(t *testing.T) {
	suite.Run(t, new(MetricFiltersSuite))
}

func (s *MetricFiltersSuite) TestMetricValsFilters() {

	monitorPath := "ad-integrationId::tbl1::row_count"

	t0 := time.Unix(0, 0).Add(time.Second).UTC()
	t1 := t0.Add(30 * time.Minute)
	t2 := t1.Add(30 * time.Minute)
	t3 := t2.Add(30 * time.Minute)

	series := []*MetricVal[int64]{
		{MonitorPath: monitorPath, Value: 1, At: t0, IngestedAt: t2},
		{MonitorPath: monitorPath, Value: 2, At: t1, IngestedAt: t2},
		{MonitorPath: monitorPath, Value: 3, At: t2, IngestedAt: t3},
		{MonitorPath: monitorPath, Value: 4, At: t3, IngestedAt: t3},
	}

	require.Len(s.T(), series, 4)

	s.Run("FilterMetricVals", func() {
		res := FilterMetricVals(series, monitorPath, "", t1, t2)
		require.Len(s.T(), res, 2)

		res = FilterMetricVals(series, monitorPath, "", t0, t3)
		require.Len(s.T(), res, 4)
	})

	s.Run("GetMetricValsBetween", func() {
		res := GetMetricValsBetween(series, t1, t2)
		s.Len(res, 2)

		res = GetMetricValsBetween(series, t0, t3)
		s.Len(res, 4)
	})

	s.Run("GetMetricValsIngestedFrom", func() {
		res := GetMetricValsIngestedFrom(series, t0)
		s.Len(res, 4)

		res = GetMetricValsIngestedFrom(series, t3)
		s.Len(res, 2)
	})

	s.Run("HasMetricValWithoutPrediction", func() {
		require.True(s.T(), HasMetricValWithoutPrediction(series))

		staleSeries := []*MetricVal[int64]{
			{MonitorPath: monitorPath, Value: 1, At: t0, HasPredictionItem: true},
			{MonitorPath: monitorPath, Value: 2, At: t1, HasPredictionItem: true},
		}

		require.False(s.T(), HasMetricValWithoutPrediction(staleSeries))
	})
}
