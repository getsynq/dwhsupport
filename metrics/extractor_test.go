package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type MetricExtractorSuite struct {
	suite.Suite
}

func TestMetricExtractorSuite(t *testing.T) {
	suite.Run(t, new(MetricExtractorSuite))
}

func (s *MetricExtractorSuite) TestExtractGrowthsFromMetricSeries() {
	s.Run("extract_growths_from_metric_series", func() {
		// Given
		valSeries := []*MetricVal[int64]{}
		loadSeries := []*MetricVal[time.Time]{}
		t0 := time.Unix(1, 0).UTC()
		t1 := t0.Add(30 * time.Minute)
		t2 := t1.Add(30 * time.Minute)
		t3 := t2.Add(30 * time.Minute)
		t4 := t3.Add(30 * time.Minute)
		t5 := t4.Add(30 * time.Minute)

		valSeries, loadSeries = createMetrics(2, t0, t0, valSeries, loadSeries)
		valSeries, loadSeries = createMetrics(0, t0, t1, valSeries, loadSeries) // not relevant
		valSeries, loadSeries = createMetrics(0, t0, t2, valSeries, loadSeries) // not relevant
		valSeries, loadSeries = createMetrics(4, t3, t3, valSeries, loadSeries)
		valSeries, loadSeries = createMetrics(7, t4, t4, valSeries, loadSeries)
		valSeries, loadSeries = createMetrics(0, t5, t5, valSeries, loadSeries)

		growths := ExtractGrowthsFromMetricsSeries(valSeries, loadSeries)

		s.Len(growths, 3)
		s.Equal(int64(2), growths[0].Value)
		s.Equal(int64(3), growths[1].Value)
		s.Equal(int64(-7), growths[2].Value)

		// check the growths metrics are copies
		s.Equal(int64(4), valSeries[3].Value)
		s.Equal(int64(7), valSeries[4].Value)
		s.Equal(int64(0), valSeries[5].Value)
	})

	s.Run("return_empty_metrics", func() {
		growths := ExtractGrowthsFromMetricsSeries([]*MetricVal[int64]{}, []*MetricVal[time.Time]{})

		s.Len(growths, 0)

		growths = ExtractGrowthsFromMetricsSeries([]*MetricVal[int64]{
			{Value: 2, MetricId: METRIC_VOLUME},
		}, []*MetricVal[time.Time]{})

		t0 := time.Unix(1, 0).UTC()
		t1 := t0.Add(30 * time.Minute)
		growths = ExtractGrowthsFromMetricsSeries([]*MetricVal[int64]{
			{Value: 2, At: t0},
			{Value: 2, At: t1},
		}, []*MetricVal[time.Time]{
			{Value: t0, At: t0},
			{Value: t0, At: t1},
		})

		s.Len(growths, 0)
	})
}

func createMetrics(
	val int64,
	lastLoaded time.Time,
	at time.Time,
	valSeries []*MetricVal[int64],
	loadSeries []*MetricVal[time.Time],
) ([]*MetricVal[int64], []*MetricVal[time.Time]) {
	valSeries = append(valSeries, &MetricVal[int64]{Value: val, At: at})
	loadSeries = append(loadSeries, &MetricVal[time.Time]{Value: lastLoaded, At: at})

	return valSeries, loadSeries
}
