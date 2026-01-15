package metrics

import (
	"time"

	"cloud.google.com/go/bigquery"
)

type MetricResponseI interface {
	GetIdentity() MetricIdentity
	//Initializes the metric to default values
	ToDefault(timeSegment time.Time, segment string)
	//Keeps metric value but sets segment and time segment
	WithPartition(timeSegment time.Time, segment string)
}

type MetricIdentity struct {
	Time    time.Time
	Segment string
}

func NewMetricIdentity(time time.Time, segment string) MetricIdentity {
	return MetricIdentity{time, segment}
}

func (identity MetricIdentity) ToKey() string {
	return identity.Segment + "_" + identity.Time.Format(time.RFC3339)
}

//
// CustomNumeric
//

type MetricCustomNumeric struct {
	Segment     string    `ch:"segment"        bigquery:"segment"        db:"segment"        json:"segment"`
	TimeSegment time.Time `ch:"time_segment"   bigquery:"time_segment"   db:"time_segment"   json:"time_segment"`
	Numeral     float64   `ch:"custom_numeric" bigquery:"custom_numeric" db:"custom_numeric" json:"custom_numeric"`
}

func (volume *MetricCustomNumeric) GetIdentity() MetricIdentity {
	return MetricIdentity{
		volume.TimeSegment,
		volume.Segment,
	}
}

func (volume *MetricCustomNumeric) ToDefault(timeSegment time.Time, segment string) {
	volume.Segment = segment
	volume.TimeSegment = timeSegment
	volume.Numeral = 0
}

func (volume *MetricCustomNumeric) WithPartition(timeSegment time.Time, segment string) {
	volume.Segment = segment
	volume.TimeSegment = timeSegment
}

//
// VOLUME
//

type MetricVolume struct {
	Segment     string    `ch:"segment"      bigquery:"segment"      db:"segment"      json:"segment"`
	TimeSegment time.Time `ch:"time_segment" bigquery:"time_segment" db:"time_segment" json:"time_segment"`
	NumRows     int64     `ch:"num_rows"     bigquery:"num_rows"     db:"num_rows"     json:"num_rows"`
}

func NewMetricVolume(numRows int64) *MetricVolume {
	return &MetricVolume{
		NumRows: numRows,
	}
}

func NewMetricVolumeWithSegments(numRows int64, timeSegment time.Time, segment string) *MetricVolume {
	return &MetricVolume{
		Segment:     segment,
		TimeSegment: timeSegment,
		NumRows:     numRows,
	}
}

func (volume *MetricVolume) GetIdentity() MetricIdentity {
	return MetricIdentity{
		volume.TimeSegment,
		volume.Segment,
	}
}

func (volume *MetricVolume) ToDefault(timeSegment time.Time, segment string) {
	volume.Segment = segment
	volume.TimeSegment = timeSegment
	volume.NumRows = 0
}

func (volume *MetricVolume) WithPartition(timeSegment time.Time, segment string) {
	volume.Segment = segment
	volume.TimeSegment = timeSegment
}

//
// FRESHNESS
//

type MetricFreshness struct {
	Segment     string    `ch:"segment"      bigquery:"segment"      db:"segment"      json:"segment"`
	TimeSegment time.Time `ch:"time_segment" bigquery:"time_segment" db:"time_segment" json:"time_segment"`
	At          time.Time `ch:"at"           bigquery:"at"           db:"at"           json:"at"`
}

func NewMetricFreshness(at time.Time) *MetricFreshness {
	return &MetricFreshness{
		At: at,
	}
}

func NewMetricFreshnessWithSegments(at time.Time, timeSegment time.Time, segment string) *MetricFreshness {
	return &MetricFreshness{
		Segment:     segment,
		TimeSegment: timeSegment,
		At:          at,
	}
}

func (metric *MetricFreshness) GetIdentity() MetricIdentity {
	return MetricIdentity{
		metric.TimeSegment,
		metric.Segment,
	}
}

func (metric *MetricFreshness) ToDefault(timeSegment time.Time, segment string) {
	metric.Segment = segment
	metric.TimeSegment = timeSegment
}

//
// LAST_LOADED_AT
//

type MetricLastLoadedAt struct {
	Segment      string    `ch:"segment"        bigquery:"segment"        db:"segment"        json:"segment"`
	TimeSegment  time.Time `ch:"time_segment"   bigquery:"time_segment"   db:"time_segment"   json:"time_segment"`
	LastLoadedAt time.Time `ch:"last_loaded_at" bigquery:"last_loaded_at" db:"last_loaded_at" json:"last_loaded_at"`
}

func NewMetricLastLoadedAt(lastLoadedAt time.Time) *MetricLastLoadedAt {
	return &MetricLastLoadedAt{
		LastLoadedAt: lastLoadedAt,
	}
}

func NewMetricLastLoadedAtWithSegments(updatedAt time.Time, timeSegment time.Time, segment string) *MetricLastLoadedAt {
	return &MetricLastLoadedAt{
		Segment:      segment,
		TimeSegment:  timeSegment,
		LastLoadedAt: updatedAt,
	}
}

func (metric *MetricLastLoadedAt) GetIdentity() MetricIdentity {
	return MetricIdentity{
		metric.TimeSegment,
		metric.Segment,
	}
}

func (metric *MetricLastLoadedAt) ToDefault(timeSegment time.Time, segment string) {
	metric.Segment = segment
	metric.TimeSegment = timeSegment
	metric.LastLoadedAt = time.Unix(0, 0).UTC()
}

func (metric *MetricLastLoadedAt) WithPartition(timeSegment time.Time, segment string) {
	metric.Segment = segment
	metric.TimeSegment = timeSegment
}

//
// TABLE STATS
//

type MetricTableStats struct {
	Segment      string     `ch:"segment"        bigquery:"segment"        db:"segment"        json:"segment"`
	TimeSegment  time.Time  `ch:"time_segment"   bigquery:"time_segment"   db:"time_segment"   json:"time_segment"`
	LastLoadedAt *time.Time `ch:"last_loaded_at" bigquery:"last_loaded_at" db:"last_loaded_at" json:"last_loaded_at"`
	NumRows      *int64     `ch:"num_rows"       bigquery:"num_rows"       db:"num_rows"       json:"num_rows"`
	SizeBytes    *int64     `ch:"size_bytes"     bigquery:"size_bytes"     db:"size_bytes"     json:"size_bytes"`
}

func NewMetricTableStats(lastLoadedAt *time.Time, numRows *int64, sizeBytes *int64) *MetricTableStats {
	return &MetricTableStats{
		LastLoadedAt: lastLoadedAt,
		NumRows:      numRows,
		SizeBytes:    sizeBytes,
	}
}

func NewMetricTableStatsWithSegment(lastLoadedAt *time.Time, numRows int64, timeSegment time.Time, segment string) *MetricTableStats {
	stats := NewMetricTableStats(lastLoadedAt, &numRows, nil)
	stats.Segment = segment
	stats.TimeSegment = timeSegment

	return stats
}

func (stats *MetricTableStats) GetIdentity() MetricIdentity {
	return MetricIdentity{
		stats.TimeSegment,
		stats.Segment,
	}
}

func (stats *MetricTableStats) WithPartition(timeSegment time.Time, segment string) {
	stats.Segment = segment
	stats.TimeSegment = timeSegment
}

func (stats *MetricTableStats) ToDefault(timeSegment time.Time, segment string) {
	stats.Segment = segment
	stats.TimeSegment = timeSegment
}

// TABLE STATS ALTERNATIVE
type MetricTableStatsAlt struct {
	Segment      string    `ch:"segment"        bigquery:"segment"        db:"segment"        json:"segment"`
	TimeSegment  time.Time `ch:"time_segment"   bigquery:"time_segment"   db:"time_segment"   json:"time_segment"`
	LastLoadedAt time.Time `ch:"last_loaded_at" bigquery:"last_loaded_at" db:"last_loaded_at" json:"last_loaded_at"`
	NumRows      int64     `ch:"num_rows"       bigquery:"num_rows"       db:"num_rows"       json:"num_rows"`
	SizeBytes    int64     `ch:"size_bytes"     bigquery:"size_bytes"     db:"size_bytes"     json:"size_bytes"`
}

func NewMetricTableStatsAlt(lastLoadedAt time.Time, numRows int64, sizeBytes int64) *MetricTableStatsAlt {
	return &MetricTableStatsAlt{
		LastLoadedAt: lastLoadedAt,
		NumRows:      numRows,
		SizeBytes:    sizeBytes,
	}
}

func NewMetricTableStatsWithSegmentAlt(lastLoadedAt time.Time, numRows int64, timeSegment time.Time, segment string) *MetricTableStatsAlt {
	stats := NewMetricTableStatsAlt(lastLoadedAt, numRows, 0)
	stats.Segment = segment
	stats.TimeSegment = timeSegment

	return stats
}

func (stats *MetricTableStatsAlt) GetIdentity() MetricIdentity {
	return MetricIdentity{
		stats.TimeSegment,
		stats.Segment,
	}
}

func (stats *MetricTableStatsAlt) WithPartition(timeSegment time.Time, segment string) {
	stats.Segment = segment
	stats.TimeSegment = timeSegment
}

func (stats *MetricTableStatsAlt) ToDefault(timeSegment time.Time, segment string) {
	stats.Segment = segment
	stats.TimeSegment = timeSegment
}

//
// FIELD DISTRIBUTION
//

type MetricFieldDistribution struct {
	Field       string    `ch:"field"        bigquery:"field"        db:"field"        json:"field"`
	Segment     string    `ch:"segment"      bigquery:"segment"      db:"segment"      json:"segment"`
	TimeSegment time.Time `ch:"time_segment" bigquery:"time_segment" db:"time_segment" json:"time_segment"`

	Counts []int64  `ch:"counts" bigquery:"counts" db:"counts" json:"counts"`
	Labels []string `ch:"labels" bigquery:"labels" db:"labels" json:"labels"`
}

func (metric *MetricFieldDistribution) GetIdentity() MetricIdentity {
	return MetricIdentity{
		metric.TimeSegment,
		metric.Segment,
	}
}

func (metric *MetricFieldDistribution) ToDefault(timeSegment time.Time, segment string) {
	metric.Segment = segment
	metric.TimeSegment = timeSegment
	metric.Counts = []int64{}
	metric.Labels = []string{}
}

//
// FIELD STATS
//

type MetricNumericFieldStatsBQ struct {
	Field       string    `bigquery:"field"        json:"field"`
	Segment     string    `bigquery:"segment"      json:"segment"`
	TimeSegment time.Time `bigquery:"time_segment" json:"time_segment"`

	NumTotal   int64              `bigquery:"num_rows"     json:"num_rows"`
	NumUnique  bigquery.NullInt64 `bigquery:"num_unique"   json:"num_unique"`
	NumNotNull int64              `bigquery:"num_not_null" json:"num_not_null"`
	NumEmpty   int64              `bigquery:"num_empty"    json:"num_empty"`

	PctUnique  *float64
	PctNotNull float64
	PctEmpty   float64

	Min    bigquery.NullFloat64 `bigquery:"min"    json:"min"`
	Max    bigquery.NullFloat64 `bigquery:"max"    json:"max"`
	Mean   bigquery.NullFloat64 `bigquery:"mean"   json:"mean"`
	Median bigquery.NullFloat64 `bigquery:"median" json:"median"`
	Stddev bigquery.NullFloat64 `bigquery:"stddev" json:"stddev"`
}

func (stats *MetricNumericFieldStatsBQ) GetIdentity() MetricIdentity {
	return MetricIdentity{
		stats.TimeSegment,
		stats.Segment,
	}
}

func (stats *MetricNumericFieldStatsBQ) ToDefault(timeSegment time.Time, segment string) {
	stats.Segment = segment
	stats.TimeSegment = timeSegment

	stats.NumTotal = 0
	stats.NumNotNull = 0
	stats.NumEmpty = 0
	stats.NumUnique = bigquery.NullInt64{}

	stats.PctUnique = nil
	stats.Min = bigquery.NullFloat64{}
	stats.Max = bigquery.NullFloat64{}
	stats.Mean = bigquery.NullFloat64{}
	stats.Median = bigquery.NullFloat64{}
	stats.Stddev = bigquery.NullFloat64{}
}

func (stats *MetricNumericFieldStatsBQ) WithPartition(timeSegment time.Time, segment string) {
	stats.Segment = segment
	stats.TimeSegment = timeSegment
}

type MetricNumericFieldStats struct {
	Field       string    `ch:"field"        bigquery:"field"        db:"field"        json:"field"`
	Segment     string    `ch:"segment"      bigquery:"segment"      db:"segment"      json:"segment"`
	TimeSegment time.Time `ch:"time_segment" bigquery:"time_segment" db:"time_segment" json:"time_segment"`

	NumTotal   int64  `ch:"num_rows"     bigquery:"num_rows"     db:"num_rows"     json:"num_rows"`
	NumUnique  *int64 `ch:"num_unique"   bigquery:"num_unique"   db:"num_unique"   json:"num_unique"`
	NumNotNull int64  `ch:"num_not_null" bigquery:"num_not_null" db:"num_not_null" json:"num_not_null"`
	NumEmpty   int64  `ch:"num_empty"    bigquery:"num_empty"    db:"num_empty"    json:"num_empty"`

	PctUnique  *float64
	PctNotNull float64
	PctEmpty   float64

	Min    *float64 `ch:"min"    bigquery:"min"    db:"min"    json:"min"`
	Max    *float64 `ch:"max"    bigquery:"max"    db:"max"    json:"max"`
	Mean   *float64 `ch:"mean"   bigquery:"mean"   db:"mean"   json:"mean"`
	Median *float64 `ch:"median" bigquery:"median" db:"median" json:"median"`
	Stddev *float64 `ch:"stddev" bigquery:"stddev" db:"stddev" json:"stddev"`
}

func (stats *MetricNumericFieldStats) GetIdentity() MetricIdentity {
	return MetricIdentity{
		stats.TimeSegment,
		stats.Segment,
	}
}

func (stats *MetricNumericFieldStats) ToDefault(timeSegment time.Time, segment string) {
	stats.Segment = segment
	stats.TimeSegment = timeSegment

	stats.NumTotal = 0
	stats.NumNotNull = 0
	stats.NumEmpty = 0
	stats.NumUnique = nil

	stats.PctUnique = nil
	stats.Min = nil
	stats.Max = nil
	stats.Mean = nil
	stats.Median = nil
	stats.Stddev = nil
}

func (stats *MetricNumericFieldStats) WithPartition(timeSegment time.Time, segment string) {
	stats.Segment = segment
	stats.TimeSegment = timeSegment
}

type MetricTimeFieldStats struct {
	Field       string    `ch:"field"        bigquery:"field"        db:"field"        json:"field"`
	Segment     string    `ch:"segment"      bigquery:"segment"      db:"segment"      json:"segment"`
	TimeSegment time.Time `ch:"time_segment" bigquery:"time_segment" db:"time_segment" json:"time_segment"`

	NumTotal   int64      `ch:"num_rows"     bigquery:"num_rows"     db:"num_rows"     json:"num_rows"`
	NumUnique  int64      `ch:"num_unique"   bigquery:"num_unique"   db:"num_unique"   json:"num_unique"`
	NumNotNull int64      `ch:"num_not_null" bigquery:"num_not_null" db:"num_not_null" json:"num_not_null"`
	Min        *time.Time `ch:"min"          bigquery:"min"          db:"min"          json:"min"`
	Max        *time.Time `ch:"max"          bigquery:"max"          db:"max"          json:"max"`

	PctUnique  float64
	PctNotNull float64
	PctEmpty   float64
}

func (stats *MetricTimeFieldStats) GetIdentity() MetricIdentity {
	return MetricIdentity{
		stats.TimeSegment,
		stats.Segment,
	}
}

func (stats *MetricTimeFieldStats) ToDefault(timeSegment time.Time, segment string) {
	stats.Segment = segment
	stats.TimeSegment = timeSegment
	stats.NumTotal = 0
	stats.NumUnique = 0
	stats.NumNotNull = 0
}

func (stats *MetricTimeFieldStats) WithPartition(timeSegment time.Time, segment string) {
	stats.Segment = segment
	stats.TimeSegment = timeSegment
}

type MetricTextFieldStats struct {
	Field       string    `ch:"field"        bigquery:"field"        db:"field"        json:"field"`
	Segment     string    `ch:"segment"      bigquery:"segment"      db:"segment"      json:"segment"`
	TimeSegment time.Time `ch:"time_segment" bigquery:"time_segment" db:"time_segment" json:"time_segment"`

	NumTotal   int64 `ch:"num_rows"     bigquery:"num_rows"     db:"num_rows"     json:"num_rows"`
	NumUnique  int64 `ch:"num_unique"   bigquery:"num_unique"   db:"num_unique"   json:"num_unique"`
	NumNotNull int64 `ch:"num_not_null" bigquery:"num_not_null" db:"num_not_null" json:"num_not_null"`
	NumEmpty   int64 `ch:"num_empty"    bigquery:"num_empty"    db:"num_empty"    json:"num_empty"`

	PctUnique  float64
	PctNotNull float64
	PctEmpty   float64
}

func (stats *MetricTextFieldStats) GetIdentity() MetricIdentity {
	return MetricIdentity{
		stats.TimeSegment,
		stats.Segment,
	}
}

func (stats *MetricTextFieldStats) ToDefault(timeSegment time.Time, segment string) {
	stats.Segment = segment
	stats.TimeSegment = timeSegment
	stats.NumTotal = 0
	stats.NumUnique = 0
	stats.NumNotNull = 0
	stats.NumEmpty = 0
}

func (stats *MetricTextFieldStats) WithPartition(timeSegment time.Time, segment string) {
	stats.Segment = segment
	stats.TimeSegment = timeSegment
}
