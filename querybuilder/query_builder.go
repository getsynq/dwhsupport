package querybuilder

import (
	"fmt"
	"time"

	. "github.com/getsynq/dwhsupport/sqldialect"
	"github.com/samber/lo"
)

//
// Query Builder
//

type QueryBuilder struct {
	timeCol  *TimeColExpr
	timeFrom time.Time
	timeTo   time.Time

	timeSegment      *time.Duration
	timeSegmentShift *time.Duration

	segments           []Expr
	segmentValues      map[int][]Expr
	segmentIsExcluding map[int]bool

	groupBy []Expr

	filters []CondExpr

	q *Select
}

func (b *QueryBuilder) WithSegment(segment Expr) *QueryBuilder {
	b.segments = append(b.segments, segment)

	return b
}

func (b *QueryBuilder) WithSegmentFiltered(segment Expr, values []string, isExcluding bool) *QueryBuilder {
	valueExprs := lo.Map(values, func(val string, _ int) Expr { return String(val) })
	i := len(b.segments)
	b.segments = append(b.segments, segment)
	b.segmentValues[i] = valueExprs
	b.segmentIsExcluding[i] = isExcluding
	return b
}

func (b *QueryBuilder) WithTimeRange(from, to time.Time) (*QueryBuilder, error) {
	b.timeFrom = from.Round(time.Minute)
	b.timeTo = to.Round(time.Minute)

	return b, nil
}

func (b *QueryBuilder) WithFieldTimeRange(col *TimeColExpr, from, to time.Time) *QueryBuilder {
	b.timeCol = col
	b.timeFrom = from.Round(time.Minute)
	b.timeTo = to.Round(time.Minute)

	return b
}

func (b *QueryBuilder) GetTimeRange() (time.Time, time.Time, error) {
	if b.timeCol != nil && b.timeFrom != (time.Time{}) && b.timeTo != (time.Time{}) {
		return b.timeFrom, b.timeTo, nil
	}

	return time.Time{}, time.Time{}, fmt.Errorf("time range not set")
}

func (b *QueryBuilder) WithShiftedTimeSegment(col *TimeColExpr, segment time.Duration, timeShift time.Duration) *QueryBuilder {
	b.timeCol = col
	b.timeSegment = &segment
	b.timeSegmentShift = &timeShift

	return b
}

func (b *QueryBuilder) WithTimeSegment(col *TimeColExpr, segment time.Duration) *QueryBuilder {
	b.timeCol = col
	b.timeSegment = &segment

	return b
}

func (b *QueryBuilder) WithLimit(limit int64) *QueryBuilder {
	b.q = b.q.WithLimit(Limit(Int64(limit)))

	return b
}

func (b *QueryBuilder) OrderBy(orderBy ...*OrderExpr) *QueryBuilder {
	b.q = b.q.OrderBy(orderBy...)

	return b
}

func (b *QueryBuilder) GetTimeSegment() (time.Duration, error) {
	if b.timeCol != nil && b.timeSegment != nil {
		return *b.timeSegment, nil
	}

	return 0, fmt.Errorf("time segment not set")
}

func (b *QueryBuilder) WithFilter(filter CondExpr) *QueryBuilder {
	b.filters = append(b.filters, filter)

	return b
}

func (b *QueryBuilder) WithGroupBy(groupBy ...Expr) *QueryBuilder {
	b.groupBy = append(b.groupBy, groupBy...)
	return b
}

func NewQueryBuilder(table *TableFqnExpr, cols []Expr) *QueryBuilder {
	return &QueryBuilder{q: NewSelect().
		From(table).
		Cols(cols...)}
}

func (b *QueryBuilder) ToSql(dialect Dialect) (string, error) {
	q := b.q

	// Apply custom segment

	for i, segment := range b.segments {
		values, hasFiltration := b.segmentValues[i]
		segmentExpr := Coalesce(
			ToString(segment),
			String(""),
		)
		alias := b.segmentColumnName(i)
		q = q.
			Cols(As(segmentExpr, Identifier(alias))).
			GroupBy(AggregationColumnReference(segmentExpr, alias))

		if hasFiltration {
			if b.segmentIsExcluding[i] {
				q = q.Where(NotIn(AggregationColumnReference(segmentExpr, alias), values...))
			} else {
				q = q.Where(In(AggregationColumnReference(segmentExpr, alias), values...))
			}
		}
	}

	if b.timeCol != nil && b.timeSegment != nil {
		if b.timeSegmentShift != nil {
			timeExpr := dialect.AddTime(
				dialect.CeilTime(
					dialect.SubTime(b.timeCol, *b.timeSegmentShift),
					*b.timeSegment,
				),
				*b.timeSegmentShift,
			)
			q = q.
				Cols(As(timeExpr, Identifier("time_segment"))).
				GroupBy(Identifier("time_segment"))
		} else {
			timeExpr := dialect.CeilTime(b.timeCol, *b.timeSegment)
			q = q.
				Cols(As(timeExpr, Identifier("time_segment"))).
				GroupBy(Identifier("time_segment"))
		}
	}

	// Apply time constraint and segment if given. Not using BETWEEN as its inclusive for both limits and we risk double counting.
	if b.timeCol != nil && b.timeFrom != (time.Time{}) && b.timeTo != (time.Time{}) {
		q = q.Where(
			Gte(b.timeCol, Time(b.timeFrom)),
			Lt(b.timeCol, Time(b.timeTo)),
		)
	}

	if len(b.groupBy) > 0 {
		q = q.GroupBy(b.groupBy...)
	}

	// Apply custom filters
	if len(b.filters) > 0 {
		q = q.Where(b.filters...)
	}

	return q.ToSql(dialect)
}

func (b *QueryBuilder) segmentColumnName(i int) string {
	if i == 0 {
		return "segment"
	}
	return fmt.Sprintf("segment_%d", i+1)
}
