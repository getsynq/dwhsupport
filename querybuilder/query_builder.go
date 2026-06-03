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

func NewQueryBuilder(table TableExpr, cols []Expr) *QueryBuilder {
	return &QueryBuilder{
		table: table,
		cols:  cols,
	}
}

type QueryBuilder struct {
	timeCol      *TimeColExpr
	timeFrom     time.Time
	timeTo       time.Time
	timeFromExpr Expr
	timeToExpr   Expr

	timeSegment      *time.Duration
	timeSegmentShift *time.Duration

	segments           []TextExpr
	segmentValues      map[int][]TextExpr
	segmentIsExcluding map[int]bool

	groupBy []Expr

	filters []CondExpr

	table   TableExpr
	cols    []Expr
	limit   LimitClauseExpr
	orderBy []OrderByExpr

	cteNames   []string
	cteQueries []CteExpr
}

func (b *QueryBuilder) WithSegment(segment TextExpr) *QueryBuilder {
	b.segments = append(b.segments, segment)

	return b
}

func (b *QueryBuilder) WithSegmentFiltered(segment TextExpr, values []string, isExcluding bool) *QueryBuilder {
	valueExprs := lo.Map(values, func(val string, _ int) TextExpr { return String(val) })
	i := len(b.segments)
	b.segments = append(b.segments, segment)
	if b.segmentValues == nil {
		b.segmentValues = map[int][]TextExpr{}
	}
	b.segmentValues[i] = valueExprs
	if b.segmentIsExcluding == nil {
		b.segmentIsExcluding = map[int]bool{}
	}
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

// WithTimeRangeExpr sets arbitrary expressions for the lower (inclusive) and upper
// (exclusive) bounds of the time-range WHERE clause. Useful for emitting SQL with
// placeholder bounds (e.g. {from}, {to}) instead of materialized timestamp literals.
// Requires the time column to be set via WithTimeSegment / WithShiftedTimeSegment /
// WithFieldTimeRange; otherwise the WHERE clause is omitted (same gate as the
// time.Time-based path).
func (b *QueryBuilder) WithTimeRangeExpr(from, to Expr) *QueryBuilder {
	b.timeFromExpr = from
	b.timeToExpr = to

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
	b.limit = Limit(Int64(limit))

	return b
}

func (b *QueryBuilder) OrderBy(orderBy ...OrderByExpr) *QueryBuilder {

	b.orderBy = append(b.orderBy, orderBy...)

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

func (b *QueryBuilder) ToSql(dialect Dialect) (string, error) {
	q := NewSelect().From(b.table)

	for i, cteQuery := range b.cteQueries {
		cteName := b.cteNames[i]
		q = q.Cte(CteFqn(cteName), cteQuery)
	}

	var timeExpr Expr

	// Apply time segment, we will add column after segment columns
	if b.timeCol != nil && b.timeSegment != nil {
		if b.timeSegmentShift != nil {
			timeExpr = dialect.AddTime(
				dialect.CeilTime(
					dialect.SubTime(b.timeCol, *b.timeSegmentShift),
					*b.timeSegment,
				),
				*b.timeSegmentShift,
			)
			q = q.
				GroupBy(AggregationColumnReference(timeExpr, "time_segment"))
		} else {
			timeExpr = dialect.CeilTime(b.timeCol, *b.timeSegment)
			q = q.
				GroupBy(AggregationColumnReference(timeExpr, "time_segment"))
		}
	}

	// Apply custom segment

	for i, segment := range b.segments {
		values, hasFiltration := b.segmentValues[i]
		segmentExpr := Coalesce(
			segment,
			String(""),
		)
		alias := b.segmentColumnName(i)
		q = q.
			Cols(As(segmentExpr, Identifier(alias))).
			GroupBy(AggregationColumnReference(segmentExpr, alias))

		if hasFiltration {

			if b.segmentIsExcluding[i] {
				q = q.Where(NotIn(AggregationColumnReference(segmentExpr, alias), ToExprSlice(values)...))
			} else {
				q = q.Where(In(AggregationColumnReference(segmentExpr, alias), ToExprSlice(values)...))
			}
		}
	}

	if timeExpr != nil {
		q = q.Cols(As(timeExpr, Identifier("time_segment")))
	}

	if len(b.cols) > 0 {
		q = q.Cols(b.cols...)
	}

	// Apply time constraint and segment if given. Not using BETWEEN as its inclusive for both limits and we risk double counting.
	if b.timeCol != nil {
		fromExpr, toExpr := b.resolveTimeBoundExprs()
		if fromExpr != nil && toExpr != nil {
			q = q.Where(
				Gte(b.timeCol, fromExpr),
				Lt(b.timeCol, toExpr),
			)
		}
	}

	if len(b.groupBy) > 0 {
		q = q.GroupBy(b.groupBy...)
	}

	if len(b.orderBy) > 0 {
		q = q.OrderBy(b.orderBy...)
	} else {
		var orderBy []OrderByExpr
		if timeExpr != nil {
			orderBy = append(orderBy, Asc(AggregationColumnReference(timeExpr, "time_segment")))
		}
		for i, segment := range b.segments {
			segmentExpr := Coalesce(
				segment,
				String(""),
			)
			alias := b.segmentColumnName(i)
			orderBy = append(orderBy, Asc(AggregationColumnReference(segmentExpr, alias)))
		}

		q = q.OrderBy(orderBy...)
	}

	q.WithLimit(b.limit)

	// Apply custom filters
	if len(b.filters) > 0 {
		q = q.Where(AndGroups(b.filters...))
	}

	return q.ToSql(dialect)
}

// resolveTimeBoundExprs returns the (from, to) WHERE-clause bound expressions.
// Expression-typed bounds (WithTimeRangeExpr) take precedence over time.Time bounds.
// Returns (nil, nil) if neither pair is fully set.
func (b *QueryBuilder) resolveTimeBoundExprs() (Expr, Expr) {
	fromExpr := b.timeFromExpr
	toExpr := b.timeToExpr
	if fromExpr == nil && b.timeFrom != (time.Time{}) {
		fromExpr = Time(b.timeFrom)
	}
	if toExpr == nil && b.timeTo != (time.Time{}) {
		toExpr = Time(b.timeTo)
	}
	if fromExpr == nil || toExpr == nil {
		return nil, nil
	}
	return fromExpr, toExpr
}

func (b *QueryBuilder) segmentColumnName(i int) string {
	if i == 0 {
		return "segment"
	}
	return fmt.Sprintf("segment_%d", i+1)
}

func (b *QueryBuilder) WithCte(name string, query CteExpr) *QueryBuilder {
	b.cteNames = append(b.cteNames, name)
	b.cteQueries = append(b.cteQueries, query)
	return b
}
