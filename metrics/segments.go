package metrics

import (
	"fmt"
	"time"

	"github.com/getsynq/dwhsupport/querybuilder"
	. "github.com/getsynq/dwhsupport/sqldialect"
	"github.com/samber/lo"
)

type Partition struct {
	Field string
	From  time.Time
	To    time.Time
}

func SegmentsListQuery(
	tableFqn TableExpr,
	args *MonitorArgs,
	partition *Partition,
	rowsLimit int64,
	segmentLengthLimit int64,
) (*querybuilder.QueryBuilder, error) {
	if len(args.Segmentation) == 0 {
		return nil, fmt.Errorf("segmentation is not configured")
	}

	var selectExpressions []Expr
	var aggregationExpressions []Expr
	var segmentColumns []string

	for i, s := range args.Segmentation {
		alias := fmt.Sprintf("segment%d", i+1)
		if i == 0 {
			alias = "segment"
		}
		segmentColumns = append(segmentColumns, alias)
		selectExpressions = append(selectExpressions, As(SubString(ToString(s.Expression), 1, segmentLengthLimit), Identifier(alias)))
		aggregationExpressions = append(aggregationExpressions, SubString(ToString(s.Expression), 1, segmentLengthLimit))
	}
	countColExpr := Identifier(string(METRIC_NUM_ROWS))
	selectExpressions = append(selectExpressions, As(CountAll(), countColExpr))

	query := querybuilder.NewQueryBuilder(tableFqn, selectExpressions)

	groupBy := lo.Map(segmentColumns, func(segmentColumn string, i int) Expr {
		return AggregationColumnReference(aggregationExpressions[i], segmentColumn)
	})
	query.WithGroupBy(groupBy...)

	if partition != nil {
		query = query.WithFieldTimeRange(TimeCol(partition.Field), partition.From, partition.To)
	}

	for _, condition := range args.Conditions {
		query = query.WithFilter(condition)
	}

	query = query.OrderBy(Desc(AggregationColumnReference(CountAll(), string(METRIC_NUM_ROWS)))).WithLimit(rowsLimit)

	return query, nil
}
