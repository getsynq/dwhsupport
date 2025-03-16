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
	tableFqn *TableFqnExpr,
	args *MonitorArgs,
	partition *Partition,
	rowsLimit int64,
	segmentLengthLimit int64,
) (*querybuilder.QueryBuilder, error) {
	if len(args.Segmentation) == 0 {
		return nil, fmt.Errorf("segmentation is not configured")
	}

	var expressions []Expr
	var segmentColumns []string

	for i, s := range args.Segmentation {
		alias := fmt.Sprintf("segment%d", i+1)
		if i == 0 {
			alias = "segment"
		}
		segmentColumns = append(segmentColumns, alias)
		expressions = append(expressions, As(SubString(ToString(Sql(s.Field)), 1, segmentLengthLimit), Identifier(alias)))
	}
	countColExpr := Identifier(string(METRIC_NUM_ROWS))
	expressions = append(expressions, As(CountAll(), countColExpr))

	query := querybuilder.NewQueryBuilder(tableFqn, expressions).OrderBy(Desc(countColExpr)).WithLimit(rowsLimit)

	groupBy := lo.Map(segmentColumns, func(segmentColumn string, i int) Expr {
		return AggregationColumnReference(expressions[i], segmentColumn)
	})
	query.WithGroupBy(groupBy...)

	if partition != nil {
		query = query.WithFieldTimeRange(TimeCol(partition.Field), partition.From, partition.To)
	}

	for _, condition := range args.Conditions {
		query = query.WithFilter(condition)
	}

	return query, nil
}
