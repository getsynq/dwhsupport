package metrics

import (
	"fmt"
	"time"

	"github.com/getsynq/dwhsupport/querybuilder"
	. "github.com/getsynq/dwhsupport/sqldialect"
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
	limit int64,
) (*querybuilder.QueryBuilder, error) {
	filter := args.Filter
	segmentation := args.Segmentation

	if segmentation == nil {
		return nil, fmt.Errorf("segmentation is not configured")
	}

	query := querybuilder.
		NewQueryBuilder(tableFqn, []Expr{Distinct(As(ToString(Sql(segmentation.Field)), Identifier("segment")))}).
		OrderBy(Asc(Identifier("segment"))).
		WithLimit(limit)

	if partition != nil {
		query = query.WithFieldTimeRange(TimeCol(partition.Field), partition.From, partition.To)
	}

	if filter != "" {
		query = query.WithFilter(Sql(filter))
	}

	return query, nil
}
