package metrics

import (
	"fmt"

	"github.com/getsynq/dwhsupport/querybuilder"
	. "github.com/getsynq/dwhsupport/sqldialect"
	"github.com/samber/lo"
)

type ColumnProfile int

const (
	ColumnProfileUnknown ColumnProfile = iota
	ColumnProfileString  ColumnProfile = iota
	ColumnProfileNumeric ColumnProfile = iota
	ColumnProfileTime    ColumnProfile = iota
)

type ColumnToProfile struct {
	Column        string
	ColumnProfile ColumnProfile
}

func ProfileColumns(
	tableFqn *TableFqnExpr,
	columnsToProfile []*ColumnToProfile,
	args *MonitorArgs,
	partition *Partition,
	limit int64,
	segmentLengthLimit int64,
) (*querybuilder.QueryBuilder, error) {

	var expressions []Expr
	var segmentColumns []string

	for i, s := range args.Segmentation {
		alias := fmt.Sprintf("segment%d", i+1)
		if i == 0 {
			alias = "segment"
		}
		segmentColumns = append(segmentColumns, alias)
		expressions = append(expressions, As(SubString(ToString(s.Expression), 1, segmentLengthLimit), Identifier(alias)))
	}
	countColExpr := Identifier(string(METRIC_NUM_ROWS))
	expressions = append(expressions, As(CountAll(), countColExpr))

	for _, toProfile := range columnsToProfile {
		switch toProfile.ColumnProfile {
		case ColumnProfileUnknown:
			expressions = append(expressions, UnknownMetricsValuesCols(toProfile.Column, WithPrefixForColumn(toProfile.Column))...)
		case ColumnProfileString:
			expressions = append(expressions, TextMetricsValuesCols(toProfile.Column, WithPrefixForColumn(toProfile.Column))...)
			expressions = append(expressions, TextMetricsLengthCols(toProfile.Column, WithPrefixForColumn(toProfile.Column))...)
		case ColumnProfileNumeric:
			expressions = append(expressions, NumericMetricsValuesCols(toProfile.Column, WithPrefixForColumn(toProfile.Column))...)
		case ColumnProfileTime:
			expressions = append(expressions, TimeMetricsValuesCols(toProfile.Column, WithPrefixForColumn(toProfile.Column))...)
		}
	}

	query := querybuilder.NewQueryBuilder(tableFqn, expressions).OrderBy(Desc(countColExpr)).WithLimit(limit)

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
