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

// ProfiledColumn records the output-column alias prefix assigned to a profiled
// column. The metric output columns for the column are named
// "<AliasPrefix>__<metricId>". AliasPrefix is a valid SQL identifier, unique
// within a single ProfileColumns call: collisions after sanitization (e.g. a
// nested field "sku.id" and a literal column "sku_id" both sanitizing to
// "sku_id") are disambiguated with an increasing numeric suffix. Callers map
// result columns back to the requested column via this mapping instead of
// replicating the sanitization rule.
type ProfiledColumn struct {
	Column        string
	ColumnProfile ColumnProfile
	AliasPrefix   string
}

func ProfileColumns(
	dialect Dialect,
	tableFqn TableExpr,
	columnsToProfile []*ColumnToProfile,
	args *MonitorArgs,
	partition *Partition,
	limit int64,
	segmentLengthLimit int64,
) (*querybuilder.QueryBuilder, []*ProfiledColumn, error) {

	var expressions []Expr
	var segmentColumns []string
	var segmentExprs []Expr

	for i, s := range args.Segmentation {
		alias := fmt.Sprintf("segment%d", i+1)
		if i == 0 {
			alias = "segment"
		}
		segmentColumns = append(segmentColumns, alias)
		segmentExpr := SubString(ToString(s.Expression), 1, segmentLengthLimit)
		segmentExprs = append(segmentExprs, segmentExpr)
		expressions = append(expressions, As(segmentExpr, Identifier(alias)))
	}
	countColExpr := Identifier(string(METRIC_NUM_ROWS))
	expressions = append(expressions, As(CountAll(), countColExpr))

	usedAliasPrefixes := map[string]struct{}{}
	profiledColumns := make([]*ProfiledColumn, 0, len(columnsToProfile))

	for _, toProfile := range columnsToProfile {
		aliasPrefix := uniqueAliasPrefix(toProfile.Column, usedAliasPrefixes)
		profiledColumns = append(profiledColumns, &ProfiledColumn{
			Column:        toProfile.Column,
			ColumnProfile: toProfile.ColumnProfile,
			AliasPrefix:   aliasPrefix,
		})
		switch toProfile.ColumnProfile {
		case ColumnProfileUnknown:
			expressions = append(expressions, UnknownMetricsValuesCols(toProfile.Column, WithPrefixForColumn(aliasPrefix))...)
		case ColumnProfileString:
			expressions = append(expressions, TextMetricsValuesCols(toProfile.Column, WithPrefixForColumn(aliasPrefix))...)
			expressions = append(expressions, TextMetricsLengthCols(toProfile.Column, WithPrefixForColumn(aliasPrefix))...)
		case ColumnProfileNumeric:
			expressions = append(expressions, NumericMetricsValuesCols(toProfile.Column, dialect, WithPrefixForColumn(aliasPrefix))...)
		case ColumnProfileTime:
			expressions = append(expressions, TimeMetricsValuesCols(toProfile.Column, WithPrefixForColumn(aliasPrefix))...)
		}
	}

	query := querybuilder.NewQueryBuilder(tableFqn, expressions).OrderBy(Desc(countColExpr)).WithLimit(limit)

	groupBy := lo.Map(segmentColumns, func(segmentColumn string, i int) Expr {
		return AggregationColumnReference(segmentExprs[i], segmentColumn)
	})
	query.WithGroupBy(groupBy...)

	if partition != nil {
		query = query.WithFieldTimeRange(TimeCol(partition.Field), partition.From, partition.To)
	}

	for _, condition := range args.Conditions {
		query = query.WithFilter(condition)
	}

	return query, profiledColumns, nil
}

// uniqueAliasPrefix sanitizes column into a valid identifier and guarantees it
// is unique among used: the first occurrence keeps the sanitized name, later
// collisions get an increasing "_2", "_3", ... suffix. Readable aliases keep
// the generated SQL legible in query logs while staying collision-free, so two
// columns that sanitize to the same form (e.g. "sku.id" and "sku_id") never
// share an output column. used is mutated to record every prefix returned.
func uniqueAliasPrefix(column string, used map[string]struct{}) string {
	base := sanitizeAliasPrefix(column)
	candidate := base
	for n := 2; ; n++ {
		if _, taken := used[candidate]; !taken {
			used[candidate] = struct{}{}
			return candidate
		}
		candidate = fmt.Sprintf("%s_%d", base, n)
	}
}
