package metrics

import (
	"fmt"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/querybuilder"
	dwhsql "github.com/getsynq/dwhsupport/sqldialect"
	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/suite"
)

type MetricsSuite struct {
	suite.Suite
}

func TestMetricsSuite(t *testing.T) {
	suite.Run(t, new(MetricsSuite))
}

func (s *MetricsSuite) TestSimpleQueryBuilder() {
	for _, dialect := range DialectsToTest() {
		tableFqnExpr := dwhsql.TableFqn("db", "default", "runs")
		queryBuilder := querybuilder.NewQueryBuilder(tableFqnExpr, TextMetricsCols("workspace"))
		sql, err := queryBuilder.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("SimpleQueryBuilder"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *MetricsSuite) TestMultiMetricValues() {
	for _, dialect := range DialectsToTest() {
		tableFqnExpr := dwhsql.TableFqn("db", "default", "runs")
		queryBuilder := querybuilder.NewQueryBuilder(tableFqnExpr, append(
			TextMetricsValuesCols("workspace", WithPrefixForColumn("workspace")),
			NumericMetricsValuesCols("run_type", dialect.Dialect, WithPrefixForColumn("run_type"))...,
		))
		queryBuilder = queryBuilder.WithSegment(dwhsql.Identifier("workspace"))
		queryBuilder = queryBuilder.WithSegment(dwhsql.Identifier("run_type"))
		sql, err := queryBuilder.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("MultiMetricValues"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *MetricsSuite) TestApplyMonitorDefArgs() {

	tableFqnExpr := dwhsql.TableFqn("db", "default", "runs")

	for _, dialect := range DialectsToTest() {

		for _, cond := range []struct {
			name       string
			conditions []dwhsql.CondExpr
		}{
			{
				"no_conditions",
				nil,
			},
			{
				"single_condition",
				[]dwhsql.CondExpr{
					dwhsql.Sql("1=1"),
				},
			},

			{
				"multi_condition",
				[]dwhsql.CondExpr{
					dwhsql.Sql("run_status > 0"),
					dwhsql.Sql("run_type > 0"),
				},
			},
		} {
			for _, seg := range []struct {
				name         string
				segmentation []*Segmentation
			}{
				{
					"no_segmentation",
					nil,
				},
				{
					"single_segmentation_all",
					[]*Segmentation{
						{
							Expression: dwhsql.Sql("run_type"),
							Rule:       AllSegments(),
						},
					},
				},
				{
					"multi_segmentation",
					[]*Segmentation{
						{
							Expression: dwhsql.Sql("workspace"),
							Rule:       ExcludeSegments("synq-demo"),
						},
						{
							Expression: dwhsql.Sql("run_status"),
							Rule:       AcceptSegments("1", "2", "3", "4"),
						},
						{
							Expression: dwhsql.Sql("run_type"),
							Rule:       AllSegments(),
						},
					},
				},
			} {
				monitorArgs := &MonitorArgs{
					Conditions:   cond.conditions,
					Segmentation: seg.segmentation,
				}

				s.Run(fmt.Sprintf("%s_%s_%s", dialect.Name, cond.name, seg.name), func() {

					expressions := TimeMetricsCols("ingested_at")

					s.Run("partitioning", func() {

						partitioning := &MonitorPartitioning{
							Field:    "ingested_at",
							Interval: 1 * time.Hour,
						}

						queryBuilder := querybuilder.NewQueryBuilder(tableFqnExpr, expressions)
						queryBuilder = ApplyMonitorDefArgs(queryBuilder, monitorArgs, partitioning, 100)
						sql, err := queryBuilder.ToSql(dialect.Dialect)
						s.Require().NoError(err)
						s.Require().NotEmpty(sql)
						s.T().Log(sql)

						snaps.WithConfig(snaps.Dir("ApplyMonitorDefArgs"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
					})

					s.Run("no_partitioning", func() {

						queryBuilder := querybuilder.NewQueryBuilder(tableFqnExpr, expressions)
						queryBuilder = ApplyMonitorDefArgs(queryBuilder, monitorArgs, nil, 100)
						sql, err := queryBuilder.ToSql(dialect.Dialect)
						s.Require().NoError(err)
						s.Require().NotEmpty(sql)
						s.T().Log(sql)

						snaps.WithConfig(snaps.Dir("ApplyMonitorDefArgs"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
					})
				})
			}
		}

	}
}

func (s *MetricsSuite) TestSegmentationRules() {

	tableFqnExpr := dwhsql.TableFqn("db", "default", "runs")

	for _, dialect := range DialectsToTest() {
		for _, seg := range []struct {
			name         string
			segmentation []*Segmentation
		}{
			{
				"empty_exclude",
				[]*Segmentation{
					{
						Expression: dwhsql.Sql("workspace"),
						Rule:       ExcludeSegments(),
					},
				},
			},
			{
				"empty_include",
				[]*Segmentation{
					{
						Expression: dwhsql.Sql("workspace"),
						Rule:       AcceptSegments(),
					},
				},
			},
			{
				"allowed_segments",
				[]*Segmentation{
					{
						Expression: dwhsql.Sql("workspace"),
						Rule:       AcceptSegments("synq-demo", "synq-demo-2"),
					},
				},
			},
		} {
			monitorArgs := &MonitorArgs{
				Segmentation: seg.segmentation,
			}

			s.Run(fmt.Sprintf("%s_%s", dialect.Name, seg.name), func() {

				expressions := TimeMetricsValuesCols("ingested_at")

				queryBuilder := querybuilder.NewQueryBuilder(tableFqnExpr, expressions)
				queryBuilder = ApplyMonitorDefArgs(queryBuilder, monitorArgs, nil, 100)
				sql, err := queryBuilder.ToSql(dialect.Dialect)
				s.Require().NoError(err)
				s.Require().NotEmpty(sql)
				s.T().Log(sql)

				snaps.WithConfig(snaps.Dir("SegmentationRules"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
			})
		}
	}
}

func (s *MetricsSuite) TestPartition() {

	tableFqnExpr := dwhsql.TableFqn("db", "default", "runs")

	for _, dialect := range DialectsToTest() {

		expressions := TimeMetricsCols("ingested_at")

		partition := &Partition{
			Field: "ingested_at",
			From:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			To:    time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond),
		}

		queryBuilder := querybuilder.NewQueryBuilder(tableFqnExpr, expressions)
		queryBuilder = queryBuilder.WithFieldTimeRange(dwhsql.TimeCol(partition.Field), partition.From, partition.To)
		sql, err := queryBuilder.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("Partition"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *MetricsSuite) TestPartitionWithTimeRange() {

	tableFqnExpr := dwhsql.TableFqn("db", "default", "runs")

	for _, dialect := range DialectsToTest() {

		expressions := NumericMetricsCols("run_type", dialect.Dialect)

		partition := &Partition{
			Field: "ingested_at",
			From:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			To:    time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond),
		}

		queryBuilder := querybuilder.NewQueryBuilder(tableFqnExpr, expressions)
		queryBuilder = queryBuilder.WithTimeSegment(dwhsql.TimeCol(partition.Field), 24*time.Hour)
		queryBuilder = queryBuilder.WithFieldTimeRange(dwhsql.TimeCol(partition.Field), partition.From, partition.To)
		sql, err := queryBuilder.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("PartitionWithTimeRange"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *MetricsSuite) TestSegmentWithTimeRangeWithFilter() {

	tableFqnExpr := dwhsql.TableFqn("db", "default", "runs")

	for _, dialect := range DialectsToTest() {

		expressions := NumericMetricsCols("run_type", dialect.Dialect)

		partition := &Partition{
			Field: "ingested_at",
			From:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			To:    time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond),
		}

		queryBuilder := querybuilder.NewQueryBuilder(tableFqnExpr, expressions)
		queryBuilder = queryBuilder.WithSegment(dwhsql.ToString(dwhsql.Identifier("workspace")))
		queryBuilder = queryBuilder.WithFieldTimeRange(dwhsql.TimeCol(partition.Field), partition.From, partition.To)
		queryBuilder = queryBuilder.WithFilter(dwhsql.Sql("workspace = 'synq-demo' OR 1=1"))
		sql, err := queryBuilder.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("SegmentWithTimeRange"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}
