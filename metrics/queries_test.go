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

type TestedDialect struct {
	Name    string
	Dialect dwhsql.Dialect
}

func (s *MetricsSuite) Dialects() []*TestedDialect {
	return []*TestedDialect{
		{"clickhouse", dwhsql.NewClickHouseDialect()},
		{"snowflake", dwhsql.NewSnowflakeDialect()},
		{"redshift", dwhsql.NewRedshiftDialect()},
		{"bigquery", dwhsql.NewBigQueryDialect()},
		{"postgres", dwhsql.NewPostgresDialect()},
		{"mysql", dwhsql.NewMySQLDialect()},
		{"databricks", dwhsql.NewDatabricksDialect()},
		{"duckdb", dwhsql.NewDuckDBDialect()},
	}
}

func (s *MetricsSuite) TestSimpleQueryBuilder() {
	for _, dialect := range s.Dialects() {
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
	for _, dialect := range s.Dialects() {
		tableFqnExpr := dwhsql.TableFqn("db", "default", "runs")
		queryBuilder := querybuilder.NewQueryBuilder(tableFqnExpr, append(
			TextMetricsValuesCols("workspace", WithPrefixForColumn("workspace")),
			NumericMetricsValuesCols("run_type", WithPrefixForColumn("run_type"))...,
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

	for _, dialect := range s.Dialects() {

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
							Field: "run_type",
							Rule:  AllSegments(),
						},
					},
				},
				{
					"multi_segmentation",
					[]*Segmentation{
						{
							Field: "workspace",
							Rule:  ExcludeSegments("synq-demo"),
						},
						{
							Field: "run_status",
							Rule:  AcceptSegments("1", "2", "3", "4"),
						},
						{
							Field: "run_type",
							Rule:  AllSegments(),
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
						queryBuilder = ApplyMonitorDefArgs(queryBuilder, monitorArgs, partitioning)
						sql, err := queryBuilder.ToSql(dialect.Dialect)
						s.Require().NoError(err)
						s.Require().NotEmpty(sql)
						s.T().Log(sql)

						snaps.WithConfig(snaps.Dir("ApplyMonitorDefArgs"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
					})

					s.Run("no_partitioning", func() {

						queryBuilder := querybuilder.NewQueryBuilder(tableFqnExpr, expressions)
						queryBuilder = ApplyMonitorDefArgs(queryBuilder, monitorArgs, nil)
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

	for _, dialect := range s.Dialects() {
		for _, seg := range []struct {
			name         string
			segmentation []*Segmentation
		}{
			{
				"empty_exclude",
				[]*Segmentation{
					{
						Field: "workspace",
						Rule:  ExcludeSegments(),
					},
				},
			},
			{
				"empty_include",
				[]*Segmentation{
					{
						Field: "workspace",
						Rule:  AcceptSegments(),
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
				queryBuilder = ApplyMonitorDefArgs(queryBuilder, monitorArgs, nil)
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

	for _, dialect := range s.Dialects() {

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

	for _, dialect := range s.Dialects() {

		expressions := NumericMetricsCols("run_type")

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

func (s *MetricsSuite) TestSegmentWithTimeRange() {

	tableFqnExpr := dwhsql.TableFqn("db", "default", "runs")

	for _, dialect := range s.Dialects() {

		expressions := NumericMetricsCols("run_type")

		partition := &Partition{
			Field: "ingested_at",
			From:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			To:    time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond),
		}

		queryBuilder := querybuilder.NewQueryBuilder(tableFqnExpr, expressions)
		queryBuilder = queryBuilder.WithSegment(dwhsql.ToString(dwhsql.Identifier("workspace")))
		queryBuilder = queryBuilder.WithFieldTimeRange(dwhsql.TimeCol(partition.Field), partition.From, partition.To)
		sql, err := queryBuilder.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("SegmentWithTimeRange"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}
