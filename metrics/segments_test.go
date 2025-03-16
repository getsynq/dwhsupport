package metrics

import (
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/sqldialect"
	dwhsql "github.com/getsynq/dwhsupport/sqldialect"
	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/suite"
)

type SegmentsSuite struct {
	suite.Suite
}

func TestSegmentsSuite(t *testing.T) {
	suite.Run(t, new(SegmentsSuite))
}

func (s *SegmentsSuite) TestSegmentQueries() {

	for _, dialect := range []struct {
		name    string
		dialect dwhsql.Dialect
	}{
		{"clickhouse", dwhsql.NewClickHouseDialect()},
		{"snowflake", dwhsql.NewSnowflakeDialect()},
		{"redshift", dwhsql.NewRedshiftDialect()},
		{"bigquery", dwhsql.NewBigQueryDialect()},
		{"postgres", dwhsql.NewPostgresDialect()},
		{"mysql", dwhsql.NewMySQLDialect()},
		{"databricks", dwhsql.NewDatabricksDialect()},
		{"duckdb", dwhsql.NewDuckDBDialect()},
	} {

		tableFqnExpr := sqldialect.TableFqn("db", "default", "runs")

		args := &MonitorArgs{
			Conditions: []sqldialect.CondExpr{
				sqldialect.Sql("1=1"),
			},
			Segmentation: []*Segmentation{
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
		}

		partition := &Partition{
			Field: "created_at",
			From:  time.Date(1985, 7, 16, 0, 0, 0, 0, time.UTC),
			To:    time.Date(2025, 3, 16, 0, 0, 0, 0, time.UTC),
		}
		rowsLimit := int64(10)
		segmentLengthLimit := int64(100)

		s.Run(dialect.name, func() {

			queryBuilder, err := SegmentsListQuery(tableFqnExpr, args, partition, rowsLimit, segmentLengthLimit)
			s.Require().NoError(err)
			s.Require().NotNil(queryBuilder)

			sql, err := queryBuilder.ToSql(dialect.dialect)
			s.Require().NoError(err)
			s.Require().NotEmpty(sql)
			s.T().Log(sql)

			snaps.WithConfig(snaps.Dir("SegmentsListQuery"), snaps.Filename(dialect.name)).MatchSnapshot(s.T(), sql)

		})

	}
}
