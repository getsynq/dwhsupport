package metrics

import (
	"fmt"
	"testing"

	dwhsql "github.com/getsynq/dwhsupport/sqldialect"
	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/suite"
)

type ProfileSuite struct {
	suite.Suite
}

func TestProfileSuite(t *testing.T) {
	suite.Run(t, new(ProfileSuite))
}

func (s *ProfileSuite) TestProfileColumns() {

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

					columnsToProfile := []*ColumnToProfile{
						{
							Column:        "workspace",
							ColumnProfile: ColumnProfileString,
						},
						{
							Column:        "meta",
							ColumnProfile: ColumnProfileString,
						},
						{
							Column:        "run_status",
							ColumnProfile: ColumnProfileNumeric,
						},
						{
							Column:        "created_at",
							ColumnProfile: ColumnProfileTime,
						},
					}

					queryBuilder, err := ProfileColumns(tableFqnExpr, columnsToProfile, monitorArgs, nil, 1000, 100)
					s.Require().NoError(err)
					s.Require().NotNil(queryBuilder)
					sql, err := queryBuilder.ToSql(dialect.Dialect)
					s.Require().NoError(err)
					s.Require().NotEmpty(sql)
					s.T().Log(sql)

					snaps.WithConfig(snaps.Dir("ProfileColumns"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)

				})
			}
		}

	}
}
