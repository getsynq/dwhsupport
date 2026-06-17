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

	for _, dialect := range dwhsql.DialectsToTest() {

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
						{
							// STRUCT/RECORD and other unmapped types: the minimal
							// profile must stay valid SQL (non-null count only) —
							// no `= 0` or COUNT(DISTINCT) that warehouses reject on
							// non-comparable types.
							Column:        "sku",
							ColumnProfile: ColumnProfileUnknown,
						},
					}

					queryBuilder, _, err := ProfileColumns(dialect.Dialect, tableFqnExpr, columnsToProfile, monitorArgs, nil, 1000, 100)
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

// TestProfileColumnsAliasMapping asserts ProfileColumns reports a unique, valid
// alias prefix per requested column and disambiguates columns that sanitize to
// the same identifier with an increasing numeric suffix — so callers can map
// "<prefix>__<metric>" result columns back to the original column.
func (s *ProfileSuite) TestProfileColumnsAliasMapping() {
	dialect := dwhsql.NewBigQueryDialect()
	tableFqnExpr := dwhsql.TableFqn("db", "default", "runs")

	columnsToProfile := []*ColumnToProfile{
		{Column: "amount", ColumnProfile: ColumnProfileNumeric},
		// Nested struct field arrives as a dotted path...
		{Column: "sku.id", ColumnProfile: ColumnProfileString},
		// ...and collides after sanitization with a literal "sku_id" column.
		{Column: "sku_id", ColumnProfile: ColumnProfileString},
		// A third collision exercises the "_3" suffix.
		{Column: "sku-id", ColumnProfile: ColumnProfileString},
	}

	qb, profiled, err := ProfileColumns(dialect, tableFqnExpr, columnsToProfile, &MonitorArgs{}, nil, 1000, 100)
	s.Require().NoError(err)
	s.Require().Len(profiled, len(columnsToProfile))

	s.Equal("amount", profiled[0].AliasPrefix)
	s.Equal("sku_id", profiled[1].AliasPrefix)
	s.Equal("sku_id_2", profiled[2].AliasPrefix)
	s.Equal("sku_id_3", profiled[3].AliasPrefix)

	seen := map[string]struct{}{}
	for i, pc := range profiled {
		s.Equal(columnsToProfile[i].Column, pc.Column, "original column preserved in mapping")
		_, dup := seen[pc.AliasPrefix]
		s.Falsef(dup, "alias prefix %q repeated", pc.AliasPrefix)
		seen[pc.AliasPrefix] = struct{}{}
	}

	// Proof the reported mapping matches the SQL actually generated: every
	// metric output column is named "<AliasPrefix>__<metric>", so each prefix
	// must appear in the query, and the lossy dotted/literal collision must not.
	sql, err := qb.ToSql(dialect)
	s.Require().NoError(err)
	for _, pc := range profiled {
		s.Containsf(sql, pc.AliasPrefix+"__", "alias prefix %q for column %q missing from SQL", pc.AliasPrefix, pc.Column)
	}
	s.NotContains(sql, "sku.id__", "dotted path must not leak into an output column alias")
}
