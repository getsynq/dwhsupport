package sqldialect

import (
	"testing"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/suite"
)

type RowCompareSuite struct {
	suite.Suite
}

func TestRowCompareSuite(t *testing.T) {
	suite.Run(t, new(RowCompareSuite))
}

// Composite half-open lower bound: (workspace, path) >= ('ws', 'p').
func (s *RowCompareSuite) TestCompositeGte() {
	for _, dialect := range DialectsToTest() {
		expr := RowCompare(
			[]Expr{Sql("workspace"), Sql("path")},
			COMPARE_GTE,
			[]Expr{String("ws"), String("p")},
		)
		sql, err := expr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(dialect.Name, "=>", sql)
		snaps.WithConfig(snaps.Dir("RowCompareCompositeGte"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

// Composite exclusive upper bound: (workspace, path) < ('ws', 'p').
func (s *RowCompareSuite) TestCompositeLt() {
	for _, dialect := range DialectsToTest() {
		expr := RowCompare(
			[]Expr{Sql("workspace"), Sql("path")},
			COMPARE_LT,
			[]Expr{String("ws"), String("p")},
		)
		sql, err := expr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		snaps.WithConfig(snaps.Dir("RowCompareCompositeLt"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

// Three-column key exercises the multi-level equality prefixes in the expansion.
func (s *RowCompareSuite) TestThreeColumnGt() {
	for _, dialect := range DialectsToTest() {
		expr := RowCompare(
			[]Expr{Sql("a"), Sql("b"), Sql("c")},
			COMPARE_GT,
			[]Expr{Int64(1), Int64(2), Int64(3)},
		)
		sql, err := expr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		snaps.WithConfig(snaps.Dir("RowCompareThreeColumnGt"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

// A single-column key must collapse to a plain scalar comparison in every
// dialect — no tuple syntax, no expansion.
func (s *RowCompareSuite) TestSingleColumnIsScalar() {
	for _, dialect := range DialectsToTest() {
		expr := RowCompare([]Expr{Sql("id")}, COMPARE_GTE, []Expr{Int64(42)})
		sql, err := expr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().Equal("id >= 42", sql)
	}
}

// A mismatched column/value arity is a correctness trap (a dropped component
// silently broadens the predicate), so it must error rather than truncate.
func (s *RowCompareSuite) TestMismatchedTupleLengthsError() {
	expr := RowCompare(
		[]Expr{Sql("a"), Sql("b"), Sql("c")},
		COMPARE_GTE,
		[]Expr{Int64(1), Int64(2)},
	)
	for _, dialect := range DialectsToTest() {
		_, err := expr.ToSql(dialect.Dialect)
		s.Require().Error(err, dialect.Name)
		s.Require().Contains(err.Error(), "mismatched tuple lengths")
	}
}

// Postgres opts into the native row-value form; ClickHouse keeps the expansion
// (it prunes its primary key from the OR form, not from a native tuple).
func (s *RowCompareSuite) TestNativeVsExpansionSelection() {
	expr := RowCompare(
		[]Expr{Sql("workspace"), Sql("path")},
		COMPARE_GTE,
		[]Expr{String("ws"), String("p")},
	)

	pg, err := expr.ToSql(NewPostgresDialect())
	s.Require().NoError(err)
	s.Require().Equal(`(workspace, path) >= ('ws', 'p')`, pg)

	ch, err := expr.ToSql(NewClickHouseDialect())
	s.Require().NoError(err)
	s.Require().Contains(ch, " or ")
	s.Require().NotContains(ch, `(workspace, path) >=`)
}
