package sqldialect

import (
	"testing"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/suite"
)

type CaseExprSuite struct {
	suite.Suite
}

func TestCaseExprSuite(t *testing.T) {
	suite.Run(t, new(CaseExprSuite))
}

func (s *CaseExprSuite) TestSimpleCaseWithElse() {
	// CASE WHEN col > 10 THEN 1 ELSE 0 END
	for _, dialect := range DialectsToTest() {
		caseExpr := Case().
			When(condSql("col > 10"), Int64(1)).
			Else(Int64(0))

		sql, err := caseExpr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("SimpleCaseWithElse"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *CaseExprSuite) TestMultipleWhenClauses() {
	// CASE WHEN col < 10 THEN 'small' WHEN col < 100 THEN 'medium' ELSE 'large' END
	for _, dialect := range DialectsToTest() {
		caseExpr := Case().
			When(condSql("col < 10"), String("small")).
			When(condSql("col < 100"), String("medium")).
			Else(String("large"))

		sql, err := caseExpr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("MultipleWhenClauses"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *CaseExprSuite) TestCaseWithoutElse() {
	// CASE WHEN col > 10 THEN 1 END (no ELSE, returns NULL for non-matching rows)
	for _, dialect := range DialectsToTest() {
		caseExpr := Case().
			When(condSql("col > 10"), Int64(1))

		sql, err := caseExpr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("CaseWithoutElse"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *CaseExprSuite) TestCaseWithComplexExpressions() {
	// CASE WHEN id >= 1 AND id < 100 THEN 0 WHEN id >= 100 AND id < 200 THEN 1 END
	for _, dialect := range DialectsToTest() {
		caseExpr := Case().
			When(condSql("id >= 1 AND id < 100"), Int64(0)).
			When(condSql("id >= 100 AND id < 200"), Int64(1))

		sql, err := caseExpr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("CaseWithComplexExpressions"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *CaseExprSuite) TestCaseAsNumericExpression() {
	// Verify CaseExpr implements NumericExpr and can be used in SUM
	for _, dialect := range DialectsToTest() {
		caseExpr := Case().
			When(condSql("col > 10"), Int64(1)).
			Else(Int64(0))

		// Should be usable in aggregations
		sumExpr := Fn("SUM", caseExpr)

		sql, err := sumExpr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("CaseAsNumericExpression"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *CaseExprSuite) TestCaseInColumnList() {
	for _, dialect := range DialectsToTest() {
		caseExpr := Case().
			When(condSql("amount > 100"), String("high")).
			When(condSql("amount > 10"), String("medium")).
			Else(String("low"))

		sel := NewSelect().
			From(tableSql("orders")).
			Cols(
				Sql("id"),
				As(caseExpr, Sql("category")),
			)

		sql, err := sel.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("CaseInColumnList"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *CaseExprSuite) TestCaseInGroupBy() {
	for _, dialect := range DialectsToTest() {
		bucketExpr := Case().
			When(condSql("id < 100"), Int64(0)).
			Else(Int64(1))

		sel := NewSelect().
			From(tableSql("data")).
			Cols(
				As(bucketExpr, Sql("bucket")),
				As(Fn("COUNT", Sql("*")), Sql("cnt")),
			).
			GroupBy(Sql("bucket"))

		sql, err := sel.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("CaseInGroupBy"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}
