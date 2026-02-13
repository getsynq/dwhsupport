package sqldialect

import (
	"testing"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/suite"
)

type ConcatSuite struct {
	suite.Suite
}

func TestConcatSuite(t *testing.T) {
	suite.Run(t, new(ConcatSuite))
}

func (s *ConcatSuite) TestConcatWithSeparatorBasic() {
	for _, dialect := range DialectsToTest() {
		expr := dialect.Dialect.ConcatWithSeparator("|", Sql("col1"), Sql("col2"), Sql("col3"))
		sql, err := expr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("ConcatWithSeparatorBasic"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *ConcatSuite) TestConcatWithSeparatorTwoColumns() {
	for _, dialect := range DialectsToTest() {
		expr := dialect.Dialect.ConcatWithSeparator("|", Sql("col1"), Sql("col2"))
		sql, err := expr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("ConcatWithSeparatorTwoColumns"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *ConcatSuite) TestConcatWithSeparatorSingleColumn() {
	for _, dialect := range DialectsToTest() {
		expr := dialect.Dialect.ConcatWithSeparator("|", Sql("col1"))
		sql, err := expr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("ConcatWithSeparatorSingleColumn"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *ConcatSuite) TestConcatWithSeparatorEmpty() {
	for _, dialect := range DialectsToTest() {
		expr := dialect.Dialect.ConcatWithSeparator("|")
		sql, err := expr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("ConcatWithSeparatorEmpty"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *ConcatSuite) TestConcatWsHelper() {
	// Test the ConcatWs helper function delegates to dialect correctly
	for _, dialect := range DialectsToTest() {
		expr := ConcatWs("|", Sql("col1"), Sql("col2"), Sql("col3"))
		sql, err := expr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("ConcatWsHelper"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *ConcatSuite) TestConcatWsWithCoalesce() {
	// Common pattern: concat_ws('|', COALESCE(col1, 'NULL'), COALESCE(col2, 'NULL'))
	for _, dialect := range DialectsToTest() {
		expr := ConcatWs("|",
			Coalesce(Sql("col1"), String("<NULL>")),
			Coalesce(Sql("col2"), String("<NULL>")),
		)

		sql, err := expr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("ConcatWsWithCoalesce"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *ConcatSuite) TestConcatWsInSelect() {
	for _, dialect := range DialectsToTest() {
		expr := ConcatWs("|", Sql("col1"), Sql("col2"))

		// Should be usable where TextExpr is expected
		sel := NewSelect().
			From(tableSql("test_table")).
			Cols(As(expr, Sql("concatenated")))

		sql, err := sel.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("ConcatWsInSelect"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *ConcatSuite) TestConcatWithToString() {
	// Common pattern: concat_ws('|', CAST(id AS VARCHAR), CAST(amount AS VARCHAR))
	for _, dialect := range DialectsToTest() {
		expr := dialect.Dialect.ConcatWithSeparator("|",
			dialect.Dialect.ToString(Sql("id")),
			dialect.Dialect.ToString(Sql("amount")),
		)
		sql, err := expr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("ConcatWithToString"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *ConcatSuite) TestConcatWithEmptySeparator() {
	for _, dialect := range DialectsToTest() {
		expr := dialect.Dialect.ConcatWithSeparator("", Sql("col1"), Sql("col2"))
		sql, err := expr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("ConcatWithEmptySeparator"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *ConcatSuite) TestConcatWithMultiCharSeparator() {
	for _, dialect := range DialectsToTest() {
		expr := dialect.Dialect.ConcatWithSeparator(" | ", Sql("col1"), Sql("col2"))
		sql, err := expr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("ConcatWithMultiCharSeparator"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}
