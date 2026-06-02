package sqldialect

import (
	"testing"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/suite"
)

type CrossJoinSuite struct {
	suite.Suite
}

func TestCrossJoinSuite(t *testing.T) {
	suite.Run(t, new(CrossJoinSuite))
}

// TestCrossJoinExpr renders a bare CROSS JOIN clause against a table.
func (s *CrossJoinSuite) TestCrossJoinExpr() {
	for _, dialect := range DialectsToTest() {
		expr := CrossJoin(SubqueryTable("SELECT COUNT(*) AS cnt FROM t", "_recon_tgt"))
		sql, err := expr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("CrossJoinExpr"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

// TestSelectCrossJoinSubqueries combines two single-row subqueries into one
// statement via CROSS JOIN — the shape used to fuse a source/target comparison
// into a single round-trip when both run on the same connection.
func (s *CrossJoinSuite) TestSelectCrossJoinSubqueries() {
	for _, dialect := range DialectsToTest() {
		sel := NewSelect().
			From(SubqueryTable("SELECT COUNT(*) AS cnt FROM users", "_recon_src")).
			CrossJoin(SubqueryTable("SELECT COUNT(*) AS cnt FROM orders", "_recon_tgt")).
			Cols(
				As(Sql("_recon_src.cnt"), Identifier("source_cnt")),
				As(Sql("_recon_tgt.cnt"), Identifier("target_cnt")),
			)

		sql, err := sel.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("SelectCrossJoinSubqueries"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

// TestSelectMixedJoins verifies a CROSS JOIN and a conditional JOIN can coexist
// on the same Select (exercising the JoinTableExpr slice).
func (s *CrossJoinSuite) TestSelectMixedJoins() {
	for _, dialect := range DialectsToTest() {
		sel := NewSelect().
			From(Sql("a")).
			CrossJoin(Sql("b")).
			Join(Sql("c"), On(Eq(Sql("a.id"), Sql("c.id")))).
			Cols(Sql("*"))

		sql, err := sel.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("SelectMixedJoins"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}
