package sqldialect

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type CondSuite struct {
	suite.Suite
}

func TestCondSuite(t *testing.T) {
	suite.Run(t, new(CondSuite))
}

// pg is an arbitrary dialect; And/Or rendering is dialect-independent (the
// operands resolve per dialect, the boolean joining does not).
func (s *CondSuite) sql(expr Expr) string {
	out, err := expr.ToSql(NewPostgresDialect())
	s.Require().NoError(err)
	return out
}

func (s *CondSuite) TestAndFlatNoOuterParens() {
	expr := And(
		Gte(Sql("id"), Int64(1)),
		Lt(Sql("id"), Int64(26)),
	)
	s.Equal("id >= 1 and id < 26", s.sql(expr))
}

func (s *CondSuite) TestAndSingleCondition() {
	s.Equal("id >= 1", s.sql(And(Gte(Sql("id"), Int64(1)))))
}

func (s *CondSuite) TestAndEmpty() {
	s.Equal("", s.sql(And()))
}

// AND binds tighter than OR, so a flat And nested in Or stays correct without
// extra parentheses around the And; the Or supplies its own.
func (s *CondSuite) TestAndNestedInOr() {
	expr := Or(
		And(Gte(Sql("id"), Int64(1)), Lt(Sql("id"), Int64(26))),
		Eq(Sql("id"), Int64(99)),
	)
	s.Equal("(id >= 1 and id < 26 or id = 99)", s.sql(expr))
}

// Or parenthesizes itself, so an Or nested inside And is well-formed.
func (s *CondSuite) TestOrNestedInAnd() {
	expr := And(
		Or(Eq(Sql("a"), Int64(1)), Eq(Sql("a"), Int64(2))),
		Eq(Sql("b"), Int64(3)),
	)
	s.Equal("(a = 1 or a = 2) and b = 3", s.sql(expr))
}
