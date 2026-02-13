package sqldialect

import (
	"testing"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/suite"
)

// Helper to create CondExpr from raw SQL for tests
func condSql(sql string) CondExpr {
	s := Sql(sql)
	return s
}

// Helper to create TableExpr from raw SQL for tests
func tableSql(sql string) TableExpr {
	s := Sql(sql)
	return s
}

type SubquerySuite struct {
	suite.Suite
}

func TestSubquerySuite(t *testing.T) {
	suite.Run(t, new(SubquerySuite))
}

func (s *SubquerySuite) TestSimpleSubquery() {
	subquery := "SELECT * FROM users WHERE active = true"

	for _, dialect := range DialectsToTest() {
		subqTable := SubqueryTable(subquery, "active_users")
		sql, err := subqTable.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("SimpleSubquery"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *SubquerySuite) TestSubqueryWithNewlines() {
	subquery := `SELECT id, name
FROM users
WHERE created_at > '2024-01-01'`

	for _, dialect := range DialectsToTest() {
		subqTable := SubqueryTable(subquery, "recent_users")
		sql, err := subqTable.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("SubqueryWithNewlines"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *SubquerySuite) TestSubqueryInSelectFrom() {
	subquery := "SELECT id, amount FROM orders"

	for _, dialect := range DialectsToTest() {
		subqTable := SubqueryTable(subquery, "order_data")

		sel := NewSelect().
			From(subqTable).
			Cols(
				As(Fn("COUNT", Sql("*")), Sql("cnt")),
				As(Fn("SUM", Sql("amount")), Sql("total")),
			)

		sql, err := sel.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("SubqueryInSelectFrom"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *SubquerySuite) TestNestedSubqueries() {
	// Create nested query as string (simulating what would be done in practice)
	middleStr := "SELECT id FROM (\nSELECT * FROM raw_data\n) AS filtered WHERE id > 100"

	for _, dialect := range DialectsToTest() {
		outerSubq := SubqueryTable(middleStr, "final")
		sql, err := outerSubq.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("NestedSubqueries"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *SubquerySuite) TestSubqueryWithSQLComments() {
	// SQL comments should be preserved in subquery
	subquery := `SELECT id, name FROM users -- active users only
WHERE active = true`

	for _, dialect := range DialectsToTest() {
		subqTable := SubqueryTable(subquery, "users_active")
		sql, err := subqTable.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("SubqueryWithSQLComments"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *SubquerySuite) TestSubqueryWithWhere() {
	subquery := "SELECT id, amount FROM transactions"

	for _, dialect := range DialectsToTest() {
		subqTable := SubqueryTable(subquery, "txn")

		sel := NewSelect().
			From(subqTable).
			Cols(Sql("id"), Sql("amount")).
			Where(condSql("amount > 100"))

		sql, err := sel.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("SubqueryWithWhere"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *SubquerySuite) TestSubqueryWithGroupBy() {
	subquery := "SELECT id, name, value FROM data"

	for _, dialect := range DialectsToTest() {
		subqTable := SubqueryTable(subquery, "_recon_base")

		caseExpr := Case().
			When(condSql("id < 100"), Int64(0)).
			Else(Int64(1))

		sel := NewSelect().
			From(subqTable).
			Cols(
				As(caseExpr, Sql("bucket")),
				As(Fn("COUNT", Sql("*")), Sql("cnt")),
			).
			GroupBy(Sql("bucket")).
			OrderBy(Asc(Sql("bucket")))

		sql, err := sel.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("SubqueryWithGroupBy"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *SubquerySuite) TestSumHelper() {
	for _, dialect := range DialectsToTest() {
		sumExpr := Sum(Sql("amount"))

		sql, err := sumExpr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("SumHelper"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *SubquerySuite) TestSumWithCase() {
	for _, dialect := range DialectsToTest() {
		caseExpr := Case().
			When(condSql("status = 'active'"), Sql("amount")).
			Else(Int64(0))

		sumExpr := Sum(caseExpr)

		sql, err := sumExpr.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("SumWithCase"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *SubquerySuite) TestSumInSelect() {
	for _, dialect := range DialectsToTest() {
		sel := NewSelect().
			From(tableSql("orders")).
			Cols(
				As(Sum(Sql("amount")), Sql("total_amount")),
			)

		sql, err := sel.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("SumInSelect"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

func (s *SubquerySuite) TestReconciliationPattern() {
	// This mimics the pattern used in synq-recon:
	// SELECT bucket, COUNT(*), SUM(checksum) FROM (baseQuery) AS _recon_base WHERE ... GROUP BY bucket

	baseQuery := "SELECT id, name, value FROM test_data"

	for _, dialect := range DialectsToTest() {
		subqTable := SubqueryTable(baseQuery, "_recon_base")

		// Bucket assignment using CASE
		bucketExpr := Case().
			When(condSql("id >= 1 AND id < 50"), Int64(0)).
			When(condSql("id >= 50 AND id < 100"), Int64(1)).
			Else(Int64(2))

		// Row checksum using concat_ws
		checksumInner := dialect.Dialect.ConcatWithSeparator("|",
			Coalesce(dialect.Dialect.ToString(Sql("id")), String("<NULL>")),
			Coalesce(dialect.Dialect.ToString(Sql("name")), String("<NULL>")),
			Coalesce(dialect.Dialect.ToString(Sql("value")), String("<NULL>")),
		)
		checksumInnerSql, _ := checksumInner.ToSql(dialect.Dialect)

		// Wrap in MD5 (simplified - real version has offset subtraction)
		checksumExpr := Sql("MD5(" + checksumInnerSql + ")")

		sel := NewSelect().
			From(subqTable).
			Cols(
				As(bucketExpr, Sql("bucket")),
				As(Fn("COUNT", Sql("*")), Sql("cnt")),
				As(Sum(checksumExpr), Sql("checksum")),
			).
			Where(condSql("id >= 1 AND id < 100")).
			GroupBy(Sql("bucket")).
			OrderBy(Asc(Sql("bucket")))

		sql, err := sel.ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.Require().NotEmpty(sql)
		s.T().Log(sql)

		snaps.WithConfig(snaps.Dir("ReconciliationPattern"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}
