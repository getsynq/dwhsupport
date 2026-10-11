package sqldialect

import (
	"fmt"
	"strings"
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

// engineConcatWsArgumentLimits is the most arguments one CONCAT_WS call takes
// on each engine that has a limit, the separator included, as measured on the
// engines: Postgres refuses more than 100 arguments in any call, SQL Server and
// Fabric take at most 254 in a CONCAT_WS, Trino and Athena 127.
var engineConcatWsArgumentLimits = map[string]int{
	"postgres": 100,
	"mssql":    254,
	"fabric":   254,
	"trino":    127,
}

// TestConcatWsStaysWithinEachEngineArgumentLimit renders concatenations around
// and far past each engine's limit and checks that no CONCAT_WS call in the
// result passes more arguments than that engine accepts.
func (s *ConcatSuite) TestConcatWsStaysWithinEachEngineArgumentLimit() {
	for _, dialect := range DialectsToTest() {
		limit, ok := engineConcatWsArgumentLimits[dialect.Name]
		if !ok {
			continue
		}
		s.Run(dialect.Name, func() {
			for _, n := range []int{limit - 2, limit - 1, limit, limit + 1, 2*limit + 5, 1100, 20000} {
				sql, err := ConcatWs("|", numberedColumns(n)...).ToSql(dialect.Dialect)
				s.Require().NoError(err)

				calls := concatWsArgumentCounts(sql)
				s.Require().NotEmpty(calls, "n=%d", n)
				for _, args := range calls {
					s.LessOrEqual(args, limit, "n=%d: a CONCAT_WS call passes %d arguments", n, args)
					// SQL Server refuses a CONCAT_WS with fewer than three.
					s.GreaterOrEqual(args, 3, "n=%d: a CONCAT_WS call passes %d arguments", n, args)
				}
			}
		})
	}
}

// TestConcatWsWithinTheLimitIsOneCall pins that a concatenation the engine
// accepts as one call is still rendered as one call, so the SQL of every
// caller that already worked is unchanged.
func (s *ConcatSuite) TestConcatWsWithinTheLimitIsOneCall() {
	for _, dialect := range DialectsToTest() {
		limit, ok := engineConcatWsArgumentLimits[dialect.Name]
		if !ok {
			limit = 1101
		}
		sql, err := ConcatWs("|", numberedColumns(limit-1)...).ToSql(dialect.Dialect)
		s.Require().NoError(err)
		s.LessOrEqual(len(concatWsArgumentCounts(sql)), 1, dialect.Name)
	}
}

// TestConcatWsWide shows the shape a concatenation past every limit takes on
// each engine.
func (s *ConcatSuite) TestConcatWsWide() {
	for _, dialect := range DialectsToTest() {
		sql, err := ConcatWs("|", numberedColumns(300)...).ToSql(dialect.Dialect)
		s.Require().NoError(err)
		snaps.WithConfig(snaps.Dir("ConcatWsWide"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
	}
}

// TestConcatWsOnRedshiftIsNotAFunctionCall pins that Redshift gets no
// CONCAT_WS: the engine has none, only a two-argument CONCAT, so a call to it
// fails with "function concat_ws(...) does not exist" whatever the arguments.
func (s *ConcatSuite) TestConcatWsOnRedshiftIsNotAFunctionCall() {
	redshift := NewRedshiftDialect()
	for _, n := range []int{2, 3, 99, 100, 1100} {
		sql, err := ConcatWs("|", numberedColumns(n)...).ToSql(redshift)
		s.Require().NoError(err)
		s.NotContains(strings.ToLower(sql), "concat", "n=%d", n)
	}
}

// TestOracleMd5OfLongText pins the SQL of the Oracle digest: a ConcatWs is
// measured, then hashed as a VARCHAR2 or as a CLOB, and anything else goes to
// STANDARD_HASH alone.
func (s *ConcatSuite) TestOracleMd5OfLongText() {
	oracle := NewOracleDialect()
	for name, text := range map[string]Expr{
		"concat_ws":  ConcatWs("|", Sql("col1"), Sql("col2"), Sql("col3")),
		"single_col": ConcatWs("|", Sql("col1")),
		"plain_expr": Sql("col1"),
	} {
		sql, err := OracleMd5OfLongText(text).ToSql(oracle)
		s.Require().NoError(err)
		snaps.WithConfig(snaps.Dir("OracleMd5OfLongText"), snaps.Filename(name)).MatchSnapshot(s.T(), sql)
	}
}

func numberedColumns(n int) []Expr {
	exprs := make([]Expr, n)
	for i := range exprs {
		exprs[i] = Sql(fmt.Sprintf("c%d", i))
	}
	return exprs
}

// concatWsArgumentCounts returns how many arguments each CONCAT_WS call in sql
// passes, innermost first. String literals are skipped, so a separator holding
// a comma or a parenthesis is not miscounted.
func concatWsArgumentCounts(sql string) []int {
	type call struct {
		concat bool
		args   int
	}
	var (
		open   []call
		counts []int
	)
	for i := 0; i < len(sql); i++ {
		switch sql[i] {
		case '\'':
			for i++; i < len(sql); i++ {
				if sql[i] != '\'' {
					continue
				}
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++
					continue
				}
				break
			}
		case '(':
			name := strings.ToLower(strings.TrimRight(sql[:i], " \t\n"))
			open = append(open, call{concat: strings.HasSuffix(name, "concat_ws"), args: 1})
		case ',':
			if len(open) > 0 {
				open[len(open)-1].args++
			}
		case ')':
			last := open[len(open)-1]
			open = open[:len(open)-1]
			if last.concat {
				counts = append(counts, last.args)
			}
		}
	}
	return counts
}
