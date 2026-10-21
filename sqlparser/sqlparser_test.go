package sqlparser_test

import (
	"testing"

	"github.com/DataDog/go-sqllexer"
	"github.com/getsynq/dwhsupport/sqlparser"
	"github.com/stretchr/testify/suite"
)

type SqlParserSuite struct {
	suite.Suite
}

func TestSqlParserSuite(t *testing.T) {
	suite.Run(t, new(SqlParserSuite))
}

func (s *SqlParserSuite) TestScanAllTokens() {

	sql := `SELECT 1;SELECT 2; SELECT 3;`
	lexer := sqllexer.New(sql)
	tokens := sqlparser.ScanAllTokens(lexer)
	s.Equal(13, len(tokens))

	statements := sqlparser.SplitTokensIntoStatements(tokens)
	s.Equal(3, len(statements))

}

func (s *SqlParserSuite) TestScanAllTokensNoPunctuation() {

	sql := `SELECT 1;SELECT 2; SELECT 3`
	lexer := sqllexer.New(sql)
	tokens := sqlparser.ScanAllTokens(lexer)
	s.Equal(12, len(tokens))

	statements := sqlparser.SplitTokensIntoStatements(tokens)
	s.Equal(3, len(statements))

}

func (s *SqlParserSuite) TestScanAllTokensEmptyDangling() {

	sql := `SELECT 1;SELECT 2; SELECT 3; `
	lexer := sqllexer.New(sql)
	tokens := sqlparser.ScanAllTokens(lexer)
	s.Equal(14, len(tokens))

	statements := sqlparser.SplitTokensIntoStatements(tokens)
	s.Equal(4, len(statements))

}

func (s *SqlParserSuite) TestParseIdentifierEndsAtASpace() {
	cases := map[string]string{
		`DB.S.PRODUCTS cluster by (ID)`:     "DB.S.PRODUCTS",
		`DB.S.STREAM_1 on table T`:          "DB.S.STREAM_1",
		`DB."my schema"."T" (`:              `DB."my schema"."T"`,
		`"my db"."S"."T" cluster by (ID)`:   `"my db"."S"."T"`,
		`DB."a  ""b"" c".T cluster by (ID)`: `DB."a  ""b"" c".T`,
		`DB.S.V(`:                           "DB.S.V",
		`  /* c */ DB.S.MV COMMENT='x' AS`:  "DB.S.MV",
		"DB.S.T\n-- comment\n(ID NUMBER)":   "DB.S.T",
	}
	for sql, name := range cases {
		parser := sqlparser.BaseParser{Tokens: sqlparser.ScanAllTokens(sqllexer.New(sql, sqllexer.WithDBMS(sqllexer.DBMSSnowflake)))}
		got, err := parser.ParseIdentifier()
		s.Require().NoError(err, sql)
		s.Equal(name, got, sql)
	}
}

func (s *SqlParserSuite) TestParseIdentifierWithoutAName() {
	parser := sqlparser.BaseParser{Tokens: sqlparser.ScanAllTokens(sqllexer.New(`(ID)`))}
	_, err := parser.ParseIdentifier()
	s.Error(err)
	s.Equal(0, parser.Index, "nothing is consumed")
}
