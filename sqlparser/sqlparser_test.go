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
