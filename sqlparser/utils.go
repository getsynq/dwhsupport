package sqlparser

import (
	"fmt"
	"strings"

	"github.com/DataDog/go-sqllexer"
)

func ScanAllTokens(lexer *sqllexer.Lexer) []*sqllexer.Token {
	var tokens []*sqllexer.Token
	for {
		t := lexer.Scan()
		if t == nil {
			break
		}
		tval := *t
		if t.Type == sqllexer.EOF {
			break
		} else {
			tokens = append(tokens, &tval)
		}
	}
	return tokens
}

func SplitTokensIntoStatements(tokens []*sqllexer.Token) [][]*sqllexer.Token {
	var groups [][]*sqllexer.Token
	var startOfGroup = 0

	fp := &BaseParser{Tokens: tokens}

	for {
		nextInd, nextToken := fp.PeekToken()
		if nextToken.Type == sqllexer.EOF {
			group := fp.Tokens[startOfGroup:]
			if len(group) > 0 {
				groups = append(groups, group)
			}
			break
		}
		if nextToken.Type == sqllexer.PUNCTUATION && nextToken.Value == ";" {
			group := fp.Tokens[startOfGroup:nextInd]
			if len(group) > 0 {
				groups = append(groups, group)
			}
			fp.Index = nextInd

			startOfGroup = nextInd
			fp.Index = nextInd + 1
		}
		fp.Index = nextInd
	}

	return groups
}

func PrintTokens(tokens []*sqllexer.Token) string {
	var sb strings.Builder
	for i := 0; i < len(tokens); i++ {
		sb.WriteString(tokens[i].Value)
	}
	return strings.TrimSpace(sb.String())
}

func TokenName(tokenType sqllexer.TokenType) string {
	switch tokenType {
	case sqllexer.ERROR:
		return "ERROR"
	case sqllexer.EOF:
		return "EOF"
	case sqllexer.SPACE:
		return "SPACE"
	case sqllexer.STRING:
		return "STRING"
	case sqllexer.INCOMPLETE_STRING:
		return "INCOMPLETE_STRING"
	case sqllexer.NUMBER:
		return "NUMBER"
	case sqllexer.IDENT:
		return "IDENT"
	case sqllexer.QUOTED_IDENT:
		return "QUOTED_IDENT"
	case sqllexer.OPERATOR:
		return "OPERATOR"
	case sqllexer.WILDCARD:
		return "WILDCARD"
	case sqllexer.COMMENT:
		return "COMMENT"
	case sqllexer.MULTILINE_COMMENT:
		return "MULTILINE_COMMENT"
	case sqllexer.PUNCTUATION:
		return "PUNCTUATION"
	case sqllexer.DOLLAR_QUOTED_FUNCTION:
		return "DOLLAR_QUOTED_FUNCTION"
	case sqllexer.DOLLAR_QUOTED_STRING:
		return "DOLLAR_QUOTED_STRING"
	case sqllexer.POSITIONAL_PARAMETER:
		return "POSITIONAL_PARAMETER"
	case sqllexer.BIND_PARAMETER:
		return "BIND_PARAMETER"
	case sqllexer.FUNCTION:
		return "FUNCTION"
	case sqllexer.SYSTEM_VARIABLE:
		return "SYSTEM_VARIABLE"
	case sqllexer.UNKNOWN:
		return "UNKNOWN"
	case sqllexer.COMMAND:
		return "COMMAND"
	case sqllexer.KEYWORD:
		return "KEYWORD"
	case sqllexer.JSON_OP:
		return "JSON_OP"
	case sqllexer.BOOLEAN:
		return "BOOLEAN"
	case sqllexer.NULL:
		return "NULL"
	case sqllexer.PROC_INDICATOR:
		return "PROC_INDICATOR"
	case sqllexer.CTE_INDICATOR:
		return "CTE_INDICATOR"
	case sqllexer.ALIAS_INDICATOR:
		return "ALIAS_INDICATOR"
	default:
		panic(fmt.Sprintf("unknown token type %+v", tokenType))
	}
}

func DumpToken(token *sqllexer.Token) string {
	return fmt.Sprintf("Token{Type: %s, Value: '%s'}", TokenName(token.Type), token.Value)
}

func IsIdentifierLikeToken(t *sqllexer.Token) bool {
	return t.Type == sqllexer.STRING ||
		t.Type == sqllexer.IDENT ||
		t.Type == sqllexer.KEYWORD ||
		t.Type == sqllexer.QUOTED_IDENT ||
		t.Type == sqllexer.FUNCTION || t.Type == sqllexer.BIND_PARAMETER
}
