package sqlparser

import (
	"strings"

	"github.com/DataDog/go-sqllexer"
	"github.com/pkg/errors"
)

type BaseParser struct {
	Tokens []*sqllexer.Token
	Index  int
}

func (p *BaseParser) PeekToken() (int, *sqllexer.Token) {
	ind := p.Index
	for ind < len(p.Tokens) {
		switch p.Tokens[ind].Type {
		case sqllexer.SPACE, sqllexer.COMMENT, sqllexer.MULTILINE_COMMENT:
			ind++
			continue
		default:
			return ind + 1, p.Tokens[ind]
		}
	}
	return ind + 1, &sqllexer.Token{Type: sqllexer.EOF}
}

func (p *BaseParser) ParseToken(token sqllexer.Token) bool {
	ind, tok := p.PeekToken()
	if tok.Type == token.Type && strings.ToLower(tok.Value) == strings.ToLower(token.Value) {
		p.Index = ind
		return true
	}
	return false
}

func (p *BaseParser) ExpectToken(token sqllexer.Token) error {
	nextInd, nextToken := p.PeekToken()
	if nextToken.Type != token.Type || strings.ToLower(nextToken.Value) != strings.ToLower(token.Value) {
		return errors.Errorf("expected token %v, got %v", token, nextToken)
	}
	p.Index = nextInd
	return nil
}

// ParseIdentifier reads the name at the parser's position: the next
// identifier-like token, joined with the identifier-like tokens that follow it
// directly. A space or a comment ends the name, so the words after it
// (`PRODUCTS cluster by (...)`, `STREAM on table T`) are not read into it,
// except inside a quoted part: the lexer splits `DB."my schema".T` at the
// space when the name does not start with the quote.
func (p *BaseParser) ParseIdentifier() (string, error) {
	nextInd, nextToken := p.PeekToken()
	if !IsIdentifierLikeToken(nextToken) {
		return "", errors.Errorf("expected string, got %s", DumpToken(nextToken))
	}

	var value strings.Builder
	value.WriteString(nextToken.Value)
	p.Index = nextInd
	for p.Index < len(p.Tokens) {
		token := p.Tokens[p.Index]
		insideQuotes := strings.Count(value.String(), `"`)%2 == 1
		if !insideQuotes && !IsIdentifierLikeToken(token) {
			break
		}
		value.WriteString(token.Value)
		p.Index++
	}

	return value.String(), nil
}
