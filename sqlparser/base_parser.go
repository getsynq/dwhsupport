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

func (p *BaseParser) ParseIdentifier() (string, error) {

	hadString := false
	var value strings.Builder
	for {
		nextInd, nextToken := p.PeekToken()
		if IsIdentifierLikeToken(nextToken) {
			hadString = true
			value.WriteString(nextToken.Value)
		} else {
			break
		}
		p.Index = nextInd
	}

	if !hadString {
		_, nextToken := p.PeekToken()
		return "", errors.Errorf("expected string, got %s", DumpToken(nextToken))
	}

	return value.String(), nil

}
