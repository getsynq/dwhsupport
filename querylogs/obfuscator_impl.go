package querylogs

// This file is forked from github.com/DataDog/go-sqllexer
// Original source: https://github.com/DataDog/go-sqllexer/blob/main/obfuscator.go
// License: MIT
//
// Forked to fix issue with ReplaceDigits behavior that incorrectly replaces
// digits in identifiers (e.g., table1 -> table?).
//
// TODO: Contribute fix back to upstream and remove this fork once merged.

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/DataDog/go-sqllexer"
)

type obfuscatorConfig struct {
	PreserveNumbers            bool     `json:"preserve_numbers"`            // if true, keep numeric literals (123, 45.67) unchanged
	ReplacePositionalParameter bool     `json:"replace_positional_parameter"` // PostgreSQL $1, $2 style parameters
	ReplaceBoolean             bool     `json:"replace_boolean"`             // true/false literals
	ReplaceNull                bool     `json:"replace_null"`                // NULL literals
	KeepJsonPath               bool     `json:"keep_json_path"`              // preserve JSON path expressions like $.field
	ReplaceBindParameter       bool     `json:"replace_bind_parameter"`      // Oracle :name style parameters
	PreserveLiteralPatterns    []string `json:"preserve_literal_patterns"`   // regex patterns for literals to preserve unchanged (e.g., dates, UUIDs)
}

type sqlObfuscator struct {
	config         *obfuscatorConfig
	whitelistRegex *regexp.Regexp // combined regex from whitelist patterns, compiled once for efficiency
}

func newSqlObfuscator(opts ...ObfuscatorOption) (*sqlObfuscator, error) {
	obfuscator := &sqlObfuscator{
		config: &obfuscatorConfig{},
	}

	for _, opt := range opts {
		opt(obfuscator.config)
	}

	// Compile preserve literal patterns into a single efficient regex
	if len(obfuscator.config.PreserveLiteralPatterns) > 0 {
		// Combine all patterns with | (OR)
		// Wrap each pattern in a non-capturing group to ensure proper precedence
		var combinedPattern strings.Builder
		for i, pattern := range obfuscator.config.PreserveLiteralPatterns {
			if i > 0 {
				combinedPattern.WriteString("|")
			}
			// Wrap pattern in non-capturing group
			combinedPattern.WriteString("(?:")
			combinedPattern.WriteString(pattern)
			combinedPattern.WriteString(")")
		}

		// Compile the combined regex - return error if invalid
		compiled, err := regexp.Compile(combinedPattern.String())
		if err != nil {
			return nil, fmt.Errorf("failed to compile preserve literal patterns: %w", err)
		}
		obfuscator.whitelistRegex = compiled
	}

	return obfuscator, nil
}

const (
	StringPlaceholder = "?"
	NumberPlaceholder = "?"
)

// Obfuscate takes an input SQL string and returns an obfuscated SQL string.
// The obfuscator replaces all literal values with a single placeholder
func (o *sqlObfuscator) Obfuscate(input string) string {
	var obfuscatedSQL strings.Builder
	obfuscatedSQL.Grow(len(input))

	lexer := sqllexer.New(input)

	var lastValueToken *lastValueToken

	for {
		token := lexer.Scan()
		if token.Type == sqllexer.EOF {
			break
		}
		o.obfuscateTokenValue(token, lastValueToken)
		obfuscatedSQL.WriteString(token.Value)
		if isValueToken(token) {
			lastValueToken = getLastValueToken(token)
		}
	}

	return strings.TrimSpace(obfuscatedSQL.String())
}

func (o *sqlObfuscator) obfuscateTokenValue(token *sqllexer.Token, lastValueToken *lastValueToken) {
	switch token.Type {
	case sqllexer.NUMBER:
		if o.config.KeepJsonPath && lastValueToken != nil && lastValueToken.Type == sqllexer.JSON_OP {
			break
		}
		// Check if numbers should be preserved
		if o.config.PreserveNumbers {
			break
		}
		// Check if this number matches preserve literal patterns (e.g., dates like 2023-10-01)
		if o.isWhitelisted(token.Value) {
			break
		}
		token.Value = NumberPlaceholder
	case sqllexer.STRING, sqllexer.INCOMPLETE_STRING, sqllexer.DOLLAR_QUOTED_STRING, sqllexer.DOLLAR_QUOTED_FUNCTION:
		if o.config.KeepJsonPath && lastValueToken != nil && lastValueToken.Type == sqllexer.JSON_OP {
			break
		}
		// For strings, we need to extract the content without quotes
		// to check against whitelist patterns
		content := o.extractStringContent(token.Value, token.Type)
		if o.isWhitelisted(content) {
			break
		}
		token.Value = StringPlaceholder
	case sqllexer.POSITIONAL_PARAMETER:
		if o.config.ReplacePositionalParameter {
			token.Value = StringPlaceholder
		}
	case sqllexer.BIND_PARAMETER:
		if o.config.ReplaceBindParameter {
			token.Value = StringPlaceholder
		}
	case sqllexer.BOOLEAN:
		if o.config.ReplaceBoolean {
			token.Value = StringPlaceholder
		}
	case sqllexer.NULL:
		if o.config.ReplaceNull {
			token.Value = StringPlaceholder
		}
		// NOTE: We intentionally DO NOT replace digits in IDENT or QUOTED_IDENT tokens.
		// The ReplaceDigits option should only affect numeric literals (NUMBER tokens),
		// not identifiers like table1, col2, etc.
		// This was the bug in the upstream sqllexer that we forked to fix.
	}
}

// isWhitelisted checks if a value matches any whitelist pattern
func (o *sqlObfuscator) isWhitelisted(value string) bool {
	if o.whitelistRegex == nil {
		return false
	}
	return o.whitelistRegex.MatchString(value)
}

// extractStringContent removes quotes from string tokens to get the actual content
func (o *sqlObfuscator) extractStringContent(value string, tokenType sqllexer.TokenType) string {
	switch tokenType {
	case sqllexer.STRING, sqllexer.INCOMPLETE_STRING:
		// Remove single quotes: 'content' -> content
		if len(value) >= 2 && value[0] == '\'' {
			if value[len(value)-1] == '\'' {
				return value[1 : len(value)-1]
			}
			// INCOMPLETE_STRING might not have closing quote
			return value[1:]
		}
		return value
	case sqllexer.DOLLAR_QUOTED_STRING:
		// Remove dollar quotes: $tag$content$tag$ -> content
		// Find the first $...$ tag
		firstDollarEnd := strings.Index(value[1:], "$")
		if firstDollarEnd > 0 {
			tagLen := firstDollarEnd + 2 // +2 for the two $ characters
			if len(value) >= 2*tagLen {
				return value[tagLen : len(value)-tagLen]
			}
		}
		return value
	default:
		return value
	}
}

// lastValueToken is a simplified version of sqllexer.Token for tracking
type lastValueToken struct {
	Type sqllexer.TokenType
}

// getLastValueToken extracts relevant info from a token
func getLastValueToken(t *sqllexer.Token) *lastValueToken {
	return &lastValueToken{
		Type: t.Type,
	}
}

// isValueToken checks if a token is a value token
// A value token is a token that is not a space, comment, or EOF
func isValueToken(token *sqllexer.Token) bool {
	return token.Type != sqllexer.EOF &&
		token.Type != sqllexer.SPACE &&
		token.Type != sqllexer.COMMENT &&
		token.Type != sqllexer.MULTILINE_COMMENT
}
