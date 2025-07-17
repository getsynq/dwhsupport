package sqldialect

import (
	"unicode"

	"github.com/lib/pq"
)

func PqQuoteIdentifierIfUpper(identifier string) string {
	if identifier == "" || IsLower(identifier) {
		return identifier
	}
	return pq.QuoteIdentifier(identifier)
}

func IsLower(s string) bool {
	for _, r := range s {
		if !unicode.IsLower(r) && unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

func IsUpper(s string) bool {
	for _, r := range s {
		if !unicode.IsUpper(r) && unicode.IsLetter(r) {
			return false
		}
	}
	return true
}
