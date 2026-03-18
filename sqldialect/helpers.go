package sqldialect

import (
	"fmt"
	"strings"
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

// StandardSQLStringLiteral implements the standard SQL string literal escaping
// where single quotes are escaped by doubling them (”).
// This is used by most SQL dialects: Snowflake, BigQuery, Postgres, Redshift,
// Databricks, DuckDB, Trino, and MySQL.
func StandardSQLStringLiteral(s string) string {
	escaped := strings.ReplaceAll(s, "'", "''")
	return fmt.Sprintf("'%s'", escaped)
}

// needsQuoting returns true if an identifier contains characters that require quoting.
// Safe unquoted identifiers consist only of ASCII letters, digits, and underscores,
// and must not start with a digit.
func needsQuoting(identifier string) bool {
	if identifier == "" {
		return true
	}
	for i, r := range identifier {
		if r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			continue
		}
		if r >= '0' && r <= '9' && i > 0 {
			continue
		}
		return true
	}
	return false
}

// QuoteWithDoubleQuotes quotes an identifier with double quotes (ANSI SQL standard).
// Used by Trino, Postgres, Redshift, DuckDB.
func QuoteWithDoubleQuotes(identifier string) string {
	if !needsQuoting(identifier) {
		return identifier
	}
	escaped := strings.ReplaceAll(identifier, `"`, `""`)
	return fmt.Sprintf(`"%s"`, escaped)
}

// QuoteWithBackticks quotes an identifier with backticks.
// Used by BigQuery, ClickHouse, MySQL.
func QuoteWithBackticks(identifier string) string {
	if !needsQuoting(identifier) {
		return identifier
	}
	escaped := strings.ReplaceAll(identifier, "`", "``")
	return fmt.Sprintf("`%s`", escaped)
}
