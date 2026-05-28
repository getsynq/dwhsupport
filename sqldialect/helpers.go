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

// QuoteForFoldUpper quotes an identifier for fold-to-upper dialects
// (Snowflake, Oracle). Leaves pure-upper AND pure-lower identifiers
// unquoted (pure-upper matches catalog canonical case directly; pure-lower
// folds up to match). Quotes mixed-case and identifiers containing
// characters requiring quoting.
// MUST NOT be used for fold-to-lower dialects (Postgres, Redshift, Trino) —
// pure-upper input would slip through unquoted and fold to lower,
// missing the catalog entry. Use QuoteForFoldLower there.
// quoteChar must be a single character (e.g. `"`).
func QuoteForFoldUpper(identifier string, quoteChar string) string {
	if needsQuoting(identifier) || (!IsUpper(identifier) && !IsLower(identifier)) {
		escaped := strings.ReplaceAll(identifier, quoteChar, quoteChar+quoteChar)
		return quoteChar + escaped + quoteChar
	}
	return identifier
}

// QuoteForFoldLower quotes an identifier for fold-to-lower dialects
// (Postgres, Redshift, Trino). Leaves only pure-lower identifiers with no
// characters requiring quoting unquoted. Any uppercase letter or special
// char would otherwise fold or parse incorrectly.
// MUST NOT be used for fold-to-upper dialects (Snowflake, Oracle) —
// pure-upper input would be needlessly quoted. Use QuoteForFoldUpper there.
// quoteChar must be a single character (e.g. `"`).
func QuoteForFoldLower(identifier string, quoteChar string) string {
	if !needsQuoting(identifier) && IsLower(identifier) {
		return identifier
	}
	escaped := strings.ReplaceAll(identifier, quoteChar, quoteChar+quoteChar)
	return quoteChar + escaped + quoteChar
}

// QuoteWithBracketsIfNeeded quotes an identifier with [brackets] (MSSQL syntax)
// only when needed. Returns the identifier raw when it contains only safe chars.
// Internal `]` characters are escaped by doubling.
func QuoteWithBracketsIfNeeded(identifier string) string {
	if !needsQuoting(identifier) {
		return identifier
	}
	escaped := strings.ReplaceAll(identifier, "]", "]]")
	return fmt.Sprintf("[%s]", escaped)
}

// sqlExpressionMarkers are substrings whose presence in a field string
// indicates a SQL expression rather than a bare column name. Used by
// each dialect's ResolveFieldRef to skip identifier quoting for
// expressions while still quoting catalog column names that contain
// special chars (e.g. Fivetran-style "_meta/mtime").
var sqlExpressionMarkers = []string{
	"(", ")",
	"->>", "->",
	"#>>", "#>",
	"::",
	",",
	" AS ", " as ",
}

// isLikelyExpression returns true when the string contains a substring
// that strongly suggests a SQL expression (function call, json path, cast,
// multi-arg list, AS clause). Heuristic — see sqlExpressionMarkers.
func isLikelyExpression(s string) bool {
	for _, m := range sqlExpressionMarkers {
		if strings.Contains(s, m) {
			return true
		}
	}
	return false
}
