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

// QuoteWithDoubleQuotesIfNeeded quotes an identifier with double quotes
// (ANSI SQL standard) only when needed. Returns the identifier raw when it
// contains only safe chars. Used by Trino, Postgres, Redshift, DuckDB.
func QuoteWithDoubleQuotesIfNeeded(identifier string) string {
	if !needsQuoting(identifier) {
		return identifier
	}
	escaped := strings.ReplaceAll(identifier, `"`, `""`)
	return fmt.Sprintf(`"%s"`, escaped)
}

// QuoteWithBackticksIfNeeded quotes an identifier with backticks only when
// needed. Returns the identifier raw when it contains only safe chars.
// Used by BigQuery, ClickHouse, MySQL.
func QuoteWithBackticksIfNeeded(identifier string) string {
	if !needsQuoting(identifier) {
		return identifier
	}
	escaped := strings.ReplaceAll(identifier, "`", "``")
	return fmt.Sprintf("`%s`", escaped)
}

// QuoteForFoldUpper quotes an identifier with double quotes for
// fold-to-upper dialects (Snowflake, Oracle). Leaves pure-upper AND
// pure-lower identifiers unquoted (pure-upper matches catalog canonical
// case directly; pure-lower folds up to match). Quotes mixed-case and
// identifiers containing characters requiring quoting.
// MUST NOT be used for fold-to-lower dialects (Postgres, Redshift, Trino) —
// pure-upper input would slip through unquoted and fold to lower,
// missing the catalog entry. Use QuoteForFoldLower there.
func QuoteForFoldUpper(identifier string) string {
	if needsQuoting(identifier) || (!IsUpper(identifier) && !IsLower(identifier)) {
		escaped := strings.ReplaceAll(identifier, `"`, `""`)
		return `"` + escaped + `"`
	}
	return identifier
}

// QuoteForFoldLower quotes an identifier with double quotes for
// fold-to-lower dialects (Postgres, Redshift, Trino). Leaves only
// pure-lower identifiers with no characters requiring quoting unquoted.
// Any uppercase letter or special char would otherwise fold or parse
// incorrectly.
// MUST NOT be used for fold-to-upper dialects (Snowflake, Oracle) —
// pure-upper input would be needlessly quoted. Use QuoteForFoldUpper there.
func QuoteForFoldLower(identifier string) string {
	if !needsQuoting(identifier) && IsLower(identifier) {
		return identifier
	}
	escaped := strings.ReplaceAll(identifier, `"`, `""`)
	return `"` + escaped + `"`
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
//
// `.` is included so dotted paths like `metadata.created_at` (nested
// struct access on BigQuery / Databricks, qualified column refs elsewhere)
// pass through raw. Trade-off: a column literally named `my.col` cannot be
// referenced through this path — callers must pre-quote it themselves.
//
// ` AS ` / ` as ` intentionally excluded: every realistic AS clause sits
// inside `CAST(...)` or a function call, so `(` already triggers passthrough.
// Including a bare ` as ` marker would false-positive on column names
// containing that substring (e.g. `things as service`).
var sqlExpressionMarkers = [...]string{
	"(", ")",
	"->>", "->",
	"#>>", "#>",
	"::",
	",",
	".",
}

// isLikelyExpression returns true when the string contains a substring
// that strongly suggests a SQL expression (function call, json path, cast,
// dotted nested access, multi-arg list). Heuristic — see sqlExpressionMarkers.
func isLikelyExpression(s string) bool {
	for _, m := range sqlExpressionMarkers {
		if strings.Contains(s, m) {
			return true
		}
	}
	return false
}

// isQuotedWith reports whether s is wrapped by the given single-byte
// open/close delimiters (len(s) >= 2). Used by dialect ResolveFieldRef
// implementations to pass through pre-quoted identifiers idempotently.
// A column literally named including the delimiter chars (rare in practice)
// is unsupported through this path — callers must avoid that case.
func isQuotedWith(s string, open, close byte) bool {
	if len(s) < 2 {
		return false
	}
	return s[0] == open && s[len(s)-1] == close
}
